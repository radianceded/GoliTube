package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"golang.org/x/time/rate"
)

const (
	PingInterval = 25 * time.Second
	PongWait     = 60 * time.Second
	writeWait    = 5 * time.Second
)

var upgrader = websocket.Upgrader{}

type Message struct {
	Type int
	Data []byte
}

// a ws message waiting to be sent
type Client struct {
	Conn *websocket.Conn
	Send chan Message
}

// a client conn and its sending queue
func (c *Client) writePump(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer c.Conn.Close()

	for {
		var message Message

		// 先处理已到期的心跳，避免持续有弹幕时反复延后 Ping。
		select {
		case <-ticker.C:
			message.Type = websocket.PingMessage
		default:
			select {
			case next, open := <-c.Send:
				if !open {
					return
				}
				message = next
			case <-ticker.C:
				message.Type = websocket.PingMessage
			}
		}

		err := c.Conn.SetWriteDeadline(
			time.Now().Add(writeWait),
		)
		// 设置写超时，避免阻塞在慢连接上。
		if err != nil {
			log.Println("设置写超时失败:", err)
			return
		}

		err = c.Conn.WriteMessage(message.Type, message.Data)
		if err != nil {
			log.Println("发送消息失败:", err)
			return
		}
	}
}

// continuely reading sending queue and writing to the websocket connection, until the queue is closed or an error occurs

// Hub 保存本实例的房间连接；rooms[ID][conn] = client，由 mu 保护。
type Hub struct {
	mu      sync.Mutex
	rooms   map[string]map[*websocket.Conn]*Client
	closing bool // CloseAll 之后不再接纳新连接。
}

func NewHub() *Hub {
	return &Hub{rooms: make(map[string]map[*websocket.Conn]*Client)}
}

func goingAwayFrame() []byte {
	return websocket.FormatCloseMessage(websocket.CloseGoingAway, "服务正在重启")
}

// CloseAll 向本实例的所有连接发送 1001（离开）关闭帧，并等待读循环退出、成员清理完成。
// 调用后 Hub 不再接纳新连接。ctx 到期后强制断开剩余连接。
func (h *Hub) CloseAll(ctx context.Context) {
	h.mu.Lock()
	h.closing = true
	var conns []*websocket.Conn
	for _, members := range h.rooms {
		for conn := range members {
			conns = append(conns, conn)
		}
	}
	h.mu.Unlock()

	// WriteControl 可与 writePump 的写入并发调用；没有截止时间的 ctx 也限制在 writeWait 内。
	deadline := time.Now().Add(writeWait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	frame := goingAwayFrame()
	var sent sync.WaitGroup
	for _, conn := range conns {
		sent.Add(1)
		go func() {
			defer sent.Done()
			conn.WriteControl(websocket.CloseMessage, frame, deadline)
		}()
	}
	sent.Wait()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for len(h.LocalRoomCounts()) > 0 {
		select {
		case <-ctx.Done():
			for _, conn := range conns {
				conn.Close()
			}
			return
		case <-ticker.C:
		}
	}
}

func (h *Hub) Broadcast(roomID string, messageType int, message []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	members := h.rooms[roomID]
	for conn, client := range members {
		select {
		case client.Send <- Message{
			Type: messageType,
			Data: message,
		}:
			// 成功放入该客户端的发送队列。
		default:
			// 队列没有可用位置，移除并断开慢连接。
			log.Println("发送队列已满，断开连接")
			delete(members, conn)
			close(client.Send)
			conn.Close()
		}
	}

	if len(members) == 0 {
		delete(h.rooms, roomID)
	}
}

// SendLimits 描述单个连接上弹幕发送的本地限流。零值表示不限流。
// 令牌桶在解码和会话查询之前检查，挡住单连接洪水而不增加 Redis 和 Kafka 的负载；
// 跨连接、跨实例的按用户限流需要先确定用户，由 decode 回调返回 rate_limited 错误实现。
type SendLimits struct {
	// 每秒补充 Rate 个令牌，最多积累 Burst 个。
	Rate  rate.Limit
	Burst int
	// 连续被拒绝超过 MaxStrikes 次后关闭连接，0 表示不关闭。
	MaxStrikes int
}

// 被限流时的私有错误回复。
func RateLimitedError() *danmaku.ProtocolError {
	return &danmaku.ProtocolError{Type: "error", Code: "rate_limited", Message: "发送过于频繁，请稍后再试"}
}

// 显式传入解码函数和心跳期限，便于独立测试传输与业务逻辑。
func (h *Hub) ServeWebSocket(w http.ResponseWriter, r *http.Request, interval, wait time.Duration,
	publish func(context.Context, danmaku.RoomEvent) error,
	decode func(string, int, []byte) (danmaku.RoomEvent, error),
) {
	h.ServeWebSocketLimited(w, r, interval, wait, publish, decode, SendLimits{})
}

// ServeWebSocketLimited 在 ServeWebSocket 的基础上对发送增加限流。
func (h *Hub) ServeWebSocketLimited(w http.ResponseWriter, r *http.Request, interval, wait time.Duration,
	publish func(context.Context, danmaku.RoomEvent) error,
	decode func(string, int, []byte) (danmaku.RoomEvent, error),
	limits SendLimits,
) {
	roomID := strings.TrimSpace(r.URL.Query().Get("room"))
	if roomID == "" {
		roomID = "lobby"
	}
	if len(roomID) > 128 {
		http.Error(w, "room name must be at most 128 bytes", http.StatusBadRequest)
		return
	}
	if !utf8.ValidString(roomID) {
		http.Error(w, "room name must be UTF-8", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("升级WebSocket连接失败:", err)
		return
	}
	// upgrade from http to websocket
	defer conn.Close()
	conn.SetReadLimit(danmaku.MaxMessageSize)
	// set a maximum message size to prevent abuse
	if err := conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
		log.Println("设置初始读超时失败:", err)
		return
	}
	conn.SetPongHandler(func(_ string) error {
		return conn.SetReadDeadline(time.Now().Add(wait))
	})

	client := &Client{
		Conn: conn,
		Send: make(chan Message, 256),
	}
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		conn.WriteControl(websocket.CloseMessage, goingAwayFrame(), time.Now().Add(writeWait))
		return
	}
	if h.rooms[roomID] == nil {
		h.rooms[roomID] = make(map[*websocket.Conn]*Client)
	}
	h.rooms[roomID][conn] = client
	count := len(h.rooms[roomID])
	h.mu.Unlock()
	//lock to avoid race condition when adding connection to room and counting connections

	log.Println("WebSocket连接已建立")

	defer func() {
		h.mu.Lock()

		members := h.rooms[roomID]
		if _, exists := members[conn]; exists {
			delete(members, conn)
			close(client.Send)
		}

		count := len(members)
		if count == 0 {
			delete(h.rooms, roomID)
		}

		h.mu.Unlock()
		log.Printf("连接移除，房间 %q 在线连接数：%d", roomID, count)
	}()

	go client.writePump(interval)

	// 错误仅回复发送者，也经过同一个 writePump，避免并发写连接。
	// 与移除成员、关闭 Send 使用同一把锁，避免向已关闭的队列发送。队列满或成员已被清理时返回 false。
	reply := func(failure *danmaku.ProtocolError) bool {
		payload, _ := json.Marshal(failure)
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, exists := h.rooms[roomID][conn]; !exists {
			return false
		}
		select {
		case client.Send <- Message{Type: websocket.TextMessage, Data: payload}:
			return true
		default:
			return false
		}
	}
	var bucket *rate.Limiter
	if limits.Rate > 0 && limits.Burst > 0 {
		bucket = rate.NewLimiter(limits.Rate, limits.Burst)
	}
	strikes := 0

	log.Printf("连接加入，房间 %q 在线连接数：%d", roomID, count)
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("读取消息失败:", err)
			return
		}

		// 限流先于解码和会话查询，超限消息不消耗 Redis 和 Kafka。
		if bucket != nil && !bucket.Allow() {
			strikes++
			if limits.MaxStrikes > 0 && strikes > limits.MaxStrikes {
				log.Printf("连续超限 %d 次，关闭连接", strikes)
				conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "发送过于频繁"), time.Now().Add(writeWait))
				return
			}
			if !reply(RateLimitedError()) {
				return
			}
			continue
		}
		strikes = 0

		event, err := decode(roomID, messageType, message)
		if err != nil {
			failure := &danmaku.ProtocolError{Type: "error", Code: "invalid_danmaku", Message: err.Error()}
			var protocolError *danmaku.ProtocolError
			if errors.As(err, &protocolError) {
				failure = protocolError
			}
			if !reply(failure) {
				return
			}
			continue
		}

		publishCtx, cancelPublish := context.WithTimeout(r.Context(), 3*time.Second)
		err = publish(publishCtx, event)
		cancelPublish()
		if err != nil {
			log.Println("发布弹幕失败，关闭连接:", err)
			return
		}
	}
}
