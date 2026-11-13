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
	mu    sync.Mutex
	rooms map[string]map[*websocket.Conn]*Client
}

func NewHub() *Hub {
	return &Hub{rooms: make(map[string]map[*websocket.Conn]*Client)}
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

// 显式传入解码函数和心跳期限，便于独立测试传输与业务逻辑。
func (h *Hub) ServeWebSocket(w http.ResponseWriter, r *http.Request, interval, wait time.Duration,
	publish func(context.Context, danmaku.RoomEvent) error,
	decode func(string, int, []byte) (danmaku.RoomEvent, error),
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

	log.Printf("连接加入，房间 %q 在线连接数：%d", roomID, count)
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("读取消息失败:", err)
			return
		}

		event, err := decode(roomID, messageType, message)
		if err != nil {
			failure := &danmaku.ProtocolError{Type: "error", Code: "invalid_danmaku", Message: err.Error()}
			var protocolError *danmaku.ProtocolError
			if errors.As(err, &protocolError) {
				failure = protocolError
			}
			payload, _ := json.Marshal(failure)
			// 错误仅回复发送者，也经过同一个 writePump，避免并发写连接。
			// 与移除成员、关闭 Send 使用同一把锁，避免向已关闭的队列发送。
			h.mu.Lock()
			sent := false
			if _, exists := h.rooms[roomID][conn]; exists {
				select {
				case client.Send <- Message{Type: websocket.TextMessage, Data: payload}:
					sent = true
				default:
				}
			}
			h.mu.Unlock()
			if !sent {
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
