package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	maxMessageSize = 4096
	pingInterval   = 25 * time.Second
	pongWait       = 60 * time.Second
	writeWait      = 5 * time.Second
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

var rooms = make(map[string]map[*websocket.Conn]*Client)

// rooms[ID][conn] = client
var roomsMu sync.Mutex

// initialize a global mutex to protect the rooms map

func broadcast(roomID string, messageType int, message []byte) {
	roomsMu.Lock()
	defer roomsMu.Unlock()

	members := rooms[roomID]
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
		delete(rooms, roomID)
	}
}

func handleWebSocket(publish func(context.Context, RoomEvent) error, auth *authHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Cookie 固定来自本次握手；每次发送重新查 Redis，使退出、过期和轮换生效。
		serveWebSocketWithDecoder(w, r, pingInterval, pongWait, publish, func(room string, kind int, data []byte) (RoomEvent, error) {
			if auth == nil || auth.rdb == nil {
				return RoomEvent{}, &ProtocolError{Type: "error", Code: "auth_unavailable", Message: "账号服务暂不可用"}
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			user, err := auth.currentUser(ctx, r)
			cancel()
			if errors.Is(err, redis.Nil) {
				return RoomEvent{}, &ProtocolError{Type: "error", Code: "unauthenticated", Message: "请先登录，并重新建立弹幕连接"}
			}
			if err != nil {
				log.Println("弹幕发送身份查询失败:", err)
				return RoomEvent{}, &ProtocolError{Type: "error", Code: "auth_unavailable", Message: "账号服务暂不可用"}
			}
			return decodeDanmaku(room, kind, data, user)
		})
	}
}

// 显式传入解码函数和心跳期限，便于独立测试传输与业务逻辑。
func serveWebSocketWithDecoder(w http.ResponseWriter, r *http.Request, interval, wait time.Duration,
	publish func(context.Context, RoomEvent) error,
	decode func(string, int, []byte) (RoomEvent, error),
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
	conn.SetReadLimit(maxMessageSize)
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
	roomsMu.Lock()

	if rooms[roomID] == nil {
		rooms[roomID] = make(map[*websocket.Conn]*Client)
	}
	rooms[roomID][conn] = client
	count := len(rooms[roomID])
	roomsMu.Unlock()
	//lock to avoid race condition when adding connection to room and counting connections

	log.Println("WebSocket连接已建立")

	defer func() {
		roomsMu.Lock()

		members := rooms[roomID]
		if _, exists := members[conn]; exists {
			delete(members, conn)
			close(client.Send)
		}

		count := len(members)
		if count == 0 {
			delete(rooms, roomID)
		}

		roomsMu.Unlock()
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
			failure := &ProtocolError{Type: "error", Code: "invalid_danmaku", Message: err.Error()}
			var protocolError *ProtocolError
			if errors.As(err, &protocolError) {
				failure = protocolError
			}
			payload, _ := json.Marshal(failure)
			// 错误仅回复发送者，也经过同一个 writePump，避免并发写连接。
			// 与移除成员、关闭 Send 使用同一把锁，避免向已关闭的队列发送。
			roomsMu.Lock()
			sent := false
			if _, exists := rooms[roomID][conn]; exists {
				select {
				case client.Send <- Message{Type: websocket.TextMessage, Data: payload}:
					sent = true
				default:
				}
			}
			roomsMu.Unlock()
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

func health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, "ccb")
}

func main() {
	addr := flag.String("addr", "localhost:8081", "HTTP 服务监听地址")
	redisAddr := flag.String("redis-addr", "127.0.0.1:6379", "Redis 服务地址")
	mode := flag.String("mode", "server", "运行模式：server、persist 或 migrate-history")
	brokers := flag.String("kafka-brokers", "", "Kafka 地址，逗号分隔；server 留空时仅实时广播")
	flag.Parse()

	var err error
	switch *mode {
	case "server":
		if *brokers == "" {
			err = run(*addr, *redisAddr)
		} else {
			writer := newEventWriter(strings.Split(*brokers, ","))
			err = runServer(*addr, *redisAddr, func(ctx context.Context, event RoomEvent) error {
				return enqueueRoomEvent(ctx, writer, event)
			})
			writer.Close()
		}
	case "persist", "migrate-history":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		if *mode == "persist" {
			err = runPersistence(ctx, strings.Split(*brokers, ","), os.Getenv("GOLITUBE_MYSQL_DSN"), *redisAddr)
		} else {
			err = runHistoryBackfill(ctx, os.Getenv("GOLITUBE_MYSQL_DSN"))
		}
	default:
		err = fmt.Errorf("未知运行模式 %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func run(addr, redisAddr string) error {
	return runServer(addr, redisAddr, nil)
}

func runServer(addr, redisAddr string, enqueue func(context.Context, RoomEvent) error) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:                  redisAddr,
		DialTimeout:           3 * time.Second,
		ReadTimeout:           3 * time.Second,
		WriteTimeout:          3 * time.Second,
		ContextTimeoutEnabled: true,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := rdb.Ping(ctx).Err()
	cancel()
	if err != nil {
		return fmt.Errorf("连接 Redis 失败: %w", err)
	}
	log.Println("Redis 连接成功")

	subCtx, stopSubscription := context.WithCancel(context.Background())
	sub := rdb.Subscribe(subCtx, danmakuChannel)

	// Redis 确认订阅成功后，才开放 HTTP 服务。
	confirmCtx, cancelConfirm := context.WithTimeout(subCtx, 3*time.Second)
	_, err = sub.Receive(confirmCtx)
	cancelConfirm()
	if err != nil {
		stopSubscription()
		sub.Close()
		return fmt.Errorf("订阅 Redis 频道失败: %w", err)
	}

	subscriptionDone := make(chan struct{})
	go func() {
		defer close(subscriptionDone)
		consumeRoomEvents(subCtx, sub)
	}()
	defer func() {
		stopSubscription()
		sub.Close()
		<-subscriptionDone
	}()
	log.Println("Redis 频道订阅成功:", danmakuChannel)
	presenceDone := make(chan struct{})
	go func() {
		defer close(presenceDone)
		reportRoomPresence(subCtx, rdb)
	}()
	defer func() {
		stopSubscription()
		<-presenceDone
	}()

	publish := func(ctx context.Context, event RoomEvent) error {
		return publishRoomEvent(ctx, rdb, event)
	}
	if enqueue != nil {
		publish = durablePublish(enqueue, publish)
	}
	var history func(context.Context, HistoryQuery) (HistoryPage, error)
	var appDB *sql.DB
	if dsn := os.Getenv("GOLITUBE_MYSQL_DSN"); dsn != "" {
		db, err := newMySQLDB(dsn)
		if err != nil {
			return err
		}
		defer db.Close()
		appDB = db
		history = cachedHistory(rdb, func(ctx context.Context, q HistoryQuery) (HistoryPage, error) {
			return queryHistory(ctx, db, q)
		})
	}
	auth := &authHandler{db: appDB, rdb: rdb, namespace: "golitube", secureCookie: os.Getenv("GOLITUBE_COOKIE_SECURE") == "1",
		trustLoopbackProxy: os.Getenv("GOLITUBE_TRUST_LOOPBACK_PROXY") == "1"}
	router := newRouter(auth, history, publish)
	log.Printf("准备监听 http://%s", addr)

	return http.ListenAndServe(addr, router)
}
