package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

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

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	serveWebSocket(w, r, pingInterval, pongWait)
}

// 显式传入心跳时间，便于测试使用较短的期限，不修改全局配置。
func serveWebSocket(w http.ResponseWriter, r *http.Request, interval, wait time.Duration) {
	roomID := strings.TrimSpace(r.URL.Query().Get("room"))
	if roomID == "" {
		roomID = "lobby"
	}
	if len(roomID) > 128 {
		http.Error(w, "room name must be at most 128 bytes", http.StatusBadRequest)
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

		log.Println("消息类型:", messageType, "消息内容:", string(message))

		broadcast(roomID, messageType, message)
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
	flag.Parse()

	if err := run(*addr, *redisAddr); err != nil {
		log.Fatal(err)
	}
}

func run(addr, redisAddr string) error {
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

	http.HandleFunc("/health", health)
	http.HandleFunc("/ws", handleWebSocket)
	log.Printf("准备监听 http://%s", addr)

	return http.ListenAndServe(addr, nil)
}
