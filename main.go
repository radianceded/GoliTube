package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}
type Message struct{
	Type int
	Data []byte
}
//a ws message waiting to be sent
type Client struct{
	Conn *websocket.Conn
	Send chan Message
}
// a client conn and its sending queue
func (c *Client) writePump() {
    defer c.Conn.Close()

    for message := range c.Send {
        err := c.Conn.SetWriteDeadline(time.Now().Add(3.6 * time.Second))
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

// 第一层按房间名查找，第二层保存该房间的在线连接。
var rooms = make(map[string]map[*websocket.Conn]*Client)
var roomsMu sync.Mutex

func broadcast(roomID string, messageType int, message []byte) {
	// ponytail: 全局锁串行广播；慢连接会延迟其他连接，后续用每连接发送队列隔离。
	roomsMu.Lock()
	defer roomsMu.Unlock()

	members := rooms[roomID]
	for client := range members {
		err := client.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err == nil {
			err = client.WriteMessage(messageType, message)
		}
		if err != nil {
			log.Println("广播消息失败:", err)
			delete(members, client)
			client.Close()
		}
	}
	if len(members) == 0 {
		delete(rooms, roomID)
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
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

	roomsMu.Lock()
	if rooms[roomID] == nil {
		rooms[roomID] = make(map[*websocket.Conn]bool)
	}
	rooms[roomID][conn] = true
	count := len(rooms[roomID])
	roomsMu.Unlock()
	//上锁创建房间

	log.Println("WebSocket连接已建立")

	defer conn.Close()

	defer func() {
		roomsMu.Lock()
		delete(rooms[roomID], conn)
		count := len(rooms[roomID])
		if count == 0 {
			delete(rooms, roomID)
		}
		roomsMu.Unlock()

		log.Printf("连接移除，房间 %q 在线连接数：%d", roomID, count)
	}()

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
	http.HandleFunc("/health", health)
	http.HandleFunc("/ws", handleWebSocket)
	log.Println("准备监听http://localhost:8081")

	err := http.ListenAndServe("localhost:8081", nil)
	if err != nil {
		log.Fatal(err)
	}
}
