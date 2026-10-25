package main

import (
	"github.com/gorilla/websocket"
	"fmt"
	"log"
	"net/http"
)
var upgrader = websocket.Upgrader{}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("升级WebSocket连接失败:", err)
		return
	}

	log.Println("WebSocket连接已建立")

	defer conn.Close()

	messageType, message, err := conn.ReadMessage()
	if err != nil {
		log.Println("读取消息失败:", err)
		return
	}

	log.Println("消息类型:", messageType,"消息内容:", string(message))

	

	err = conn.WriteMessage(messageType, message)
	if err != nil {
		log.Println("发送消息失败:", err)
		return
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
