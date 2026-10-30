package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const danmakuChannel = "golitube:danmaku"

type RoomEvent struct {
	RoomID string `json:"room_id"`
	Type   int    `json:"type"`
	Data   []byte `json:"data"`
}

func consumeRoomEvents(ctx context.Context, sub *redis.PubSub) {
	messages := sub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case message, open := <-messages:
			if !open {
				return
			}

			var event RoomEvent
			if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
				log.Println("Redis 事件解码失败:", err)
				continue
			}
			if event.RoomID == "" ||
				len(event.RoomID) > 128 ||
				len(event.Data) > maxMessageSize ||
				(event.Type != websocket.TextMessage && event.Type != websocket.BinaryMessage) {
				log.Println("忽略非法 Redis 弹幕事件")
				continue
			}

			broadcast(event.RoomID, event.Type, event.Data)
		}
	}
}
