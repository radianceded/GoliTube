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

func publishRoomEvent(ctx context.Context, rdb *redis.Client, event RoomEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return rdb.Publish(ctx, danmakuChannel, payload).Err()
}
//用来接收 Redis 中的弹幕事件，并将其广播给对应房间的所有客户端。
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
}//这个函数用于将弹幕消息广播给指定房间的所有客户端。它会遍历房间中的所有客户端，并将消息发送到每个客户端的发送队列中。如果发送队列已关闭，则会从房间中移除该客户端。

