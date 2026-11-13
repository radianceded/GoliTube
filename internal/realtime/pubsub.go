package realtime

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/redis/go-redis/v9"
)

const Channel = "golitube:danmaku"

func Publish(ctx context.Context, rdb *redis.Client, event danmaku.RoomEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return rdb.Publish(ctx, Channel, payload).Err()
}

// 用来接收 Redis 中的弹幕事件，并将其广播给对应房间的所有客户端。
func (h *Hub) ConsumeEvents(ctx context.Context, sub *redis.PubSub) {
	messages := sub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case message, open := <-messages:
			if !open {
				return
			}

			var event danmaku.RoomEvent
			if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
				log.Println("Redis 事件解码失败:", err)
				continue
			}
			if event.RoomID == "" ||
				len(event.RoomID) > 128 ||
				len(event.Data) > danmaku.MaxMessageSize ||
				(event.Type != websocket.TextMessage && event.Type != websocket.BinaryMessage) {
				log.Println("忽略非法 Redis 弹幕事件")
				continue
			}

			h.Broadcast(event.RoomID, event.Type, event.Data)
		}
	}
}
