package server

import (
	"context"
	"crypto/rand"
	"net/http"
	"testing"
	"time"

	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/realtime"
)

var testHub = realtime.NewHub()

// 单实例测试注入本地转发，避免把 Redis 作为所有测试的前置条件。
func localPublish(_ context.Context, event danmaku.RoomEvent) error {
	testHub.Broadcast(event.RoomID, event.Type, event.Data)
	return nil
}

func handleTransportWebSocket(publish func(context.Context, danmaku.RoomEvent) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		testHub.ServeWebSocket(w, r, realtime.PingInterval, realtime.PongWait, publish, func(room string, kind int, data []byte) (danmaku.RoomEvent, error) {
			return danmaku.RoomEvent{ID: rand.Text(), RoomID: room, Type: kind, Data: data}, nil
		})
	}
}

func waitForRoomCount(t *testing.T, roomID string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		count, exists := testHub.LocalRoomCounts()[roomID]
		if count == want && (want != 0 || !exists) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("room %q: connections = %d, exists = %v, want %d", roomID, count, exists, want)
		}
		time.Sleep(time.Millisecond)
	}
}
