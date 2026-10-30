package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func TestConsumeRoomEvents(t *testing.T) {
	addr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_ADDR 显式启用真实 Redis 测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer rdb.Close()
	sub := rdb.Subscribe(ctx, danmakuChannel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(handleWebSocket(localPublish))
	defer server.Close()
	roomID := fmt.Sprintf("redis-test-%d", time.Now().UnixNano())
	defer waitForRoomCount(t, roomID, 0)
	conn, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http")+"?room="+roomID, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitForRoomCount(t, roomID, 1)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		consumeRoomEvents(ctx, sub)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("取消后订阅接收循环未退出")
		}
	}()

	publish := func(payload interface{}) {
		t.Helper()
		if err := rdb.Publish(ctx, danmakuChannel, payload).Err(); err != nil {
			t.Fatal(err)
		}
	}
	publish("invalid JSON")
	for _, event := range []RoomEvent{
		{RoomID: roomID, Type: websocket.CloseMessage},
		{RoomID: roomID, Type: websocket.TextMessage, Data: make([]byte, maxMessageSize+1)},
		{RoomID: "", Type: websocket.TextMessage},
		{RoomID: strings.Repeat("x", 129), Type: websocket.TextMessage},
		{RoomID: roomID + "-other", Type: websocket.TextMessage, Data: []byte("other room")},
		{RoomID: roomID, Type: websocket.TextMessage, Data: []byte("hello")},
		{RoomID: roomID, Type: websocket.BinaryMessage, Data: []byte{0, 255}},
	} {
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		publish(payload)
	}
	for _, want := range []Message{
		{Type: websocket.TextMessage, Data: []byte("hello")},
		{Type: websocket.BinaryMessage, Data: []byte{0, 255}},
	} {
		kind, data, err := conn.ReadMessage()
		if err != nil || kind != want.Type || string(data) != string(want.Data) {
			t.Fatalf("收到 type=%d data=%q err=%v，期望 %+v", kind, data, err, want)
		}
	}
}
