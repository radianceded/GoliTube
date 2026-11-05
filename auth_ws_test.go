package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func TestWebSocketAuthorizationFailure(t *testing.T) {
	// 已关闭的 Redis 客户端让依赖失败确定发生，不需要外部服务或等待网络超时。
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	rdb.Close()
	for _, test := range []struct {
		name, cookie, code string
		auth               *authHandler
	}{
		{name: "no-session", code: "unauthenticated", auth: &authHandler{rdb: rdb}},
		{name: "invalid-cookie", cookie: "short", code: "unauthenticated", auth: &authHandler{rdb: rdb}},
		{name: "redis-unavailable", cookie: strings.Repeat("x", 32), code: "auth_unavailable", auth: &authHandler{rdb: rdb}},
		{name: "not-configured", code: "auth_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			room := "auth-failure-" + test.name
			defer waitForRoomCount(t, room, 0)
			server := httptest.NewServer(handleWebSocket(func(context.Context, RoomEvent) error {
				t.Error("unauthorized message reached publisher")
				return nil
			}, test.auth))
			defer server.Close()
			headers := http.Header{}
			if test.cookie != "" {
				headers.Set("Cookie", sessionCookie+"="+test.cookie)
			}
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, headers)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for range 2 { // 拒绝发送后保留连接，游客仍能观看。
				if err := conn.WriteMessage(websocket.TextMessage, testDanmakuPayload(t, "denied")); err != nil {
					t.Fatal(err)
				}
				conn.SetReadDeadline(time.Now().Add(time.Second))
				_, body, err := conn.ReadMessage()
				var response ProtocolError
				if err != nil || json.Unmarshal(body, &response) != nil || response.Code != test.code {
					t.Fatalf("error=%+v read=%v", response, err)
				}
			}
		})
	}
}
