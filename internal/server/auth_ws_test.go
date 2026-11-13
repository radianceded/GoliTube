package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func TestWebSocketAuthorizationFailure(t *testing.T) {
	// 已关闭的 Redis 客户端让依赖失败确定发生，不需要外部服务或等待网络超时。
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	rdb.Close()
	for _, test := range []struct {
		name, cookie, code string
		a                  *auth.Handler
	}{
		{name: "no-session", code: "unauthenticated", a: &auth.Handler{RDB: rdb}},
		{name: "invalid-cookie", cookie: "short", code: "unauthenticated", a: &auth.Handler{RDB: rdb}},
		{name: "redis-unavailable", cookie: strings.Repeat("x", 32), code: "auth_unavailable", a: &auth.Handler{RDB: rdb}},
		{name: "not-configured", code: "auth_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			room := "auth-failure-" + test.name
			defer waitForRoomCount(t, room, 0)
			server := httptest.NewServer(handleWebSocket(testHub, func(context.Context, danmaku.RoomEvent) error {
				t.Error("unauthorized message reached publisher")
				return nil
			}, test.a))
			defer server.Close()
			headers := http.Header{}
			if test.cookie != "" {
				headers.Set("Cookie", auth.SessionCookie+"="+test.cookie)
			}
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, headers)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for range 2 { // 拒绝发送后保留连接，游客仍能观看。
				if err := conn.WriteMessage(websocket.TextMessage, testutil.DanmakuPayload(t, "denied")); err != nil {
					t.Fatal(err)
				}
				conn.SetReadDeadline(time.Now().Add(time.Second))
				_, body, err := conn.ReadMessage()
				var response danmaku.ProtocolError
				if err != nil || json.Unmarshal(body, &response) != nil || response.Code != test.code {
					t.Fatalf("error=%+v read=%v", response, err)
				}
			}
		})
	}
}
