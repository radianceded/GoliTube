package server

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/history"
	"github.com/radianceded/GoliTube/internal/web"
)

func TestGinRouterCompatibility(t *testing.T) {
	a := &auth.Handler{}
	publish := func(context.Context, danmaku.RoomEvent) error { return nil }
	router := newRouter(a, testHub, nil, publish)
	// 保存迁移前的入口行为，检查 Gin 对路径、方法和错误响应的影响。
	previous := http.NewServeMux()
	previous.HandleFunc("/", web.ServeClient)
	previous.HandleFunc("/health", health)
	previous.Handle("/auth/", a)
	previous.HandleFunc("/ws", handleWebSocket(testHub, publish, a))
	previous.HandleFunc("/danmaku/history", history.Handler(nil))
	for _, path := range []string{
		"/", "/app.js", "/app.css", "/health", "/health/", "/ws", "/ws/",
		"/auth", "/auth?keep=1", "/auth/", "/auth/register", "/auth/login", "/auth/me", "/auth/logout", "/auth/unknown",
		"/danmaku/history", "/danmaku/history?room=1001&start_ms=0&end_ms=1000", "/danmaku/history/", "/missing",
	} {
		for _, method := range []string{"GET", "POST", "HEAD", "OPTIONS", "BREW"} {
			t.Run(method+path, func(t *testing.T) {
				before, after := httptest.NewRecorder(), httptest.NewRecorder()
				previous.ServeHTTP(before, httptest.NewRequest(method, path, nil))
				router.ServeHTTP(after, httptest.NewRequest(method, path, nil))
				if before.Code != after.Code || before.Body.String() != after.Body.String() {
					t.Fatalf("before status=%d body=%q; after status=%d body=%q", before.Code, before.Body.String(), after.Code, after.Body.String())
				}
				for _, header := range []string{"Allow", "Content-Type", "Cache-Control", "Location"} {
					if before.Header().Get(header) != after.Header().Get(header) {
						t.Errorf("%s before=%q after=%q", header, before.Header().Get(header), after.Header().Get(header))
					}
				}
			})
		}
	}
}

func TestGinRecoveryDoesNotLogCredentials(t *testing.T) {
	router := newRouter(&auth.Handler{}, testHub, nil, localPublish)
	const secret = "private-cookie-password-token"
	router.GET("/panic-test", func(c *gin.Context) { panic(secret) })
	var output bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(oldOutput)
	request := httptest.NewRequest("GET", "/panic-test?token="+secret, nil)
	request.Header.Set("Cookie", "session="+secret)
	request.Header.Set("Authorization", "Bearer "+secret)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != 500 || !strings.Contains(response.Body.String(), "internal_error") {
		t.Fatalf("unexpected recovery response: %d %s", response.Code, response.Body.String())
	}
	if strings.Contains(output.String(), secret) || strings.Contains(response.Body.String(), secret) {
		t.Fatal("recovery leaked request credentials or panic payload")
	}
}
