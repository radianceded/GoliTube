package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/realtime"
	"github.com/radianceded/GoliTube/internal/testutil"
)

func TestOnlineValidation(t *testing.T) {
	router := newRouter(&auth.Handler{}, testHub, nil, localPublish)
	for _, test := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/rooms/online", 400},
		{"GET", "/rooms/online?room=%FF", 400},
		{"GET", "/rooms/online?room=valid", 503},
		{"POST", "/rooms/online?room=valid", 405},
	} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(test.method, test.path, nil))
		if w.Code != test.status || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s %s status=%d", test.method, test.path, w.Code)
		}
	}
}

func TestCrossInstanceOnlineConnections(t *testing.T) {
	rdb := testutil.RedisClient(t)
	addr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	first, second := startTestInstance(t, addr), startTestInstance(t, addr)
	room := "online-process-" + rand.Text()
	t.Cleanup(func() { rdb.Del(context.Background(), realtime.PresenceKeys(room)...) })
	dial := func(addr string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws?room="+room, nil)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}
	a, b := dial(first), dial(second)
	defer a.Close()
	defer b.Close()
	client := &http.Client{Timeout: time.Second}
	wait := func(want int) {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			response, err := client.Get("http://" + first + "/rooms/online?room=" + room)
			if err != nil {
				t.Fatal(err)
			}
			var data struct {
				Online int `json:"online_connections"`
			}
			err = json.NewDecoder(response.Body).Decode(&data)
			response.Body.Close()
			if response.StatusCode == 200 && err == nil && data.Online == want {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("跨实例在线连接数未更新到 %d", want)
	}
	wait(2)
	b.Close()
	wait(1)
	a.Close()
	wait(0)
}
