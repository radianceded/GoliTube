package main

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
	"github.com/redis/go-redis/v9"
)

func TestRoomPresenceAggregation(t *testing.T) {
	rdb := testRedisClient(t)
	ctx := t.Context()
	room := "presence-" + rand.Text()
	other := room + "-other"
	t.Cleanup(func() {
		rdb.Del(context.Background(), append(presenceKeys(room), presenceKeys(other)...)...)
	})
	write := func(instance string, counts map[string]int) {
		t.Helper()
		if err := writeRoomPresence(ctx, rdb, instance, counts); err != nil {
			t.Fatal(err)
		}
	}
	check := func(want int64) {
		t.Helper()
		got, err := readPresenceScript.Run(ctx, rdb, presenceKeys(room)).Int64()
		if err != nil || got != want {
			t.Fatalf("online=%d want=%d err=%v", got, want, err)
		}
	}
	write("server-a", map[string]int{room: 2, other: 9})
	write("server-b", map[string]int{room: 3})
	check(5)
	write("server-a", map[string]int{room: 1})
	check(4) // 重复上报覆盖本实例贡献，不会重复累加。
	write("server-a", map[string]int{room: 0})
	check(3)
	// 将 B 的租约标为过期，模拟进程崩溃后没有再上报。
	if err := rdb.ZAdd(ctx, presenceKeys(room)[1], redis.Z{Score: 0, Member: "server-b"}).Err(); err != nil {
		t.Fatal(err)
	}
	check(0)
	if rdb.HExists(ctx, presenceKeys(room)[0], "server-b").Val() {
		t.Fatal("过期实例的计数没有回收")
	}
	write("server-c", map[string]int{room: 1})
	for _, key := range presenceKeys(room) {
		if ttl := rdb.PTTL(ctx, key).Val(); ttl <= 0 || ttl > 2*presenceLease {
			t.Fatalf("在线状态没有设置 TTL: %v", ttl)
		}
	}
	router := newRouter(&authHandler{rdb: rdb}, nil, localPublish)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/rooms/online?room="+room, nil))
	var response struct {
		RoomID string `json:"room_id"`
		Online int64  `json:"online_connections"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 || response.RoomID != room || response.Online != 1 {
		t.Fatalf("online response=%d %s", w.Code, w.Body.String())
	}
	rdb.Close()
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/rooms/online?room="+room, nil))
	if w.Code != 503 {
		t.Fatal("Redis 不可用时不能显示为 0 人")
	}
}

func TestOnlineValidation(t *testing.T) {
	router := newRouter(&authHandler{}, nil, localPublish)
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
	rdb := testRedisClient(t)
	addr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	first, second := startTestInstance(t, addr), startTestInstance(t, addr)
	room := "online-process-" + rand.Text()
	t.Cleanup(func() { rdb.Del(context.Background(), presenceKeys(room)...) })
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
