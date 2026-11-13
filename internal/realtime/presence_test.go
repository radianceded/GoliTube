package realtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/radianceded/GoliTube/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func TestRoomPresenceAggregation(t *testing.T) {
	rdb := testutil.RedisClient(t)
	ctx := t.Context()
	room := "presence-" + rand.Text()
	other := room + "-other"
	t.Cleanup(func() {
		rdb.Del(context.Background(), append(PresenceKeys(room), PresenceKeys(other)...)...)
	})
	write := func(instance string, counts map[string]int) {
		t.Helper()
		if err := writeRoomPresence(ctx, rdb, instance, counts); err != nil {
			t.Fatal(err)
		}
	}
	check := func(want int64) {
		t.Helper()
		got, err := readPresenceScript.Run(ctx, rdb, PresenceKeys(room)).Int64()
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
	if err := rdb.ZAdd(ctx, PresenceKeys(room)[1], redis.Z{Score: 0, Member: "server-b"}).Err(); err != nil {
		t.Fatal(err)
	}
	check(0)
	if rdb.HExists(ctx, PresenceKeys(room)[0], "server-b").Val() {
		t.Fatal("过期实例的计数没有回收")
	}
	write("server-c", map[string]int{room: 1})
	for _, key := range PresenceKeys(room) {
		if ttl := rdb.PTTL(ctx, key).Val(); ttl <= 0 || ttl > 2*presenceLease {
			t.Fatalf("在线状态没有设置 TTL: %v", ttl)
		}
	}
	handler := OnlineHandler(rdb)
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "/rooms/online?room="+room, nil))
	var response struct {
		RoomID string `json:"room_id"`
		Online int64  `json:"online_connections"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != 200 || response.RoomID != room || response.Online != 1 {
		t.Fatalf("online response=%d %s", w.Code, w.Body.String())
	}
	rdb.Close()
	w = httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "/rooms/online?room="+room, nil))
	if w.Code != 503 {
		t.Fatal("Redis 不可用时不能显示为 0 人")
	}
}
