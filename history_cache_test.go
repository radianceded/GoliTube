package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_ADDR 启用真实 Redis 测试")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	return rdb
}

func TestHistoryCache(t *testing.T) {
	rdb := testRedisClient(t)
	ctx := t.Context()
	q := HistoryQuery{RoomID: "cache-" + rand.Text(), EndMS: 60000, Limit: 20}
	key := historyPageKey(q, "0")
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	calls := 0
	query := cachedHistory(rdb, func(context.Context, HistoryQuery) (HistoryPage, error) {
		calls++
		return HistoryPage{Items: []Danmaku{{ID: "saved", RoomID: q.RoomID}}, NextCursor: "cursor"}, nil
	})
	for range 2 {
		page, err := query(ctx, q)
		if err != nil || len(page.Items) != 1 || page.Items[0].ID != "saved" || page.NextCursor != "cursor" {
			t.Fatalf("page=%+v err=%v", page, err)
		}
	}
	if calls != 1 {
		t.Fatalf("命中后回源次数=%d，期望 1", calls)
	}
	if ttl := rdb.PTTL(ctx, key).Val(); ttl <= 0 || ttl > historyCacheTTL {
		t.Fatalf("TTL=%v", ttl)
	}
	// 缓存损坏、过期都应重新查库。
	rdb.Set(ctx, key, "broken", time.Second)
	query(ctx, q)
	rdb.PExpire(ctx, key, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	query(ctx, q)
	if calls != 3 {
		t.Fatalf("损坏和过期后回源次数=%d", calls)
	}
	for _, other := range []HistoryQuery{
		{RoomID: q.RoomID, EndMS: 60000, Limit: 50},
		{RoomID: q.RoomID + "-other", EndMS: 60000, Limit: 20},
		{RoomID: q.RoomID, StartMS: 30000, EndMS: 60000, Limit: 20},
	} {
		if historyPageKey(other, "0") == key {
			t.Fatal("不同查询共用了缓存键")
		}
	}
	for _, bypass := range []HistoryQuery{
		{RoomID: q.RoomID, EndMS: 60000, Limit: 20, AfterID: "cursor"},
		{RoomID: q.RoomID, StartMS: 1, EndMS: 60000, Limit: 20},
	} {
		before := calls
		query(ctx, bypass)
		query(ctx, bypass)
		if calls != before+2 {
			t.Fatal("非公共窗口或后续页应直接回源")
		}
	}
}

func TestHistoryCacheEmptyAndFailures(t *testing.T) {
	rdb := testRedisClient(t)
	q := HistoryQuery{RoomID: "cache-empty-" + rand.Text(), EndMS: 30000, Limit: 20}
	key := historyPageKey(q, "0")
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	calls := 0
	query := cachedHistory(rdb, func(context.Context, HistoryQuery) (HistoryPage, error) {
		calls++
		return HistoryPage{Items: []Danmaku{}}, nil
	})
	query(t.Context(), q)
	query(t.Context(), q)
	if ttl := rdb.PTTL(t.Context(), key).Val(); calls != 1 || ttl <= 0 || ttl > 2*time.Second {
		t.Fatalf("空结果 calls=%d TTL=%v", calls, ttl)
	}
	rdb.Del(t.Context(), key)
	want := errors.New("database unavailable")
	failing := cachedHistory(rdb, func(context.Context, HistoryQuery) (HistoryPage, error) { return HistoryPage{}, want })
	if _, err := failing(t.Context(), q); !errors.Is(err, want) || rdb.Exists(t.Context(), key).Val() != 0 {
		t.Fatal("数据库失败不能缓存为成功空结果")
	}
	rdb.Close()
	if _, err := query(t.Context(), q); err != nil || calls != 2 {
		t.Fatalf("Redis 故障应回源 calls=%d err=%v", calls, err)
	}
}

func TestHistoryCacheInvalidationDuringFill(t *testing.T) {
	rdb := testRedisClient(t)
	ctx := t.Context()
	q := HistoryQuery{RoomID: "cache-race-" + rand.Text(), EndMS: 60000, Limit: 20}
	oldKey := historyPageKey(q, "0")
	var newKey string
	t.Cleanup(func() { rdb.Del(context.Background(), oldKey, newKey, historyVersionKey(q.RoomID)) })
	calls := 0
	query := cachedHistory(rdb, func(ctx context.Context, q HistoryQuery) (HistoryPage, error) {
		calls++
		if calls == 1 {
			// 模拟查询已读到旧快照，随后消费者提交数据库并使缓存换代。
			if err := invalidateHistory(ctx, rdb, q.RoomID); err != nil {
				t.Fatal(err)
			}
			return HistoryPage{Items: []Danmaku{{ID: "old"}}}, nil
		}
		return HistoryPage{Items: []Danmaku{{ID: "new"}}}, nil
	})
	query(ctx, q)
	version, err := rdb.Get(ctx, historyVersionKey(q.RoomID)).Result()
	if err != nil || version == "0" {
		t.Fatalf("version=%q err=%v", version, err)
	}
	newKey = historyPageKey(q, version)
	for range 2 {
		page, err := query(ctx, q)
		if err != nil || len(page.Items) != 1 || page.Items[0].ID != "new" {
			t.Fatalf("旧查询回填污染了新版本 page=%+v err=%v", page, err)
		}
	}
	if calls != 2 {
		t.Fatalf("换代后回源次数=%d，期望 2", calls)
	}
}

func TestHistoryCachePersistence(t *testing.T) {
	dsn := os.Getenv("GOLITUBE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("设置 GOLITUBE_TEST_MYSQL_DSN 启用真实 MySQL 缓存失效测试")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasSuffix(cfg.DBName, "_test") {
		t.Fatal("仅允许使用 _test 专用数据库")
	}
	rdb := testRedisClient(t)
	ctx := t.Context()
	db, err := openPersistenceDB(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := HistoryQuery{RoomID: "cache-db-" + rand.Text(), EndMS: 60000, Limit: 20}
	keys := []string{historyVersionKey(q.RoomID), historyPageKey(q, "0")}
	defer func() {
		rdb.Del(context.Background(), keys...)
		for _, table := range []string{"danmaku_history", "danmaku"} {
			if _, err := db.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE room_id = ?", q.RoomID); err != nil {
				t.Error(err)
			}
		}
	}()
	query := cachedHistory(rdb, func(ctx context.Context, q HistoryQuery) (HistoryPage, error) {
		return queryHistory(ctx, db, q)
	})
	if page, err := query(ctx, q); err != nil || len(page.Items) != 0 {
		t.Fatalf("初始历史 page=%+v err=%v", page, err)
	}
	model := Danmaku{Type: "danmaku", ID: rand.Text(), RoomID: q.RoomID, Content: "saved", PositionMS: 100, Color: "#ffffff", CreatedAtMS: time.Now().UnixMilli()}
	data, _ := json.Marshal(model)
	event := RoomEvent{ID: model.ID, RoomID: q.RoomID, Type: websocket.TextMessage, Data: data}
	invalidations := 0
	invalidate := func(ctx context.Context, room string) error {
		invalidations++
		// 使用连接池中的另一条连接，确认失效回调发生在提交以后。
		page, err := queryHistory(ctx, db, q)
		if err != nil || len(page.Items) != 1 || room != q.RoomID {
			t.Fatalf("失效前事务未提交或房间错误 page=%+v room=%q err=%v", page, room, err)
		}
		return invalidateHistory(ctx, rdb, room)
	}
	if err := saveRoomEventWithInvalidation(ctx, db, event, invalidate); err != nil {
		t.Fatal(err)
	}
	version := rdb.Get(ctx, historyVersionKey(q.RoomID)).Val()
	keys = append(keys, historyPageKey(q, version))
	if page, err := query(ctx, q); err != nil || len(page.Items) != 1 || page.Items[0].ID != model.ID {
		t.Fatalf("提交后仍命中旧空结果 page=%+v err=%v", page, err)
	}
	// 重复 ID 以原归档房间为准，使原房间的缓存失效。
	event.RoomID += "-conflict"
	if err := saveRoomEventWithInvalidation(ctx, db, event, invalidate); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := saveRoomEventWithInvalidation(canceled, db, event, invalidate); err == nil || invalidations != 2 {
		t.Fatal("数据库操作失败时不应使缓存失效")
	}
	if err := saveRoomEventWithInvalidation(ctx, db, event, func(context.Context, string) error {
		return errors.New("redis unavailable")
	}); err != nil {
		t.Fatalf("缓存失效失败不应撤回已提交事件: %v", err)
	}
}
