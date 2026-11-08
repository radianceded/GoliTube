package main

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

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
