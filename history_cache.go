package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const historyCacheTTL = 10 * time.Second

func historyVersionKey(room string) string {
	return fmt.Sprintf("golitube:history:v1:%x:version", sha256.Sum256([]byte(room)))
}

func historyPageKey(q HistoryQuery, version string) string {
	payload, _ := json.Marshal(q)
	return fmt.Sprintf("golitube:history:v1:page:%x:%s", sha256.Sum256(payload), version)
}

// 只缓存对齐 30 秒的 30/60 秒窗口第一页，限制任意游标产生的低复用缓存。
func cachedHistory(rdb *redis.Client, query func(context.Context, HistoryQuery) (HistoryPage, error)) func(context.Context, HistoryQuery) (HistoryPage, error) {
	return func(ctx context.Context, q HistoryQuery) (HistoryPage, error) {
		window := q.EndMS - q.StartMS
		if q.AfterID != "" || q.StartMS%30000 != 0 || (window != 30000 && window != 60000) {
			return query(ctx, q)
		}
		cacheCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		version, err := rdb.Get(cacheCtx, historyVersionKey(q.RoomID)).Result()
		if err == redis.Nil {
			version, err = "0", nil
		}
		key := historyPageKey(q, version)
		if err == nil {
			var payload []byte
			payload, err = rdb.Get(cacheCtx, key).Bytes()
			var page HistoryPage
			if err == nil && json.Unmarshal(payload, &page) == nil {
				cancel()
				return page, nil
			}
		}
		cancel()
		// Redis 故障时直接回源，沿用历史接口自身的超时和错误响应。
		cacheAvailable := err == nil || err == redis.Nil
		started := time.Now()
		page, err := query(ctx, q)
		if err != nil || !cacheAvailable {
			return page, err
		}
		ttl := historyCacheTTL
		if len(page.Items) == 0 {
			ttl = 2 * time.Second
		}
		// 查询耗时也计入有效期，慢查询不会延长旧快照的存活时间。
		ttl -= time.Since(started)
		if payload, marshalErr := json.Marshal(page); marshalErr == nil && ttl > 0 {
			writeCtx, stop := context.WithTimeout(ctx, 150*time.Millisecond)
			defer stop()
			// 缓存写失败不影响已成功的 MySQL 查询。
			rdb.Set(writeCtx, key, payload, ttl)
		}
		return page, nil
	}
}
