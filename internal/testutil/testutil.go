// Package testutil 提供跨包测试共用的 Redis 连接、会话和弹幕载荷。
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/redis/go-redis/v9"
)

func RedisClient(t *testing.T) *redis.Client {
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

// Redis 通信/故障测试直接准备独立会话；HTTP 注册登录另有端到端集成测试。
func SessionHeader(t *testing.T, addr string) http.Header {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	a := &auth.Handler{RDB: rdb, Namespace: "golitube"}
	token := rand.Text()
	payload, _ := json.Marshal(auth.User{ID: rand.Text(), Username: "integration_user"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, a.SessionKey(token), payload, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rdb.Del(ctx, a.SessionKey(token))
	})
	return http.Header{"Cookie": {auth.SessionCookie + "=" + token}}
}

func DanmakuPayload(t *testing.T, content string) []byte {
	t.Helper()
	position := int64(1234)
	payload, err := json.Marshal(danmaku.SendDanmaku{Content: content, PositionMS: &position})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
