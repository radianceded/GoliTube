package ratelimit

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/radianceded/GoliTube/internal/testutil"
)

func TestAllow(t *testing.T) {
	rdb := testutil.RedisClient(t)
	ctx := t.Context()
	key, other := "golitube:test:ratelimit:"+rand.Text(), "golitube:test:ratelimit:"+rand.Text()
	t.Cleanup(func() { rdb.Del(t.Context(), key, other) })

	const window = 300
	for i := 1; i <= 3; i++ {
		if ok, err := Allow(ctx, rdb, key, 3, window); err != nil || !ok {
			t.Fatalf("第 %d 次应通过: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, err := Allow(ctx, rdb, key, 3, window); err != nil || ok {
		t.Fatalf("超过上限应拒绝: ok=%v err=%v", ok, err)
	}
	if ok, err := Allow(ctx, rdb, other, 3, window); err != nil || !ok {
		t.Fatalf("不同 key 互不影响: ok=%v err=%v", ok, err)
	}
	if ttl := rdb.PTTL(ctx, key).Val(); ttl <= 0 || ttl > window*time.Millisecond {
		t.Fatalf("窗口键必须带过期时间: %v", ttl)
	}

	time.Sleep(window*time.Millisecond + 150*time.Millisecond)
	if ok, err := Allow(ctx, rdb, key, 3, window); err != nil || !ok {
		t.Fatalf("窗口结束后应恢复: ok=%v err=%v", ok, err)
	}

	// 键丢失过期时间时由下一次调用补上，不会永久封禁。
	if err := rdb.Set(ctx, other, 100, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := Allow(ctx, rdb, other, 3, window); err != nil {
		t.Fatal(err)
	}
	if ttl := rdb.PTTL(ctx, other).Val(); ttl <= 0 {
		t.Fatalf("没有补上过期时间: %v", ttl)
	}
}
