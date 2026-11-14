// Package ratelimit 提供基于 Redis 的跨实例固定窗口计数限流。
package ratelimit

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// 窗口内第一次计数时设置过期；万一键没有过期时间（例如被其他命令清除），也会补上，避免永久封禁。
var allowScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 or redis.call('PTTL', KEYS[1]) < 0 then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return n`)

// Allow 记录 key 的一次尝试，返回窗口内的次数是否仍在 limit 以内。
// 固定窗口在边界处最多允许 2 倍 limit 的突发，对防刷已经足够。被拒绝的尝试同样计数，
// 但不会延长窗口：过期时间只在窗口内第一次计数时设置，之后额度整体恢复。
func Allow(ctx context.Context, rdb *redis.Client, key string, limit int64, windowMS int64) (bool, error) {
	count, err := allowScript.Run(ctx, rdb, []string{key}, windowMS).Int64()
	if err != nil {
		return false, err
	}
	return count <= limit, nil
}
