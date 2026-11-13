package realtime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/redis/go-redis/v9"
)

const (
	presenceInterval = 3 * time.Second
	presenceLease    = 10 * time.Second
)

// 两个键共享 hash tag，便于以后使用 Redis Cluster。
func PresenceKeys(room string) []string {
	prefix := fmt.Sprintf("golitube:online:{%x}", sha256.Sum256([]byte(room)))
	return []string{prefix + ":counts", prefix + ":leases"}
}

const writePresenceScript = `
if tonumber(ARGV[2]) == 0 then
    redis.call('HDEL', KEYS[1], ARGV[1])
    redis.call('ZREM', KEYS[2], ARGV[1])
else
    local now = redis.call('TIME')
    local ms = now[1] * 1000 + math.floor(now[2] / 1000)
    for _, instance in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ms)) do
        redis.call('HDEL', KEYS[1], instance)
        redis.call('ZREM', KEYS[2], instance)
    end
    local expires = ms + tonumber(ARGV[3])
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    redis.call('ZADD', KEYS[2], expires, ARGV[1])
    redis.call('PEXPIRE', KEYS[1], ARGV[3] * 2)
    redis.call('PEXPIRE', KEYS[2], ARGV[3] * 2)
end
return 1`

var readPresenceScript = redis.NewScript(`
local now = redis.call('TIME')
local ms = now[1] * 1000 + math.floor(now[2] / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ms)
for _, instance in ipairs(expired) do
    redis.call('HDEL', KEYS[1], instance)
    redis.call('ZREM', KEYS[2], instance)
end
local total = 0
for _, count in ipairs(redis.call('HVALS', KEYS[1])) do
    total = total + tonumber(count)
end
return total`)

func (h *Hub) LocalRoomCounts() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	counts := make(map[string]int, len(h.rooms))
	for room, members := range h.rooms {
		counts[room] = len(members)
	}
	return counts
}

func writeRoomPresence(ctx context.Context, rdb *redis.Client, instance string, counts map[string]int) error {
	pipe := rdb.Pipeline()
	for room, count := range counts {
		pipe.Eval(ctx, writePresenceScript, PresenceKeys(room), instance, count, presenceLease.Milliseconds())
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (h *Hub) ReportPresence(ctx context.Context, rdb *redis.Client) {
	instance := rand.Text() // 每次启动独立身份，多个实例不会相互覆盖。
	ticker := time.NewTicker(presenceInterval)
	defer ticker.Stop()
	previous := make(map[string]int)
	for {
		counts := h.LocalRoomCounts()
		for room := range previous {
			if _, exists := counts[room]; !exists {
				counts[room] = 0 // 最后一条连接离开后，主动移除本实例的贡献。
			}
		}
		writeCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := writeRoomPresence(writeCtx, rdb, instance, counts)
		cancel()
		previous = counts
		if err != nil {
			log.Println("在线连接统计上报失败:", err)
		} else {
			for room, count := range previous {
				if count == 0 {
					delete(previous, room)
				}
			}
		}
		select {
		case <-ctx.Done():
			return // 停止后由租约回收，网络异常时也无需阻塞退出。
		case <-ticker.C:
		}
	}
}

func OnlineHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		fail := func(status int, code, message string) {
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(danmaku.ProtocolError{Type: "error", Code: code, Message: message})
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			fail(http.StatusMethodNotAllowed, "method_not_allowed", "仅支持 GET")
			return
		}
		room := strings.TrimSpace(r.URL.Query().Get("room"))
		if room == "" || len(room) > 128 || !utf8.ValidString(room) {
			fail(http.StatusBadRequest, "invalid_query", "room 必填，且须为不超过 128 字节的 UTF-8 房间名")
			return
		}
		if rdb == nil {
			fail(http.StatusServiceUnavailable, "online_unavailable", "在线统计暂不可用")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		count, err := readPresenceScript.Run(ctx, rdb, PresenceKeys(room)).Int64()
		if err != nil {
			fail(http.StatusServiceUnavailable, "online_unavailable", "在线统计暂不可用")
			return
		}
		json.NewEncoder(w).Encode(struct {
			RoomID string `json:"room_id"`
			Online int64  `json:"online_connections"`
		}{room, count})
	}
}
