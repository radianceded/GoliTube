package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

type RoomLikes struct {
	RoomID string `json:"room_id"`
	Count  int64  `json:"likes"`
	Liked  bool   `json:"liked"`
}

func (a *authHandler) roomLikesKey(room string) string {
	return a.namespace + ":room-likes:v1:" + digest(room)
}

// 集合同时保存去重关系和计数。修改与返回状态在同一个脚本内完成。
var roomLikesScript = redis.NewScript(`
if ARGV[1] == 'like' then
    redis.call('SADD', KEYS[1], ARGV[2])
elseif ARGV[1] == 'unlike' then
    redis.call('SREM', KEYS[1], ARGV[2])
end
return {redis.call('SCARD', KEYS[1]), redis.call('SISMEMBER', KEYS[1], ARGV[2])}`)

func handleRoomLikes(a *authHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store") // liked 与当前 Cookie 身份有关。
		if r.Method != http.MethodGet && r.Method != http.MethodPut {
			w.Header().Set("Allow", "GET, PUT")
			authError(w, 405, "method_not_allowed", "仅支持 GET 和 PUT")
			return
		}
		room := strings.TrimSpace(r.URL.Query().Get("room"))
		if room == "" || len(room) > 128 || !utf8.ValidString(room) {
			authError(w, 400, "invalid_query", "room 必填，且须为不超过 128 字节的 UTF-8 房间名")
			return
		}
		if r.Method == http.MethodPut && !sameOrigin(r, a.secureCookie) {
			authError(w, 403, "origin_rejected", "不接受跨站点赞操作")
			return
		}
		if a.rdb == nil {
			authError(w, 503, "likes_unavailable", "点赞服务暂不可用")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		user, err := a.currentUser(ctx, r)
		if errors.Is(err, redis.Nil) {
			if r.Method == http.MethodPut {
				authError(w, 401, "unauthenticated", "请先登录")
				return
			}
			user = User{}
		} else if err != nil {
			authError(w, 503, "likes_unavailable", "点赞服务暂不可用")
			return
		}
		action := "read"
		if r.Method == http.MethodPut {
			var input struct {
				Liked *bool `json:"liked"`
			}
			decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&input); err != nil || input.Liked == nil || decoder.Decode(new(any)) != io.EOF {
				authError(w, 400, "invalid_like", "仅接受包含布尔值 liked 的 JSON 对象")
				return
			}
			action = "unlike"
			if *input.Liked {
				action = "like"
			}
		}
		values, err := roomLikesScript.Run(ctx, a.rdb, []string{a.roomLikesKey(room)}, action, user.ID).Int64Slice()
		if err != nil || len(values) != 2 {
			authError(w, 503, "likes_unavailable", "点赞服务暂不可用")
			return
		}
		authJSON(w, 200, RoomLikes{RoomID: room, Count: values[0], Liked: values[1] == 1})
	}
}
