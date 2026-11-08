package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRoomLikesValidation(t *testing.T) {
	router := newRouter(&authHandler{}, nil, localPublish)
	for _, test := range []struct {
		method, path, origin string
		status               int
	}{
		{"POST", "/rooms/likes?room=valid", "", 405},
		{"GET", "/rooms/likes", "", 400},
		{"GET", "/rooms/likes?room=%FF", "", 400},
		{"GET", "/rooms/likes?room=" + strings.Repeat("x", 129), "", 400},
		{"PUT", "/rooms/likes?room=valid", "https://evil.example", 403},
		{"PUT", "/rooms/likes?room=valid", "null", 403},
		{"GET", "/rooms/likes?room=valid", "", 503},
	} {
		r := httptest.NewRequest(test.method, "http://example.com"+test.path, nil)
		r.Header.Set("Origin", test.origin)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != test.status || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, w.Code, w.Body.String())
		}
		if test.status == 405 && w.Header().Get("Allow") != "GET, PUT" {
			t.Fatal("错误的 Allow 响应")
		}
	}
}

func TestRoomLikesIntegration(t *testing.T) {
	rdb := testRedisClient(t)
	auth := &authHandler{rdb: rdb, namespace: "golitube"}
	router := newRouter(auth, nil, localPublish)
	room := "likes-" + rand.Text()
	other := room + "-other"
	t.Cleanup(func() { rdb.Del(context.Background(), auth.roomLikesKey(room), auth.roomLikesKey(other)) })
	first := testSessionHeader(t, os.Getenv("GOLITUBE_TEST_REDIS_ADDR"))
	second := testSessionHeader(t, os.Getenv("GOLITUBE_TEST_REDIS_ADDR"))
	request := func(method, room, body string, headers http.Header, status int) RoomLikes {
		t.Helper()
		r := httptest.NewRequest(method, "/rooms/likes?room="+room, strings.NewReader(body))
		r.Header = headers.Clone()
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s got=%d want=%d body=%s", method, w.Code, status, w.Body.String())
		}
		var likes RoomLikes
		if status == 200 && json.Unmarshal(w.Body.Bytes(), &likes) != nil {
			t.Fatal("无效的点赞响应")
		}
		return likes
	}
	if got := request("GET", room, "", nil, 200); got.Count != 0 || got.Liked {
		t.Fatal(got)
	}
	request("PUT", room, `{"liked":true}`, nil, 401)
	for _, body := range []string{`{}`, `null`, `{"liked":null}`, `{"liked":1}`, `{"liked":true,"user_id":"forged"}`, `{"liked":true} {}`, strings.Repeat(" ", 1025) + `{"liked":true}`} {
		request("PUT", room, body, first, 400)
	}
	for range 2 {
		if got := request("PUT", room, `{"liked":true}`, first, 200); got.Count != 1 || !got.Liked {
			t.Fatal("重复点赞失去幂等性", got)
		}
	}
	if got := request("GET", room, "", nil, 200); got.Count != 1 || got.Liked {
		t.Fatal("游客不应继承他人的点赞状态", got)
	}
	if got := request("PUT", room, `{"liked":true}`, second, 200); got.Count != 2 || !got.Liked {
		t.Fatal(got)
	}
	if got := request("GET", other, "", first, 200); got.Count != 0 || got.Liked {
		t.Fatal("房间点赞串房", got)
	}
	for range 2 {
		if got := request("PUT", room, `{"liked":false}`, first, 200); got.Count != 1 || got.Liked {
			t.Fatal("重复取消失去幂等性", got)
		}
	}
	// 另一个实例读取同一 Redis 后，能得到总数与当前用户的状态。
	peer := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/rooms/likes?room="+room, nil)
	r.Header = second.Clone()
	newRouter(&authHandler{rdb: rdb, namespace: "golitube"}, nil, localPublish).ServeHTTP(peer, r)
	var got RoomLikes
	if json.Unmarshal(peer.Body.Bytes(), &got) != nil || peer.Code != 200 || got.Count != 1 || !got.Liked {
		t.Fatal("跨实例查询失败", peer.Body.String())
	}
	request("POST", "", "", first, 405) // 错误方法先于参数校验。
	// 退出后，同一 Cookie 再修改会被拒绝。
	logout := httptest.NewRequest("POST", "/auth/logout", nil)
	logout.Header = first.Clone()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, logout)
	if w.Code != 204 {
		t.Fatal("退出失败")
	}
	request("PUT", room, `{"liked":true}`, first, 401)
	unavailable := redis.NewClient(&redis.Options{Addr: os.Getenv("GOLITUBE_TEST_REDIS_ADDR")})
	unavailable.Close()
	router = newRouter(&authHandler{rdb: unavailable, namespace: "golitube"}, nil, localPublish)
	request("GET", room, "", second, 503)
}

func TestRoomLikesConcurrent(t *testing.T) {
	rdb := testRedisClient(t)
	auth := &authHandler{rdb: rdb, namespace: "golitube"}
	key := auth.roomLikesKey("concurrent-" + rand.Text())
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	for _, action := range []string{"like", "unlike"} {
		var workers sync.WaitGroup
		for range 30 {
			workers.Go(func() {
				if _, err := roomLikesScript.Run(t.Context(), rdb, []string{key}, action, "same-user").Result(); err != nil {
					t.Error(err)
				}
			})
		}
		workers.Wait()
		want := int64(0)
		if action == "like" {
			want = 1
		}
		if count := rdb.SCard(t.Context(), key).Val(); count != want {
			t.Fatalf("并发 %s 后 count=%d want=%d", action, count, want)
		}
	}
	var workers sync.WaitGroup
	for i := range 30 {
		workers.Go(func() {
			if _, err := roomLikesScript.Run(t.Context(), rdb, []string{key}, "like", fmt.Sprint(i)).Result(); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if count := rdb.SCard(t.Context(), key).Val(); count != 30 {
		t.Fatalf("不同用户并发点赞 count=%d", count)
	}
}
