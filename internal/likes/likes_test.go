package likes

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/testutil"
)

func TestRoomLikesConcurrent(t *testing.T) {
	rdb := testutil.RedisClient(t)
	a := &auth.Handler{RDB: rdb, Namespace: "golitube"}
	key := Key(a, "concurrent-"+rand.Text())
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
