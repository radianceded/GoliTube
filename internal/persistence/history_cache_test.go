package persistence

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
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/history"
	"github.com/radianceded/GoliTube/internal/testutil"
)

func TestHistoryCachePersistence(t *testing.T) {
	dsn := os.Getenv("GOLITUBE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("设置 GOLITUBE_TEST_MYSQL_DSN 启用真实 MySQL 缓存失效测试")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasSuffix(cfg.DBName, "_test") {
		t.Fatal("仅允许使用 _test 专用数据库")
	}
	rdb := testutil.RedisClient(t)
	ctx := t.Context()
	db, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := history.Query{RoomID: "cache-db-" + rand.Text(), EndMS: 60000, Limit: 20}
	keys := []string{history.VersionKey(q.RoomID), history.PageKey(q, "0")}
	defer func() {
		rdb.Del(context.Background(), keys...)
		for _, table := range []string{"danmaku_history", "danmaku"} {
			if _, err := db.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE room_id = ?", q.RoomID); err != nil {
				t.Error(err)
			}
		}
	}()
	query := history.Cached(rdb, func(ctx context.Context, q history.Query) (history.Page, error) {
		return history.Fetch(ctx, db, q)
	})
	if page, err := query(ctx, q); err != nil || len(page.Items) != 0 {
		t.Fatalf("初始历史 page=%+v err=%v", page, err)
	}
	model := danmaku.Danmaku{Type: "danmaku", ID: rand.Text(), RoomID: q.RoomID, Content: "saved", PositionMS: 100, Color: "#ffffff", CreatedAtMS: time.Now().UnixMilli()}
	data, _ := json.Marshal(model)
	event := danmaku.RoomEvent{ID: model.ID, RoomID: q.RoomID, Type: websocket.TextMessage, Data: data}
	invalidations := 0
	invalidate := func(ctx context.Context, room string) error {
		invalidations++
		// 使用连接池中的另一条连接，确认失效回调发生在提交以后。
		page, err := history.Fetch(ctx, db, q)
		if err != nil || len(page.Items) != 1 || room != q.RoomID {
			t.Fatalf("失效前事务未提交或房间错误 page=%+v room=%q err=%v", page, room, err)
		}
		return history.Invalidate(ctx, rdb, room)
	}
	if err := saveRoomEventWithInvalidation(ctx, db, event, invalidate); err != nil {
		t.Fatal(err)
	}
	version := rdb.Get(ctx, history.VersionKey(q.RoomID)).Val()
	keys = append(keys, history.PageKey(q, version))
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
