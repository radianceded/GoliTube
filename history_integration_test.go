package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

func TestHistoryIntegration(t *testing.T) {
	dsn := os.Getenv("GOLITUBE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("设置 GOLITUBE_TEST_MYSQL_DSN 启用真实 MySQL 历史查询测试")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasSuffix(cfg.DBName, "_test") {
		t.Fatal("此测试只允许在名称以 _test 结尾的专用库运行")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := openPersistenceDB(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	room := "history-" + rand.Text()
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		for _, table := range []string{"danmaku_history", "danmaku"} {
			if _, err := db.ExecContext(cleanupCtx, "DELETE FROM "+table+" WHERE room_id IN (?,?)", room, room+"-other"); err != nil {
				t.Error(err)
			}
		}
	}()
	makeEvent := func(suffix string, position int64) RoomEvent {
		model := Danmaku{Type: "danmaku", ID: room + "-" + suffix, RoomID: room, Content: "弹幕 " + suffix, PositionMS: position, Color: "#ffffff", CreatedAtMS: time.Now().UnixMilli()}
		data, err := json.Marshal(model)
		if err != nil {
			t.Fatal(err)
		}
		return RoomEvent{ID: model.ID, RoomID: room, Type: websocket.TextMessage, Data: data}
	}
	archive := func(event RoomEvent) {
		t.Helper()
		if _, err := db.ExecContext(ctx, "INSERT INTO danmaku(id,room_id,message_type,data) VALUES(?,?,?,?)", event.ID, event.RoomID, event.Type, event.Data); err != nil {
			t.Fatal(err)
		}
	}
	positions := []int64{0, 5000, 5000, 9999, 10000}
	events := make([]RoomEvent, 0, len(positions))
	for i, position := range positions {
		event := makeEvent(fmt.Sprint(i), position)
		events = append(events, event)
		if i < 4 {
			archive(event)
		} else if err := saveRoomEvent(ctx, db, event); err != nil {
			t.Fatal(err)
		}
	}
	archive(RoomEvent{ID: room + "-legacy", RoomID: room, Type: websocket.TextMessage, Data: []byte("old plain message")})
	broken := makeEvent("mismatch", 100)
	broken.ID = room + "-outer-id"
	archive(broken)
	other := makeEvent("other", 100)
	other.RoomID = room + "-other"
	var otherModel Danmaku
	json.Unmarshal(other.Data, &otherModel)
	otherModel.RoomID = other.RoomID
	other.Data, _ = json.Marshal(otherModel)
	if err := saveRoomEvent(ctx, db, other); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, _, err := backfillHistory(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	// 重复 ID 的新载荷不能覆盖原归档，缺失的查询记录也必须按原归档重建。
	if _, err := db.ExecContext(ctx, "DELETE FROM danmaku_history WHERE id = ?", events[1].ID); err != nil {
		t.Fatal(err)
	}
	conflict := makeEvent("1", 1)
	if err := saveRoomEvent(ctx, db, conflict); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handleHistory(func(ctx context.Context, q HistoryQuery) (HistoryPage, error) { return queryHistory(ctx, db, q) }))
	defer server.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	get := func(start, end int64, limit int, cursor string) HistoryPage {
		t.Helper()
		values := url.Values{"room": {room}, "start_ms": {fmt.Sprint(start)}, "end_ms": {fmt.Sprint(end)}, "limit": {fmt.Sprint(limit)}}
		if cursor != "" {
			values.Set("cursor", cursor)
		}
		response, err := client.Get(server.URL + "/danmaku/history?" + values.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var page HistoryPage
		if response.StatusCode != 200 {
			t.Fatalf("status=%d", response.StatusCode)
		}
		if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	first := get(0, 10000, 2, "")
	if len(first.Items) != 2 || first.NextCursor == "" {
		t.Fatalf("unexpected first page: %+v", first)
	}
	if redisAddr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR"); redisAddr != "" {
		// 同时提供 Redis 时，额外检查真实服务入口的环境配置与路由绑定。
		t.Setenv("GOLITUBE_MYSQL_DSN", dsn)
		instance := startTestInstance(t, redisAddr)
		response, err := client.Get("http://" + instance + "/danmaku/history?room=" + room + "&start_ms=0&end_ms=10000&limit=2")
		if err != nil {
			t.Fatal(err)
		}
		var actual HistoryPage
		decodeErr := json.NewDecoder(response.Body).Decode(&actual)
		response.Body.Close()
		if response.StatusCode != 200 || decodeErr != nil || !reflect.DeepEqual(actual, first) {
			t.Fatalf("real service history route: status=%d page=%+v err=%v", response.StatusCode, actual, decodeErr)
		}
	}
	second := get(0, 10000, 2, first.NextCursor)
	if len(second.Items) != 2 || second.NextCursor != "" {
		t.Fatalf("unexpected second page: %+v", second)
	}
	all := append(first.Items, second.Items...)
	for i, event := range all {
		var want Danmaku
		if err := json.Unmarshal(events[i].Data, &want); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(event, want) {
			t.Fatalf("item=%+v want=%+v", event, want)
		}
	}
	boundary := get(10000, 10001, 200, "")
	if len(boundary.Items) != 1 || boundary.Items[0].ID != events[4].ID {
		t.Fatalf("bad exclusive boundary: %+v", boundary)
	}
	empty := get(20000, 30000, 200, "")
	if len(empty.Items) != 0 || empty.NextCursor != "" {
		t.Fatal("empty range is not empty")
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM danmaku_history WHERE room_id = ?", room).Scan(&count); err != nil || count != 5 {
		t.Fatalf("history rows=%d err=%v", count, err)
	}
	// 只对本测试 ID 注入一次查询表写入失败，检查归档写入也被事务回滚。
	constraint := "history_fail_" + rand.Text()
	failure := makeEvent("rollback", 500)
	ddl := "ALTER TABLE danmaku_history ADD CONSTRAINT " + constraint + " CHECK (id <> _binary'" + failure.ID + "')"
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if _, err := db.ExecContext(cleanupCtx, "ALTER TABLE danmaku_history DROP CHECK "+constraint); err != nil {
			t.Error(err)
		}
	}()
	if err := saveRoomEvent(ctx, db, failure); err == nil {
		t.Fatal("expected projection failure")
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM danmaku WHERE id = ?", failure.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("archive was not rolled back: rows=%d err=%v", count, err)
	}
	t.Log("真实 HTTP/MySQL：新写入、旧 JSON 回填、旧原始数据隔离、重复回填、同时间点分页、边界和事务回滚通过")
}
