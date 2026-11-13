package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/persistence"
	"github.com/segmentio/kafka-go"
)

// 使用专用测试数据库；创建并仅清理本测试拥有的 topic 和行。
func TestAsyncPersistenceIntegration(t *testing.T) {
	brokersEnv, dsn := os.Getenv("GOLITUBE_TEST_KAFKA_BROKERS"), os.Getenv("GOLITUBE_TEST_MYSQL_DSN")
	if brokersEnv == "" || dsn == "" {
		t.Skip("设置 GOLITUBE_TEST_KAFKA_BROKERS 和 GOLITUBE_TEST_MYSQL_DSN 启用真实 Kafka/MySQL 测试")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	brokers := strings.Split(brokersEnv, ",")
	db, err := persistence.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	room := "test-" + rand.Text()
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if _, err := db.ExecContext(cleanupCtx, "DELETE FROM danmaku WHERE room_id = ?", room); err != nil {
			t.Error(err)
		}
	}()

	// 独立 topic，避免重放或删除开发用 topic 中的消息。
	bootstrap, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		bootstrap.Close()
		t.Fatal(err)
	}
	controller, err := bootstrap.Controller()
	bootstrap.Close()
	if err != nil {
		t.Fatal(err)
	}
	admin, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, fmt.Sprint(controller.Port)))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := admin.CreateTopics(kafka.TopicConfig{Topic: room, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		admin.SetDeadline(time.Now().Add(5 * time.Second))
		if err := admin.DeleteTopics(room); err != nil {
			t.Error(err)
		}
	}()
	writer := persistence.NewEventWriter(brokers)
	writer.Topic = room
	defer writer.Close()
	accepted := make(chan danmaku.RoomEvent, 1)
	server := httptest.NewServer(handleTransportWebSocket(persistence.DurablePublish(func(ctx context.Context, event danmaku.RoomEvent) error {
		err := persistence.EnqueueRoomEvent(ctx, writer, event)
		if err == nil {
			accepted <- event
		}
		return err
	}, localPublish))) // 此测试将 Redis 转发替换为本地转发，Redis 自身另有集成测试。
	defer server.Close()
	defer waitForRoomCount(t, room, 0)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte{0, 255}); err != nil {
		t.Fatal(err)
	}
	kind, body, err := conn.ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || string(body) != string([]byte{0, 255}) {
		t.Fatalf("type=%d body=%v err=%v", kind, body, err)
	}
	event := <-accepted
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM danmaku WHERE id = ?", event.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("consumer has not started: rows=%d err=%v", count, err)
	}
	// 第二条为空消息，验证空 BLOB；Kafka 已接收时消费者尚未运行。
	empty := danmaku.RoomEvent{ID: rand.Text(), RoomID: room, Type: websocket.TextMessage}
	if err := persistence.EnqueueRoomEvent(ctx, writer, empty); err != nil {
		t.Fatal(err)
	}
	newReader := func() *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: room, GroupID: room,
			MinBytes: 1, MaxBytes: 10 << 20, StartOffset: kafka.FirstOffset})
	}
	reader := newReader()
	commitFailure := errors.New("simulate exit after database write, before offset commit")
	err = persistence.ConsumeEvents(ctx, reader.FetchMessage, func(ctx context.Context, got danmaku.RoomEvent) error {
		return persistence.SaveRoomEvent(ctx, db, got)
	}, func(context.Context, ...kafka.Message) error { return commitFailure })
	reader.Close()
	if !errors.Is(err, commitFailure) {
		t.Fatalf("expected simulated commit failure: %v", err)
	}
	// 用同一消费组重启：第一条会重放，但数据库中仍只有一行。
	reader = newReader()
	defer reader.Close()
	for _, want := range []danmaku.RoomEvent{event, empty} {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var got danmaku.RoomEvent
		if err := json.Unmarshal(message.Value, &got); err != nil || got.ID != want.ID {
			t.Fatalf("unexpected replay: event=%+v err=%v", got, err)
		}
		if err := persistence.SaveRoomEvent(ctx, db, got); err != nil {
			t.Fatal(err)
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			t.Fatal(err)
		}
		var data []byte
		var savedType int
		if err := db.QueryRowContext(ctx, "SELECT data, message_type FROM danmaku WHERE id = ?", want.ID).Scan(&data, &savedType); err != nil || string(data) != string(want.Data) || savedType != want.Type {
			t.Fatalf("saved data=%v type=%d err=%v", data, savedType, err)
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM danmaku WHERE room_id = ?", room).Scan(&count); err != nil || count != 2 {
		t.Fatalf("duplicate replay inserted extra rows: rows=%d err=%v", count, err)
	}
}
