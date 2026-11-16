package persistence

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/segmentio/kafka-go"
)

func TestBatchPersistenceIntegration(t *testing.T) {
	dsn := os.Getenv("GOLITUBE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("设置 GOLITUBE_TEST_MYSQL_DSN 启用批量事务测试")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasSuffix(cfg.DBName, "_test") {
		t.Fatal("仅允许 _test 专用数据库")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	room := "batch-" + rand.Text()
	makeEvent := func(id, content, author string) danmaku.RoomEvent {
		value := danmaku.Danmaku{ID: id, Type: "danmaku", RoomID: room, Content: content, UserID: author, Color: "#ffffff", PositionMS: 100, CreatedAtMS: time.Now().UnixMilli()}
		data, _ := json.Marshal(value)
		return danmaku.RoomEvent{ID: id, RoomID: room, Type: 1, Data: data}
	}
	defer func() {
		for _, table := range []string{"danmaku_history", "danmaku"} {
			if _, err := db.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE room_id = ?", room); err != nil {
				t.Error(err)
			}
		}
	}()
	original := makeEvent(room+"-original", "original", "")
	duplicate := makeEvent(original.ID, "forged", "forged-author")
	duplicate.RoomID = room + "-wrong-room"
	second := makeEvent(room+"-second", "second", "author")
	empty := danmaku.RoomEvent{ID: room + "-empty", RoomID: room, Type: 2}
	invalidations := 0
	invalidate := func(ctx context.Context, rooms []string) error {
		invalidations++
		if !reflect.DeepEqual(rooms, []string{room}) {
			t.Fatalf("wrong rooms: %v", rooms)
		}
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM danmaku WHERE room_id = ?", room).Scan(&count); err != nil || count != 3 {
			t.Fatalf("invalidation before commit: rows=%d err=%v", count, err)
		}
		return nil
	}
	if err := saveRoomEventsWithInvalidation(ctx, db, []danmaku.RoomEvent{original, duplicate, second, empty}, invalidate); err != nil {
		t.Fatal(err)
	}
	// 删除历史投影模拟修复/回填，重复载荷不能替换原归档的内容、房间和空作者。
	if _, err := db.ExecContext(ctx, "DELETE FROM danmaku_history WHERE id = ?", original.ID); err != nil {
		t.Fatal(err)
	}
	if err := saveRoomEventsWithInvalidation(ctx, db, []danmaku.RoomEvent{duplicate, second}, invalidate); err != nil {
		t.Fatal(err)
	}
	var content string
	var author *string
	if err := db.QueryRowContext(ctx, "SELECT content,user_id FROM danmaku_history WHERE id = ?", original.ID).Scan(&content, &author); err != nil || content != "original" || author != nil {
		t.Fatalf("content=%q author=%v err=%v", content, author, err)
	}
	var emptyData []byte
	if err := db.QueryRowContext(ctx, "SELECT data FROM danmaku WHERE id = ?", empty.ID).Scan(&emptyData); err != nil || emptyData == nil || len(emptyData) != 0 {
		t.Fatalf("empty data=%v err=%v", emptyData, err)
	}
	if invalidations != 2 {
		t.Fatalf("invalidations=%d", invalidations)
	}

	// 投影中任意一行失败，整批归档及投影都必须回滚，且不能失效缓存。
	good := makeEvent(room+"-rollback-good", "good", "")
	bad := makeEvent(room+"-rollback-bad", "bad", "")
	constraint := "batch_fail_" + rand.Text()
	if _, err := db.ExecContext(ctx, "ALTER TABLE danmaku_history ADD CONSTRAINT "+constraint+" CHECK (id <> _binary'"+bad.ID+"')"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := db.ExecContext(context.Background(), "ALTER TABLE danmaku_history DROP CHECK "+constraint); err != nil {
			t.Error(err)
		}
	}()
	if err := saveRoomEventsWithInvalidation(ctx, db, []danmaku.RoomEvent{good, bad}, invalidate); err == nil {
		t.Fatal("expected projection failure")
	}
	for _, table := range []string{"danmaku", "danmaku_history"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE id IN (?,?)", good.ID, bad.ID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
	if invalidations != 2 {
		t.Fatal("invalidated failed transaction")
	}

	// 另一事务先插入但尚未提交，批量写入发生竞争后仍投影数据库胜出的原始行。
	winner := makeEvent(room+"-concurrent", "winner", "")
	loser := makeEvent(winner.ID, "loser", "wrong")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO danmaku (id,room_id,message_type,data) VALUES (?,?,?,?)", winner.ID, winner.RoomID, winner.Type, winner.Data); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- saveRoomEventsWithInvalidation(ctx, db, []danmaku.RoomEvent{loser}, nil) }()
	select {
	case err := <-done:
		t.Fatalf("conflicting writer did not wait: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT content,user_id FROM danmaku_history WHERE id = ?", winner.ID).Scan(&content, &author); err != nil || content != "winner" || author != nil {
		t.Fatalf("concurrent content=%q author=%v err=%v", content, author, err)
	}
}

func TestBatchKafkaIntegration(t *testing.T) {
	brokersEnv, dsn := os.Getenv("GOLITUBE_TEST_KAFKA_BROKERS"), os.Getenv("GOLITUBE_TEST_MYSQL_DSN")
	if brokersEnv == "" || dsn == "" {
		t.Skip("设置 Kafka 和 MySQL 测试环境启用批量端到端测试")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasSuffix(cfg.DBName, "_test") {
		t.Fatal("仅允许 _test 专用数据库")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	brokers := strings.Split(brokersEnv, ",")
	topic := "batch-test-" + rand.Text()
	for _, name := range []string{topic, topic + "-dlq"} {
		if err := EnsureTopic(ctx, brokers, name); err != nil {
			t.Fatal(err)
		}
	}
	bootstrap, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
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
	defer func() {
		admin.SetDeadline(time.Now().Add(5 * time.Second))
		if err := admin.DeleteTopics(topic, topic+"-dlq"); err != nil {
			t.Error(err)
		}
	}()
	db, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer func() {
		for _, table := range []string{"danmaku_history", "danmaku"} {
			if _, err := db.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE room_id = ?", topic); err != nil {
				t.Error(err)
			}
		}
	}()
	writer := NewEventWriter(brokers)
	writer.Topic = topic
	defer writer.Close()
	for i := 0; i < 3; i++ {
		data, _ := json.Marshal(danmaku.Danmaku{Type: "danmaku", ID: fmt.Sprint(topic, i), RoomID: topic, Content: "batch", Color: "#ffffff", PositionMS: 100, CreatedAtMS: time.Now().UnixMilli()})
		event := danmaku.RoomEvent{ID: fmt.Sprint(topic, i), RoomID: topic, Type: 1, Data: data}
		if err := EnqueueRoomEvent(ctx, writer, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Value: []byte("{")}); err != nil {
		t.Fatal(err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: topic, MinBytes: 1, MaxBytes: 10 << 20, StartOffset: kafka.FirstOffset})
	defer reader.Close()
	dlqWriter := NewDeadLetterWriter(brokers)
	dlqWriter.Topic = topic + "-dlq"
	defer dlqWriter.Close()
	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()
	committed, saved, dead := 0, 0, 0
	err = ConsumeBatches(consumeCtx, reader.FetchMessage, func(ctx context.Context, events []danmaku.RoomEvent) error {
		if err := saveRoomEventsWithInvalidation(ctx, db, events, nil); err != nil {
			return err
		}
		saved += len(events)
		return nil
	}, func(ctx context.Context, messages ...kafka.Message) error {
		if err := reader.CommitMessages(ctx, messages...); err != nil {
			return err
		}
		committed += len(messages)
		if committed == 4 {
			stop()
		}
		return nil
	}, func(ctx context.Context, message kafka.Message, reason error) error {
		if err := DeadLetter(ctx, dlqWriter, message, reason); err != nil {
			return err
		}
		dead++
		return nil
	}, 100)
	if !errors.Is(err, context.Canceled) || committed != 4 || saved != 3 || dead != 1 {
		t.Fatalf("err=%v committed=%d saved=%d dead=%d", err, committed, saved, dead)
	}
	for _, table := range []string{"danmaku", "danmaku_history"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE room_id = ?", topic).Scan(&count); err != nil || count != 3 {
			t.Fatalf("%s rows=%d err=%v", table, count, err)
		}
	}
	dlqReader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic + "-dlq", Partition: 0, MinBytes: 1, MaxBytes: 10 << 20})
	defer dlqReader.Close()
	message, err := dlqReader.FetchMessage(ctx)
	if err != nil || string(message.Value) != "{" || len(message.Headers) != 4 {
		t.Fatalf("dead letter=%+v err=%v", message, err)
	}
}
