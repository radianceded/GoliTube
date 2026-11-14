package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/persistence"
	"github.com/segmentio/kafka-go"
)

// 真实 Kafka：坏消息进入死信 topic 并带着来源信息，消费不会停在它身上，后面的合法事件继续处理。
// 使用随机 topic 和消费组，不影响开发用 topic。
func TestDeadLetterIntegration(t *testing.T) {
	brokersEnv := os.Getenv("GOLITUBE_TEST_KAFKA_BROKERS")
	if brokersEnv == "" {
		t.Skip("设置 GOLITUBE_TEST_KAFKA_BROKERS 启用真实 Kafka 测试")
	}
	brokers := strings.Split(brokersEnv, ",")
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	source, dead := "test-dlq-src-"+rand.Text(), "test-dlq-dead-"+rand.Text()
	for _, topic := range []string{source, dead} {
		if err := persistence.EnsureTopic(ctx, brokers, topic); err != nil {
			t.Fatal(err)
		}
		// 再次创建不报错，满足重复启动消费者的场景。
		if err := persistence.EnsureTopic(ctx, brokers, topic); err != nil {
			t.Fatalf("重复创建 %s: %v", topic, err)
		}
	}
	defer func() {
		conn, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		conn.DeleteTopics(source, dead)
	}()

	valid := danmaku.RoomEvent{ID: rand.Text(), RoomID: "dlq-room", Type: websocket.TextMessage, Data: []byte("ok")}
	validPayload, _ := json.Marshal(valid)
	poison := []byte(`{"id":"","room_id":"dlq-room","type":1}`)
	// 使用私有 Transport：kafka-go 的 DefaultTransport 缓存集群元数据，同一进程里后面新建 topic 的测试
	// 会因为缓存里没有这个 topic 而报 Unknown Topic Or Partition。
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	producer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: source, RequiredAcks: kafka.RequireAll, BatchSize: 1, Transport: transport}
	defer producer.Close()
	if err := producer.WriteMessages(ctx,
		kafka.Message{Key: []byte("dlq-room"), Value: poison},
		kafka.Message{Key: []byte("dlq-room"), Value: validPayload},
	); err != nil {
		t.Fatal(err)
	}

	deadWriter := persistence.NewDeadLetterWriter(brokers)
	deadWriter.Topic = dead
	deadWriter.Transport = transport
	defer deadWriter.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: source, GroupID: source,
		MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 500 * time.Millisecond, StartOffset: kafka.FirstOffset})
	defer reader.Close()

	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()
	saved := make(chan string, 4)
	done := make(chan error, 1)
	go func() {
		done <- persistence.ConsumeEvents(consumeCtx, reader.FetchMessage, func(_ context.Context, event danmaku.RoomEvent) error {
			saved <- event.ID
			return nil
		}, reader.CommitMessages, func(ctx context.Context, message kafka.Message, reason error) error {
			return persistence.DeadLetter(ctx, deadWriter, message, reason)
		})
	}()
	select {
	case id := <-saved:
		if id != valid.ID {
			t.Fatalf("saved %q, want %q", id, valid.ID)
		}
	case err := <-done:
		t.Fatalf("消费者在坏消息处退出，没有继续处理后面的事件: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("坏消息之后的合法事件没有被保存")
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Logf("consumer exit: %v", err)
	}

	deadReader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: dead, Partition: 0, MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 500 * time.Millisecond})
	defer deadReader.Close()
	deadReader.SetOffset(kafka.FirstOffset)
	message, err := deadReader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("死信 topic 没有收到坏消息: %v", err)
	}
	if string(message.Value) != string(poison) || string(message.Key) != "dlq-room" {
		t.Fatalf("死信没有保留原始内容: key=%q value=%q", message.Key, message.Value)
	}
	headers := map[string]string{}
	for _, h := range message.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["source-topic"] != source || headers["source-partition"] != "0" || headers["source-offset"] != "0" || headers["dlq-reason"] == "" {
		t.Fatalf("死信缺少来源信息: %v", headers)
	}
	if _, err := strconv.Atoi(headers["source-offset"]); err != nil {
		t.Fatal(err)
	}

	// 重启后同一个消费组不会再次读到坏消息，也不会重复转存：位点已提交到坏消息之后。
	// 并行读到 EOF 的方式不可靠，这里直接检查消费组位点。
	offsetCtx, stopOffsets := context.WithTimeout(ctx, 10*time.Second)
	defer stopOffsets()
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second, Transport: transport}
	response, err := client.OffsetFetch(offsetCtx, &kafka.OffsetFetchRequest{GroupID: source, Topics: map[string][]int{source: {0}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Topics[source]; len(got) != 1 || got[0].CommittedOffset != 2 {
		t.Fatalf("消费组位点应提交到 2（坏消息和合法消息之后），实际 %+v", got)
	}
}
