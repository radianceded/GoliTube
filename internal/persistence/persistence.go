package persistence

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/history"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const persistenceTopic = "golitube-danmaku"
const persistenceGroup = "golitube-persistence-v1"

// 生产消费入口最多积累 1000 条、每条 Data 不超过 4KiB，限制事务与 SQL 大小。
const maxBatchSize = 1000

// 达到 1MiB 原始载荷后结束收集，最多额外接收一条消息，限制坏消息批次的内存。
const maxBatchBytes = 1 << 20

// 无法解析的事件转存到这个 topic，由人工排查。
const deadLetterTopic = "golitube-danmaku-dlq"
const deadLetterRetentionMS = "2592000000" // 30 天

//go:embed schema.sql
var persistenceSchema string

func NewEventWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        persistenceTopic,
		Balancer:     &kafka.Hash{}, // 同一房间使用同一个分区键。
		RequiredAcks: kafka.RequireAll,
		Async:        false, // 等待 Kafka 确认接收，不等待 MySQL 写入。
		BatchSize:    1,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxAttempts:  3,
	}
}

func NewDeadLetterWriter(brokers []string) *kafka.Writer {
	writer := NewEventWriter(brokers)
	writer.Topic = deadLetterTopic
	writer.Balancer = nil
	writer.BatchBytes = 10 << 20 // 与 NewEventReader 的 MaxBytes 一致，保证读得到的消息都写得进去。
	return writer
}

// 保留原始键和值，并在消息头记录原因与来源位置，便于排查和重放。
func DeadLetter(ctx context.Context, writer *kafka.Writer, message kafka.Message, reason error) error {
	return writer.WriteMessages(ctx, kafka.Message{
		Key:   message.Key,
		Value: message.Value,
		Headers: []kafka.Header{
			{Key: "dlq-reason", Value: []byte(reason.Error())},
			{Key: "source-topic", Value: []byte(message.Topic)},
			{Key: "source-partition", Value: []byte(strconv.Itoa(message.Partition))},
			{Key: "source-offset", Value: []byte(strconv.FormatInt(message.Offset, 10))},
		},
	})
}

// 重复调用安全：topic 已存在时直接返回。生产环境的 Compose 初始化只创建主 topic，
// 由消费者启动时补建死信 topic，避免发布顺序依赖手工操作。
// 创建 topic 必须发给集群的 controller，所以先通过任一 broker 查询它的地址。
func EnsureTopic(ctx context.Context, brokers []string, topic string, config ...kafka.ConfigEntry) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	var lastErr error
	for _, broker := range brokers {
		bootstrap, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}
		controller, err := bootstrap.Controller()
		bootstrap.Close()
		if err != nil {
			lastErr = err
			continue
		}
		admin, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			lastErr = err
			continue
		}
		defer admin.Close()
		if deadline, ok := ctx.Deadline(); ok {
			admin.SetDeadline(deadline)
		}
		err = admin.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1, ConfigEntries: config})
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			return err
		}
		return nil
	}
	return fmt.Errorf("无法连接 Kafka 创建 topic %q: %w", topic, lastErr)
}

func EnsureDeadLetterTopic(ctx context.Context, brokers []string) error {
	return EnsureTopic(ctx, brokers, deadLetterTopic, kafka.ConfigEntry{ConfigName: "retention.ms", ConfigValue: deadLetterRetentionMS})
}

func EnqueueRoomEvent(ctx context.Context, writer *kafka.Writer, event danmaku.RoomEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.RoomID), Value: payload})
}

func DurablePublish(enqueue, publish func(context.Context, danmaku.RoomEvent) error) func(context.Context, danmaku.RoomEvent) error {
	return func(ctx context.Context, event danmaku.RoomEvent) error {
		if err := enqueue(ctx, event); err != nil {
			return fmt.Errorf("Kafka 接收弹幕失败: %w", err)
		}
		// 两次写入不是一个事务：此处失败也不撤回已进入 Kafka 的任务。
		if err := publish(ctx, event); err != nil {
			return fmt.Errorf("弹幕已进入 Kafka，但 Redis 广播失败: %w", err)
		}
		return nil
	}
}

func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := NewDB(dsn)
	if err != nil {
		return nil, err
	}
	setupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// schema.sql 只含静态建表语句，不开启驱动的 multiStatements。
	for _, statement := range strings.Split(persistenceSchema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(setupCtx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("初始化弹幕表失败: %w", err)
		}
	}
	// CREATE TABLE IF NOT EXISTS 不升级已有表。旧弹幕保留 NULL 作者，不猜测身份。
	var hasAuthor int
	if err := db.QueryRowContext(setupCtx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'danmaku_history' AND column_name = 'user_id'`).Scan(&hasAuthor); err != nil {
		db.Close()
		return nil, fmt.Errorf("检查历史表作者字段失败: %w", err)
	}
	if hasAuthor == 0 {
		_, err := db.ExecContext(setupCtx, "ALTER TABLE danmaku_history ADD COLUMN user_id VARBINARY(128) NULL")
		var duplicate *mysql.MySQLError
		// 多个消费者同时启动时，另一个进程可能已添加字段。
		if err != nil && !(errors.As(err, &duplicate) && duplicate.Number == 1060) {
			db.Close()
			return nil, fmt.Errorf("升级历史表作者字段失败: %w", err)
		}
	}
	return db, nil
}

// 创建连接池不访问数据库，历史查询暂不可用时不阻止实时服务启动。
func NewDB(dsn string) (*sql.DB, error) {
	if dsn == "" {
		return nil, errors.New("请设置 GOLITUBE_MYSQL_DSN")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		// 不在日志中输出可能含密码的 DSN。
		return nil, errors.New("GOLITUBE_MYSQL_DSN 格式错误")
	}
	cfg.Timeout = 3 * time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, errors.New("MySQL 连接配置无效")
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(3 * time.Minute)
	return db, nil
}

func SaveRoomEvent(ctx context.Context, db *sql.DB, event danmaku.RoomEvent) error {
	return saveRoomEventWithInvalidation(ctx, db, event, nil)
}

func saveRoomEventWithInvalidation(ctx context.Context, db *sql.DB, event danmaku.RoomEvent, invalidate func(context.Context, string) error) error {
	var invalidateRooms func(context.Context, []string) error
	if invalidate != nil {
		invalidateRooms = func(ctx context.Context, rooms []string) error {
			for _, room := range rooms {
				if err := invalidate(ctx, room); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return saveRoomEventsWithInvalidation(ctx, db, []danmaku.RoomEvent{event}, invalidateRooms)
}

// 一批归档与历史投影在同一个事务中提交。先插入，再读取数据库中的原归档，
// 保证批内重复、重试及并发冲突都以最初保存的事件为准。
func saveRoomEventsWithInvalidation(ctx context.Context, db *sql.DB, events []danmaku.RoomEvent, invalidate func(context.Context, []string) error) error {
	if len(events) == 0 {
		return nil
	}
	if len(events) > maxBatchSize {
		return fmt.Errorf("批次不能超过 %d 条", maxBatchSize)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var query strings.Builder
	query.WriteString("INSERT INTO danmaku (id, room_id, message_type, data) VALUES ")
	args := make([]any, 0, len(events)*4)
	ids := make([]any, 0, len(events))
	seenIDs := make(map[string]bool, len(events))
	for _, event := range events {
		if seenIDs[event.ID] {
			continue
		}
		seenIDs[event.ID] = true
		if len(ids) > 0 {
			query.WriteString(",")
		}
		query.WriteString("(?,?,?,?)")
		data := event.Data
		if data == nil {
			data = []byte{}
		}
		args = append(args, event.ID, event.RoomID, event.Type, data)
		ids = append(ids, event.ID)
	}
	query.WriteString(" ON DUPLICATE KEY UPDATE id = id")
	if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
		return err
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	// 锁定读使用当前已保存的行，不依赖事务快照或客户端提交的重复载荷。
	rows, err := tx.QueryContext(ctx, "SELECT id, room_id, message_type, data FROM danmaku WHERE id IN ("+placeholders+") FOR UPDATE", ids...)
	if err != nil {
		return err
	}
	stored := make([]danmaku.RoomEvent, 0, len(ids))
	for rows.Next() {
		var event danmaku.RoomEvent
		if err := rows.Scan(&event.ID, &event.RoomID, &event.Type, &event.Data); err != nil {
			rows.Close()
			return err
		}
		stored = append(stored, event)
	}
	readErr := rows.Err()
	rows.Close()
	if readErr != nil {
		return readErr
	}
	if _, err := history.ProjectBatch(ctx, tx, stored); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if invalidate != nil {
		rooms := make([]string, 0, len(stored))
		seen := make(map[string]bool, len(stored))
		for _, event := range stored {
			if !seen[event.RoomID] {
				seen[event.RoomID] = true
				rooms = append(rooms, event.RoomID)
			}
		}
		if err := invalidate(ctx, rooms); err != nil {
			log.Println("历史缓存失效失败，等待 TTL 回收:", err)
		}
	}
	return nil
}

func NewEventReader(brokers []string, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: persistenceTopic, GroupID: group,
		MinBytes: 1, MaxBytes: 10 << 20, MaxWait: time.Second,
		StartOffset: kafka.FirstOffset,
		// CommitInterval 默认 0：CommitMessages 等待提交结果。
	})
}

func Run(ctx context.Context, brokers []string, dsn, redisAddr string, batchSize int) error {
	if batchSize < 1 || batchSize > maxBatchSize {
		return fmt.Errorf("persist-batch 必须为 1～%d", maxBatchSize)
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("persist 模式必须指定 -kafka-brokers")
		}
	}
	db, err := Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, ContextTimeoutEnabled: true,
		DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer rdb.Close()
	reader := NewEventReader(brokers, persistenceGroup)
	defer reader.Close()
	// 先建 topic 再消费：死信 topic 不存在时，写入会在遇到第一条坏消息时才失败。
	if err := EnsureDeadLetterTopic(ctx, brokers); err != nil {
		return fmt.Errorf("创建死信 topic 失败: %w", err)
	}
	deadLetters := NewDeadLetterWriter(brokers)
	defer deadLetters.Close()
	log.Printf("启动弹幕持久化消费者，消费组: %s 批量: %d 死信 topic: %s", persistenceGroup, batchSize, deadLetterTopic)
	err = ConsumeBatches(ctx, reader.FetchMessage, func(ctx context.Context, events []danmaku.RoomEvent) error {
		return saveRoomEventsWithInvalidation(ctx, db, events, func(ctx context.Context, rooms []string) error {
			return history.InvalidateRooms(ctx, rdb, rooms)
		})
	}, reader.CommitMessages, func(ctx context.Context, message kafka.Message, reason error) error {
		return DeadLetter(ctx, deadLetters, message, reason)
	}, batchSize)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// 串行消费：当前消息写入成功并提交后，才处理下一条，防止越过失败消息提交位点。
// 无法解析或字段非法的事件重试也不会成功，交给 deadLetter 转存后提交位点，不阻塞后面的弹幕。
// deadLetter 为 nil 时保留旧行为：报错退出并保留位点。
func ConsumeEvents(ctx context.Context,
	fetch func(context.Context) (kafka.Message, error),
	save func(context.Context, danmaku.RoomEvent) error,
	commit func(context.Context, ...kafka.Message) error,
	deadLetter func(context.Context, kafka.Message, error) error,
) error {
	return ConsumeBatches(ctx, fetch, func(ctx context.Context, events []danmaku.RoomEvent) error {
		return save(ctx, events[0])
	}, commit, deadLetter, 1)
}

type invalidMessage struct {
	message kafka.Message
	reason  error
}

// ConsumeBatches 按批读取：一个事务写入一批事件，然后一次提交这一批的位点。
// 达到条数上限或首条消息后 50ms 时写库；只有合法事件落库、坏消息转存成功后才提交。
// 批次内允许跨分区；CommitMessages 会按分区提交各自的最高位点。
// fillWindow 用于在低流量时尽快处理已到达的部分批次，避免等满一整批。
func ConsumeBatches(ctx context.Context,
	fetch func(context.Context) (kafka.Message, error),
	save func(context.Context, []danmaku.RoomEvent) error,
	commit func(context.Context, ...kafka.Message) error,
	deadLetter func(context.Context, kafka.Message, error) error,
	batchSize int,
) error {
	if batchSize < 1 || batchSize > maxBatchSize {
		return fmt.Errorf("批量大小必须为 1～%d", maxBatchSize)
	}
	const fillWindow = 50 * time.Millisecond
	for {
		first, err := fetch(ctx)
		if err != nil {
			return err
		}
		messages := []kafka.Message{first}
		batchBytes := len(first.Value) + len(first.Key)
		events := make([]danmaku.RoomEvent, 0, batchSize)
		invalid := make([]invalidMessage, 0)
		add := func(message kafka.Message) {
			event, reason := decodeEvent(message)
			if reason != nil {
				invalid = append(invalid, invalidMessage{message: message, reason: reason})
				return
			}
			events = append(events, event)
		}
		add(first)

		var fetchErr error
		deadline := time.Now().Add(fillWindow)
		for len(messages) < batchSize && batchBytes < maxBatchBytes {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			fillCtx, cancel := context.WithTimeout(ctx, remaining)
			next, err := fetch(fillCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// 只有本批定时器到期才是正常的部分批次；其他读取错误在处理已读消息后返回。
				if !errors.Is(err, context.DeadlineExceeded) || time.Now().Before(deadline) {
					fetchErr = err
				}
				break
			}
			messages = append(messages, next)
			batchBytes += len(next.Value) + len(next.Key)
			add(next)
		}

		if len(events) > 0 {
			if err := retryUntilDone(ctx, fmt.Sprintf("批量保存 %d 条弹幕", len(events)), func(ctx context.Context) error {
				return save(ctx, events)
			}); err != nil {
				return err
			}
		}
		for _, bad := range invalid {
			if deadLetter == nil {
				return fmt.Errorf("%w，保留位点 partition=%d offset=%d", bad.reason, bad.message.Partition, bad.message.Offset)
			}
			log.Printf("转存非法 Kafka 事件到死信 topic partition=%d offset=%d: %v", bad.message.Partition, bad.message.Offset, bad.reason)
			if err := retryUntilDone(ctx, "转存死信", func(ctx context.Context) error {
				return deadLetter(ctx, bad.message, bad.reason)
			}); err != nil {
				return err
			}
		}

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = commit(commitCtx, messages...)
		cancel()
		if err != nil {
			// 写库成功但提交失败：退出后重启可重复消费，主键保证不插入第二行。
			return fmt.Errorf("提交 Kafka 位点失败: %w", err)
		}
		if fetchErr != nil {
			return fetchErr
		}
	}
}

func decodeEvent(message kafka.Message) (danmaku.RoomEvent, error) {
	var event danmaku.RoomEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return event, fmt.Errorf("Kafka 事件解码失败: %w", err)
	}
	if event.ID == "" || len(event.ID) > 128 || event.RoomID == "" || len(event.RoomID) > 128 ||
		len(event.Data) > danmaku.MaxMessageSize || (event.Type != websocket.TextMessage && event.Type != websocket.BinaryMessage) {
		return event, errors.New("非法 Kafka 事件")
	}
	return event, nil
}

// 下游暂时不可用时每秒重试同一个操作，直到成功或 ctx 取消；期间不读取下一条也不提交位点。
func retryUntilDone(ctx context.Context, what string, operation func(context.Context) error) error {
	for {
		opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := operation(opCtx)
		cancel()
		if err == nil {
			return nil
		}
		log.Printf("%s失败，暂不提交，1 秒后重试: %v", what, err)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func RunBackfill(ctx context.Context, dsn string) error {
	db, err := Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	scanned, eligible, err := history.Backfill(ctx, db)
	if err != nil {
		return err
	}
	log.Printf("历史弹幕回填完成：扫描 %d 条，识别 %d 条正式弹幕（含已有记录）", scanned, eligible)
	return nil
}
