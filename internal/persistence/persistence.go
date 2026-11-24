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
	"sync"
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

// 生产者按分区并发合批：高并发下多个 WebSocket 读协程共享一次 Produce RPC，
// 低流量时最多等待 5ms 即发出，同时保持同步确认（Async=false, RequireAll）。
// 500 条上限避免高并发或磁盘抖动时被迫拆成多次串行 Produce RPC；Snappy 压缩降低磁盘写放大。
const (
	producerBatchSize     = 500
	producerBatchTimeout  = 10 * time.Millisecond
	defaultPersistWorkers = 3
	maxPersistWorkers     = 16
)

//go:embed schema.sql
var persistenceSchema string

func NewEventWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        persistenceTopic,
		Balancer:     &kafka.Hash{}, // 同一房间使用同一个分区键。
		RequiredAcks: kafka.RequireAll,
		Async:        false, // 等待 Kafka 确认接收，不等待 MySQL 写入。
		BatchSize:    producerBatchSize,
		BatchTimeout: producerBatchTimeout,
		Compression:  kafka.Snappy,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxAttempts:  3,
	}
}

func NewDeadLetterWriter(brokers []string) *kafka.Writer {
	writer := NewEventWriter(brokers)
	writer.Topic = deadLetterTopic
	writer.Balancer = nil
	// 死信由消费端串行逐条写入，不等待合批窗口，避免每条死信平白多等 5ms。
	writer.BatchSize = 1
	writer.BatchTimeout = 0
	writer.Compression = 0
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
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
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
	uniqueEvents := make([]danmaku.RoomEvent, 0, len(events))
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
		event.Data = data
		args = append(args, event.ID, event.RoomID, event.Type, data)
		ids = append(ids, event.ID)
		uniqueEvents = append(uniqueEvents, event)
	}
	query.WriteString(" ON DUPLICATE KEY UPDATE id = id")
	res, err := tx.ExecContext(ctx, query.String(), args...)
	if err != nil {
		return err
	}

	var stored []danmaku.RoomEvent
	// go-sql-driver/mysql 默认不启用 ClientFoundRows：ON DUPLICATE KEY UPDATE id = id
	// 对每条新插入行计 1，对已存在且未变更的冲突行计 0。仅当发生冲突/重放时才回表锁定读。
	if affected, affErr := res.RowsAffected(); affErr == nil && affected == int64(len(uniqueEvents)) {
		stored = uniqueEvents
	} else {
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
		// 锁定读使用当前已保存的行，不依赖事务快照或客户端提交的重复载荷。
		rows, err := tx.QueryContext(ctx, "SELECT id, room_id, message_type, data FROM danmaku WHERE id IN ("+placeholders+") FOR UPDATE", ids...)
		if err != nil {
			return err
		}
		stored = make([]danmaku.RoomEvent, 0, len(ids))
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
	return RunWithWorkers(ctx, brokers, dsn, redisAddr, batchSize, defaultPersistWorkers)
}

// RunWithWorkers 在同一消费组内启动 workers 个并行消费者。
// Kafka 消费组协议会将各分区自动分配给不同 worker，同一房间经 Hash 分区仍由单个 worker 顺序处理。
func RunWithWorkers(ctx context.Context, brokers []string, dsn, redisAddr string, batchSize, workers int) error {
	if batchSize < 1 || batchSize > maxBatchSize {
		return fmt.Errorf("persist-batch 必须为 1～%d", maxBatchSize)
	}
	if workers < 1 || workers > maxPersistWorkers {
		return fmt.Errorf("persist-workers 必须为 1～%d", maxPersistWorkers)
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
	if maxOpen := workers * 2; maxOpen > 16 {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(workers)
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, ContextTimeoutEnabled: true,
		DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer rdb.Close()
	// 先建 topic 再消费：死信 topic 不存在时，写入会在遇到第一条坏消息时才失败。
	if err := EnsureDeadLetterTopic(ctx, brokers); err != nil {
		return fmt.Errorf("创建死信 topic 失败: %w", err)
	}
	deadLetters := NewDeadLetterWriter(brokers)
	defer deadLetters.Close()
	log.Printf("启动弹幕持久化消费者，消费组: %s 并发数: %d 批量: %d 死信 topic: %s", persistenceGroup, workers, batchSize, deadLetterTopic)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader := NewEventReader(brokers, persistenceGroup)
			defer reader.Close()
			workerErr := ConsumeBatches(workerCtx, reader.FetchMessage, func(ctx context.Context, events []danmaku.RoomEvent) error {
				return saveRoomEventsWithInvalidation(ctx, db, events, func(ctx context.Context, rooms []string) error {
					return history.InvalidateRooms(ctx, rdb, rooms)
				})
			}, reader.CommitMessages, func(ctx context.Context, message kafka.Message, reason error) error {
				return DeadLetter(ctx, deadLetters, message, reason)
			}, batchSize)
			if workerErr != nil && workerCtx.Err() == nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = workerErr
				}
				errMu.Unlock()
				cancelWorkers()
			}
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return firstErr
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
		eventMessages := make([]kafka.Message, 0, batchSize)
		invalid := make([]invalidMessage, 0)
		add := func(message kafka.Message) {
			event, reason := decodeEvent(message)
			if reason != nil {
				invalid = append(invalid, invalidMessage{message: message, reason: reason})
				return
			}
			events = append(events, event)
			eventMessages = append(eventMessages, message)
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
			failedItems, err := saveBatchWithFallback(ctx, events, eventMessages, save)
			if err != nil {
				return err
			}
			invalid = append(invalid, failedItems...)
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

// isNonRetryableDataError 判断写库错误是否由单条数据本身违反数据库约束导致（重试也不会成功）。
// 连接断开、超时、死锁（1213）、锁等待超时（1205）等基础设施/并发错误一律视为可重试。
func isNonRetryableDataError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1048, // ER_BAD_NULL_ERROR: Column cannot be null
		1264, // ER_WARN_DATA_OUT_OF_RANGE: Out of range value
		1366, // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD: Incorrect string/charset value
		1406, // ER_DATA_TOO_LONG: Data too long for column
		1452, // ER_NO_REFERENCED_ROW_2: Foreign key constraint fails
		3819: // ER_CHECK_CONSTRAINT_VIOLATED: Check constraint violated
		return true
	default:
		return false
	}
}

// saveBatchWithFallback 优先批量落库；遇到可重试错误（如 MySQL 停服/抖动）按原策略持续重试。
// 若遇到不可重试的数据库数据约束错误，降级为逐条写库，将坏消息剥离返回给上层转存死信，
// 保证同批正常消息顺利入库而不阻塞分区消费。
func saveBatchWithFallback(
	ctx context.Context,
	events []danmaku.RoomEvent,
	eventMessages []kafka.Message,
	save func(context.Context, []danmaku.RoomEvent) error,
) ([]invalidMessage, error) {
	batchErr, err := retryUnlessNonRetryable(ctx, fmt.Sprintf("批量保存 %d 条弹幕", len(events)), func(ctx context.Context) error {
		return save(ctx, events)
	})
	if err != nil {
		return nil, err
	}
	if batchErr == nil {
		return nil, nil
	}
	if len(events) == 1 {
		return []invalidMessage{{message: eventMessages[0], reason: batchErr}}, nil
	}

	log.Printf("批量保存 %d 条弹幕遇到不可重试数据错误，降级为逐条写入以隔离坏消息: %v", len(events), batchErr)
	var failed []invalidMessage
	for i, event := range events {
		itemErr, err := retryUnlessNonRetryable(ctx, fmt.Sprintf("降级单条保存弹幕 %s", event.ID), func(ctx context.Context) error {
			return save(ctx, []danmaku.RoomEvent{event})
		})
		if err != nil {
			return nil, err
		}
		if itemErr != nil {
			failed = append(failed, invalidMessage{message: eventMessages[i], reason: itemErr})
		}
	}
	return failed, nil
}

// retryUnlessNonRetryable 在下游暂时不可用时每秒重试；若遇到不可重试数据错误则立即返回该错误供上层隔离。
func retryUnlessNonRetryable(ctx context.Context, what string, operation func(context.Context) error) (nonRetryableErr error, fatalErr error) {
	for {
		opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := operation(opCtx)
		cancel()
		if err == nil {
			return nil, nil
		}
		if isNonRetryableDataError(err) {
			return err, nil
		}
		log.Printf("%s失败，暂不提交，1 秒后重试: %v", what, err)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
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
