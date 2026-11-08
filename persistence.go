package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const persistenceTopic = "golitube-danmaku"
const persistenceGroup = "golitube-persistence-v1"

//go:embed schema.sql
var persistenceSchema string

func newEventWriter(brokers []string) *kafka.Writer {
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

func enqueueRoomEvent(ctx context.Context, writer *kafka.Writer, event RoomEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{Key: []byte(event.RoomID), Value: payload})
}

func durablePublish(enqueue, publish func(context.Context, RoomEvent) error) func(context.Context, RoomEvent) error {
	return func(ctx context.Context, event RoomEvent) error {
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

func openPersistenceDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := newMySQLDB(dsn)
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
func newMySQLDB(dsn string) (*sql.DB, error) {
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

func saveRoomEvent(ctx context.Context, db *sql.DB, event RoomEvent) error {
	return saveRoomEventWithInvalidation(ctx, db, event, nil)
}

func saveRoomEventWithInvalidation(ctx context.Context, db *sql.DB, event RoomEvent, invalidate func(context.Context, string) error) error {
	if event.Data == nil {
		event.Data = []byte{} // 空 WebSocket 消息保存为空 BLOB，而不是 SQL NULL。
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO danmaku (id, room_id, message_type, data)
		VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE id = id`,
		event.ID, event.RoomID, event.Type, event.Data)
	if err != nil {
		return err
	}
	// 重复 ID 以原归档为准，避免重试载荷不同导致两张表内容不一致。
	stored := RoomEvent{ID: event.ID}
	if err := tx.QueryRowContext(ctx, "SELECT room_id, message_type, data FROM danmaku WHERE id = ?", event.ID).
		Scan(&stored.RoomID, &stored.Type, &stored.Data); err != nil {
		return err
	}
	if _, err := projectHistory(ctx, tx, stored); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if invalidate != nil {
		// 使用原归档的房间，重复 ID 的不同载荷不能使另一个房间失效。
		if err := invalidate(ctx, stored.RoomID); err != nil {
			// MySQL 已提交。缓存仍有短 TTL，Redis 故障不阻塞后续持久化。
			log.Println("历史缓存失效失败，等待 TTL 回收:", err)
		}
	}
	return nil
}

func newEventReader(brokers []string, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: persistenceTopic, GroupID: group,
		MinBytes: 1, MaxBytes: 10 << 20, MaxWait: time.Second,
		StartOffset: kafka.FirstOffset,
		// CommitInterval 默认 0：CommitMessages 等待提交结果。
	})
}

func runPersistence(ctx context.Context, brokers []string, dsn, redisAddr string) error {
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("persist 模式必须指定 -kafka-brokers")
		}
	}
	db, err := openPersistenceDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, ContextTimeoutEnabled: true,
		DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer rdb.Close()
	reader := newEventReader(brokers, persistenceGroup)
	defer reader.Close()
	log.Println("启动弹幕持久化消费者，消费组:", persistenceGroup)
	err = consumePersistentEvents(ctx, reader.FetchMessage, func(ctx context.Context, event RoomEvent) error {
		return saveRoomEventWithInvalidation(ctx, db, event, func(ctx context.Context, room string) error {
			return invalidateHistory(ctx, rdb, room)
		})
	}, reader.CommitMessages)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// 串行消费：当前消息写入成功并提交后，才处理下一条，防止越过失败消息提交位点。
func consumePersistentEvents(ctx context.Context,
	fetch func(context.Context) (kafka.Message, error),
	save func(context.Context, RoomEvent) error,
	commit func(context.Context, ...kafka.Message) error,
) error {
	for {
		message, err := fetch(ctx)
		if err != nil {
			return err
		}
		var event RoomEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			return fmt.Errorf("Kafka 事件解码失败，保留位点 partition=%d offset=%d: %w", message.Partition, message.Offset, err)
		}
		if event.ID == "" || len(event.ID) > 128 || event.RoomID == "" || len(event.RoomID) > 128 ||
			len(event.Data) > maxMessageSize || (event.Type != websocket.TextMessage && event.Type != websocket.BinaryMessage) {
			return fmt.Errorf("非法 Kafka 事件，保留位点 partition=%d offset=%d", message.Partition, message.Offset)
		}
		for {
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = save(writeCtx, event)
			cancel()
			if err == nil {
				break
			}
			log.Printf("弹幕 %s 保存失败，暂不提交，1 秒后重试: %v", event.ID, err)
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = commit(commitCtx, message)
		cancel()
		if err != nil {
			// 写库成功但提交失败：退出后重启可重复消费，主键保证不插入第二行。
			return fmt.Errorf("提交 Kafka 位点失败: %w", err)
		}
	}
}
