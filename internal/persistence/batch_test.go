package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/segmentio/kafka-go"
)

func TestBatchCommitWaitsForPersistenceAndDeadLetters(t *testing.T) {
	for _, stage := range []string{"save-retry", "save-cancel", "dlq-retry", "dlq-cancel", "commit-fail"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			payload, _ := json.Marshal(danmaku.RoomEvent{ID: "valid", RoomID: "room", Type: 1})
			// 同批跨两个分区，第三条是第二个分区后续的坏消息。
			queue := []kafka.Message{{Partition: 0, Offset: 10, Value: payload}, {Partition: 1, Offset: 20, Value: payload}, {Partition: 1, Offset: 21, Value: []byte("{")}}
			expected := append([]kafka.Message(nil), queue...)
			saves, deadLetters, commits := 0, 0, 0
			failure := errors.New("temporary failure")
			err := ConsumeBatches(ctx, func(context.Context) (kafka.Message, error) {
				if len(queue) == 0 {
					return kafka.Message{}, io.EOF
				}
				message := queue[0]
				queue = queue[1:]
				return message, nil
			}, func(_ context.Context, events []danmaku.RoomEvent) error {
				saves++
				if len(events) != 2 {
					t.Fatalf("events=%v", events)
				}
				if stage == "save-cancel" {
					cancel()
					return failure
				}
				if stage == "save-retry" && saves == 1 {
					return failure
				}
				return nil
			}, func(_ context.Context, messages ...kafka.Message) error {
				commits++
				if saves == 0 || deadLetters == 0 || !reflect.DeepEqual(messages, expected) {
					t.Fatal("committed before all database and dead-letter operations completed")
				}
				if stage == "commit-fail" {
					return failure
				}
				return nil
			}, func(context.Context, kafka.Message, error) error {
				deadLetters++
				if stage == "dlq-cancel" {
					cancel()
					return failure
				}
				if stage == "dlq-retry" && deadLetters == 1 {
					return failure
				}
				return nil
			}, 3)
			switch stage {
			case "save-cancel", "dlq-cancel":
				if !errors.Is(err, context.Canceled) || commits != 0 {
					t.Fatalf("err=%v commits=%d", err, commits)
				}
			case "commit-fail":
				if !errors.Is(err, failure) || commits != 1 {
					t.Fatalf("err=%v commits=%d", err, commits)
				}
			default:
				if !errors.Is(err, io.EOF) || commits != 1 {
					t.Fatalf("err=%v commits=%d", err, commits)
				}
				if stage == "save-retry" && saves != 2 {
					t.Fatalf("saves=%d", saves)
				}
				if stage == "dlq-retry" && deadLetters != 2 {
					t.Fatalf("deadLetters=%d", deadLetters)
				}
			}
		})
	}
}

func TestBatchFlushesPartialBatchOnDeadlineOrReadFailure(t *testing.T) {
	for _, timed := range []bool{false, true} {
		t.Run(map[bool]string{false: "read-failure", true: "deadline"}[timed], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			payload, _ := json.Marshal(danmaku.RoomEvent{ID: "one", RoomID: "room", Type: 1})
			calls, saves, commits := 0, 0, 0
			err := ConsumeBatches(ctx, func(fetchCtx context.Context) (kafka.Message, error) {
				calls++
				if calls == 1 {
					return kafka.Message{Value: payload}, nil
				}
				if timed && calls == 2 {
					<-fetchCtx.Done()
					return kafka.Message{}, fetchCtx.Err()
				}
				return kafka.Message{}, io.EOF
			}, func(context.Context, []danmaku.RoomEvent) error { saves++; return nil },
				func(context.Context, ...kafka.Message) error { commits++; return nil }, nil, 100)
			if !errors.Is(err, io.EOF) || saves != 1 || commits != 1 {
				t.Fatalf("err=%v saves=%d commits=%d", err, saves, commits)
			}
			if timed && calls != 3 || !timed && calls != 2 {
				t.Fatalf("fetch calls=%d", calls)
			}
		})
	}
}

func TestBatchCancellationAndInvalidSizeDoNotCommit(t *testing.T) {
	for _, size := range []int{0, maxBatchSize + 1, 100} {
		ctx, cancel := context.WithCancel(t.Context())
		payload, _ := json.Marshal(danmaku.RoomEvent{ID: "one", RoomID: "room", Type: 1})
		calls := 0
		err := ConsumeBatches(ctx, func(context.Context) (kafka.Message, error) {
			calls++
			if calls == 1 {
				return kafka.Message{Value: payload}, nil
			}
			cancel()
			return kafka.Message{}, context.Canceled
		}, func(context.Context, []danmaku.RoomEvent) error { t.Fatal("saved canceled batch"); return nil },
			func(context.Context, ...kafka.Message) error { t.Fatal("committed canceled batch"); return nil }, nil, size)
		cancel()
		if err == nil {
			t.Fatal("expected error")
		}
		if size != 100 && calls != 0 {
			t.Fatal("read with invalid batch size")
		}
	}
}

func TestBatchPayloadLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), maxBatchBytes/2+1)
	remaining, dead := 3, 0
	var sizes []int
	err := ConsumeBatches(t.Context(), func(context.Context) (kafka.Message, error) {
		if remaining == 0 {
			return kafka.Message{}, io.EOF
		}
		remaining--
		return kafka.Message{Value: payload}, nil
	}, func(context.Context, []danmaku.RoomEvent) error {
		t.Fatal("invalid payload reached database")
		return nil
	},
		func(_ context.Context, messages ...kafka.Message) error {
			sizes = append(sizes, len(messages))
			return nil
		},
		func(context.Context, kafka.Message, error) error { dead++; return nil }, 100)
	if !errors.Is(err, io.EOF) || dead != 3 || !reflect.DeepEqual(sizes, []int{2, 1}) {
		t.Fatalf("err=%v dead=%d sizes=%v", err, dead, sizes)
	}
}

func TestBatchWriteDataErrorFallsBackToSingleItemAndDeadLetters(t *testing.T) {
	mustMarshal := func(e danmaku.RoomEvent) []byte {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	queue := []kafka.Message{
		{Partition: 0, Offset: 1, Value: mustMarshal(danmaku.RoomEvent{ID: "good-1", RoomID: "room", Type: 1})},
		{Partition: 0, Offset: 2, Value: mustMarshal(danmaku.RoomEvent{ID: "bad-db", RoomID: "room", Type: 1})},
		{Partition: 0, Offset: 3, Value: mustMarshal(danmaku.RoomEvent{ID: "good-2", RoomID: "room", Type: 1})},
	}
	expectedCommit := append([]kafka.Message(nil), queue...)
	dataTooLong := &mysql.MySQLError{Number: 1406, Message: "Data too long for column"}

	var savedIDs []string
	var dlqOffsets []int64
	var dlqReasons []error
	commits := 0

	err := ConsumeBatches(t.Context(), func(context.Context) (kafka.Message, error) {
		if len(queue) == 0 {
			return kafka.Message{}, io.EOF
		}
		m := queue[0]
		queue = queue[1:]
		return m, nil
	}, func(_ context.Context, events []danmaku.RoomEvent) error {
		for _, e := range events {
			if e.ID == "bad-db" {
				return dataTooLong
			}
		}
		for _, e := range events {
			savedIDs = append(savedIDs, e.ID)
		}
		return nil
	}, func(_ context.Context, messages ...kafka.Message) error {
		commits++
		if !reflect.DeepEqual(messages, expectedCommit) {
			t.Fatalf("unexpected committed messages: %+v", messages)
		}
		return nil
	}, func(_ context.Context, message kafka.Message, reason error) error {
		dlqOffsets = append(dlqOffsets, message.Offset)
		dlqReasons = append(dlqReasons, reason)
		return nil
	}, 3)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("err=%v", err)
	}
	if !reflect.DeepEqual(savedIDs, []string{"good-1", "good-2"}) {
		t.Fatalf("savedIDs=%v", savedIDs)
	}
	if !reflect.DeepEqual(dlqOffsets, []int64{2}) {
		t.Fatalf("dlqOffsets=%v", dlqOffsets)
	}
	if len(dlqReasons) != 1 || !errors.Is(dlqReasons[0], dataTooLong) {
		t.Fatalf("dlqReasons=%v", dlqReasons)
	}
	if commits != 1 {
		t.Fatalf("commits=%d", commits)
	}
}

func TestBatchWriteFallbackRetriesTransientErrorDuringSingleItemSave(t *testing.T) {
	mustMarshal := func(e danmaku.RoomEvent) []byte {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	queue := []kafka.Message{
		{Partition: 1, Offset: 10, Value: mustMarshal(danmaku.RoomEvent{ID: "bad-db", RoomID: "room", Type: 1})},
		{Partition: 1, Offset: 11, Value: mustMarshal(danmaku.RoomEvent{ID: "flaky-good", RoomID: "room", Type: 1})},
	}
	invalidCharset := &mysql.MySQLError{Number: 1366, Message: "Incorrect string value"}
	deadlock := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}

	flakyAttempts := 0
	var savedIDs []string
	var dlqOffsets []int64
	commits := 0

	err := ConsumeBatches(t.Context(), func(context.Context) (kafka.Message, error) {
		if len(queue) == 0 {
			return kafka.Message{}, io.EOF
		}
		m := queue[0]
		queue = queue[1:]
		return m, nil
	}, func(_ context.Context, events []danmaku.RoomEvent) error {
		for _, e := range events {
			if e.ID == "bad-db" {
				return invalidCharset
			}
		}
		if len(events) == 1 && events[0].ID == "flaky-good" {
			flakyAttempts++
			if flakyAttempts == 1 {
				return deadlock // 可重试错误，绝不能误判进死信
			}
		}
		for _, e := range events {
			savedIDs = append(savedIDs, e.ID)
		}
		return nil
	}, func(_ context.Context, _ ...kafka.Message) error {
		commits++
		return nil
	}, func(_ context.Context, message kafka.Message, _ error) error {
		dlqOffsets = append(dlqOffsets, message.Offset)
		return nil
	}, 2)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("err=%v", err)
	}
	if flakyAttempts != 2 {
		t.Fatalf("flakyAttempts=%d, want 2", flakyAttempts)
	}
	if !reflect.DeepEqual(savedIDs, []string{"flaky-good"}) {
		t.Fatalf("savedIDs=%v", savedIDs)
	}
	if !reflect.DeepEqual(dlqOffsets, []int64{10}) {
		t.Fatalf("dlqOffsets=%v", dlqOffsets)
	}
	if commits != 1 {
		t.Fatalf("commits=%d", commits)
	}
}

func TestBatchWriteSingleItemDataErrorDeadLettersDirectly(t *testing.T) {
	payload, _ := json.Marshal(danmaku.RoomEvent{ID: "bad-only", RoomID: "room", Type: 1})
	sent := false
	saveCalls := 0
	dlqCalls := 0
	commits := 0
	checkViolation := &mysql.MySQLError{Number: 3819, Message: "Check constraint violated"}

	err := ConsumeBatches(t.Context(), func(context.Context) (kafka.Message, error) {
		if sent {
			return kafka.Message{}, io.EOF
		}
		sent = true
		return kafka.Message{Partition: 2, Offset: 99, Value: payload}, nil
	}, func(_ context.Context, _ []danmaku.RoomEvent) error {
		saveCalls++
		return checkViolation
	}, func(_ context.Context, _ ...kafka.Message) error {
		commits++
		return nil
	}, func(_ context.Context, message kafka.Message, reason error) error {
		dlqCalls++
		if message.Offset != 99 || !errors.Is(reason, checkViolation) {
			t.Fatalf("unexpected dlq msg=%+v reason=%v", message, reason)
		}
		return nil
	}, 1)

	if !errors.Is(err, io.EOF) || saveCalls != 1 || dlqCalls != 1 || commits != 1 {
		t.Fatalf("err=%v saveCalls=%d dlqCalls=%d commits=%d", err, saveCalls, dlqCalls, commits)
	}
}
