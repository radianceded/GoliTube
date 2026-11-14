package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/segmentio/kafka-go"
)

func TestDurablePublish(t *testing.T) {
	for _, failedStage := range []string{"", "queue", "redis"} {
		t.Run(failedStage, func(t *testing.T) {
			event := danmaku.RoomEvent{ID: "stable-id", RoomID: "room", Type: websocket.TextMessage, Data: []byte("hello")}
			var calls []string
			failure := errors.New("unavailable")
			stage := func(name string) func(context.Context, danmaku.RoomEvent) error {
				return func(_ context.Context, got danmaku.RoomEvent) error {
					if !reflect.DeepEqual(got, event) {
						t.Fatalf("event changed between destinations: %+v", got)
					}
					calls = append(calls, name)
					if failedStage == name {
						return failure
					}
					return nil
				}
			}
			err := DurablePublish(stage("queue"), stage("redis"))(t.Context(), event)
			want := []string{"queue", "redis"}
			if failedStage == "queue" {
				want = []string{"queue"} // Kafka 失败时，不广播一个未确认入队的事件。
			}
			if !reflect.DeepEqual(calls, want) || (failedStage == "" && err != nil) || (failedStage != "" && !errors.Is(err, failure)) {
				t.Fatalf("calls=%v err=%v", calls, err)
			}
		})
	}
}

func TestPersistenceRetryBeforeCommit(t *testing.T) {
	event := danmaku.RoomEvent{ID: "same-across-retries", RoomID: "room", Type: websocket.BinaryMessage, Data: []byte{0, 255}}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	fetched, attempts := false, 0
	err = ConsumeEvents(t.Context(), func(context.Context) (kafka.Message, error) {
		calls = append(calls, "fetch")
		if fetched {
			return kafka.Message{}, io.EOF
		}
		fetched = true
		return kafka.Message{Partition: 2, Offset: 7, Value: payload}, nil
	}, func(_ context.Context, got danmaku.RoomEvent) error {
		calls = append(calls, "save")
		if !reflect.DeepEqual(got, event) {
			t.Fatalf("retry changed event: %+v", got)
		}
		attempts++
		if attempts == 1 {
			return errors.New("database temporarily unavailable")
		}
		return nil
	}, func(_ context.Context, messages ...kafka.Message) error {
		calls = append(calls, "commit")
		if attempts != 2 || len(messages) != 1 || messages[0].Offset != 7 || messages[0].Partition != 2 {
			t.Fatal("committed before save succeeded, or committed another message")
		}
		return nil
	}, nil)
	if !errors.Is(err, io.EOF) || !reflect.DeepEqual(calls, []string{"fetch", "save", "save", "commit", "fetch"}) {
		t.Fatalf("calls=%v err=%v", calls, err)
	}
}

func TestPersistenceStopsWithoutSkipping(t *testing.T) {
	valid := `{"id":"id","room_id":"room","type":1,"data":"aGk="}`
	for _, test := range []struct {
		name, payload string
		cancelOnSave  bool
		commitFails   bool
	}{
		{name: "bad-json", payload: "{"},
		{name: "missing-id", payload: `{"room_id":"room","type":1}`},
		{name: "invalid-type", payload: `{"id":"id","room_id":"room","type":9}`},
		{name: "cancel-during-outage", payload: valid, cancelOnSave: true},
		{name: "commit-failed-after-save", payload: valid, commitFails: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fetches, saves, commits := 0, 0, 0
			err := ConsumeEvents(ctx, func(context.Context) (kafka.Message, error) {
				fetches++
				if fetches > 1 {
					t.Fatal("fetched past a failed message")
				}
				return kafka.Message{Value: []byte(test.payload)}, nil
			}, func(context.Context, danmaku.RoomEvent) error {
				saves++
				if test.cancelOnSave {
					cancel()
					return errors.New("database offline")
				}
				return nil
			}, func(context.Context, ...kafka.Message) error {
				commits++
				return errors.New("commit unavailable")
			}, nil)
			if err == nil || fetches != 1 {
				t.Fatalf("fetches=%d err=%v", fetches, err)
			}
			if test.commitFails {
				if saves != 1 || commits != 1 || !strings.Contains(err.Error(), "提交") {
					t.Fatalf("saves=%d commits=%d err=%v", saves, commits, err)
				}
			} else if commits != 0 || (!test.cancelOnSave && saves != 0) {
				t.Fatalf("invalid or unsaved message was committed: saves=%d commits=%d", saves, commits)
			}
		})
	}
}

// 非法事件转存死信后提交位点，后面的合法弹幕继续保存。
func TestPersistenceDeadLettersInvalidEvents(t *testing.T) {
	valid := kafka.Message{Partition: 1, Offset: 4, Key: []byte("room"), Value: []byte(`{"id":"id","room_id":"room","type":1,"data":"aGk="}`)}
	for _, test := range []struct {
		name  string
		value string
	}{
		{"bad-json", "{"},
		{"missing-id", `{"room_id":"room","type":1}`},
		{"invalid-type", `{"id":"id","room_id":"room","type":9}`},
		{"oversized-data", `{"id":"id","room_id":"room","type":1,"data":"` + strings.Repeat("QUFB", danmaku.MaxMessageSize) + `"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			poison := kafka.Message{Partition: 1, Offset: 3, Key: []byte("room"), Value: []byte(test.value)}
			queue := []kafka.Message{poison, valid}
			var calls []string
			var deadLettered []kafka.Message
			err := ConsumeEvents(t.Context(), func(context.Context) (kafka.Message, error) {
				if len(queue) == 0 {
					return kafka.Message{}, io.EOF
				}
				next := queue[0]
				queue = queue[1:]
				return next, nil
			}, func(_ context.Context, event danmaku.RoomEvent) error {
				calls = append(calls, "save:"+event.ID)
				return nil
			}, func(_ context.Context, messages ...kafka.Message) error {
				calls = append(calls, "commit:"+strconv.FormatInt(messages[0].Offset, 10))
				return nil
			}, func(_ context.Context, message kafka.Message, reason error) error {
				if reason == nil {
					t.Error("dead letter without a reason")
				}
				calls = append(calls, "dlq")
				deadLettered = append(deadLettered, message)
				return nil
			})
			want := []string{"dlq", "commit:3", "save:id", "commit:4"}
			if !errors.Is(err, io.EOF) || !reflect.DeepEqual(calls, want) {
				t.Fatalf("calls=%v err=%v", calls, err)
			}
			if len(deadLettered) != 1 || !reflect.DeepEqual(deadLettered[0], poison) {
				t.Fatalf("dead-lettered %+v", deadLettered)
			}
		})
	}
}

// 死信转存失败时不能提交位点，否则坏消息和原因都会丢失；恢复后重试同一条。
func TestPersistenceDeadLetterRetryBeforeCommit(t *testing.T) {
	poison := kafka.Message{Partition: 0, Offset: 9, Value: []byte("{")}
	fetched, attempts := false, 0
	var calls []string
	err := ConsumeEvents(t.Context(), func(context.Context) (kafka.Message, error) {
		if fetched {
			return kafka.Message{}, io.EOF
		}
		fetched = true
		return poison, nil
	}, func(context.Context, danmaku.RoomEvent) error {
		t.Fatal("invalid event reached the database")
		return nil
	}, func(_ context.Context, messages ...kafka.Message) error {
		calls = append(calls, "commit")
		if attempts != 2 {
			t.Fatalf("committed after %d dead-letter attempts", attempts)
		}
		return nil
	}, func(context.Context, kafka.Message, error) error {
		attempts++
		calls = append(calls, "dlq")
		if attempts == 1 {
			return errors.New("kafka unavailable")
		}
		return nil
	})
	if !errors.Is(err, io.EOF) || !reflect.DeepEqual(calls, []string{"dlq", "dlq", "commit"}) {
		t.Fatalf("calls=%v err=%v", calls, err)
	}
}
