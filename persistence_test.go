package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

func TestDurablePublish(t *testing.T) {
	for _, failedStage := range []string{"", "queue", "redis"} {
		t.Run(failedStage, func(t *testing.T) {
			event := RoomEvent{ID: "stable-id", RoomID: "room", Type: websocket.TextMessage, Data: []byte("hello")}
			var calls []string
			failure := errors.New("unavailable")
			stage := func(name string) func(context.Context, RoomEvent) error {
				return func(_ context.Context, got RoomEvent) error {
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
			err := durablePublish(stage("queue"), stage("redis"))(t.Context(), event)
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
	event := RoomEvent{ID: "same-across-retries", RoomID: "room", Type: websocket.BinaryMessage, Data: []byte{0, 255}}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	fetched, attempts := false, 0
	err = consumePersistentEvents(t.Context(), func(context.Context) (kafka.Message, error) {
		calls = append(calls, "fetch")
		if fetched {
			return kafka.Message{}, io.EOF
		}
		fetched = true
		return kafka.Message{Partition: 2, Offset: 7, Value: payload}, nil
	}, func(_ context.Context, got RoomEvent) error {
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
	})
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
			err := consumePersistentEvents(ctx, func(context.Context) (kafka.Message, error) {
				fetches++
				if fetches > 1 {
					t.Fatal("fetched past a failed message")
				}
				return kafka.Message{Value: []byte(test.payload)}, nil
			}, func(context.Context, RoomEvent) error {
				saves++
				if test.cancelOnSave {
					cancel()
					return errors.New("database offline")
				}
				return nil
			}, func(context.Context, ...kafka.Message) error {
				commits++
				return errors.New("commit unavailable")
			})
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
