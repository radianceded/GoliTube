package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// 单实例测试注入本地转发，避免把 Redis 作为所有测试的前置条件。
func localPublish(_ context.Context, event RoomEvent) error {
	broadcast(event.RoomID, event.Type, event.Data)
	return nil
}

func waitForRoomCount(t *testing.T, roomID string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		roomsMu.Lock()
		members, exists := rooms[roomID]
		count := len(members)
		roomsMu.Unlock()
		if count == want && (want != 0 || !exists) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("room %q: connections = %d, exists = %v, want %d", roomID, count, exists, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBroadcast(t *testing.T) {
	server := httptest.NewServer(handleWebSocket(localPublish))
	defer server.Close()
	defer waitForRoomCount(t, "lobby", 0)

	clients := make([]*websocket.Conn, 2)
	for i := range clients {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = conn
		defer conn.Close()
	}
	waitForRoomCount(t, "lobby", 2)

	// 两个客户端同时发送，验证每个接收者都收到全部消息且顺序一致。
	const perClient = 10
	var writers sync.WaitGroup
	for i, conn := range clients {
		writers.Go(func() {
			if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Error(err)
				return
			}
			for j := range perClient {
				if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("%d:%d", i, j))); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	writers.Wait()

	var firstOrder []string
	for i, conn := range clients {
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		seen := make(map[string]bool)
		for j := range len(clients) * perClient {
			kind, message, err := conn.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			value := string(message)
			var sender, sequence int
			_, parseErr := fmt.Sscanf(value, "%d:%d", &sender, &sequence)
			if kind != websocket.TextMessage || parseErr != nil || sender < 0 || sender >= len(clients) || sequence < 0 || sequence >= perClient || seen[value] {
				t.Fatalf("unexpected message: type=%d, body=%q", kind, value)
			}
			seen[value] = true
			if i == 0 {
				firstOrder = append(firstOrder, value)
			} else if value != firstOrder[j] {
				t.Fatalf("message %d = %q, want %q", j, value, firstOrder[j])
			}
		}
	}

	clients[1].Close()
	waitForRoomCount(t, "lobby", 1)
	if err := clients[0].WriteMessage(websocket.BinaryMessage, []byte("still connected")); err != nil {
		t.Fatal(err)
	}
	kind, message, err := clients[0].ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || string(message) != "still connected" {
		t.Fatalf("after disconnect: type=%d, body=%q, err=%v", kind, message, err)
	}
}

func TestRoomIsolationAndRejoin(t *testing.T) {
	server := httptest.NewServer(handleWebSocket(localPublish))
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		waitForRoomCount(t, "red", 0)
		waitForRoomCount(t, "blue", 0)
	})

	dial := func(roomID string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+roomID, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	exchange := func(conn *websocket.Conn, message string) {
		t.Helper()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			t.Fatal(err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		kind, got, err := conn.ReadMessage()
		if err != nil || kind != websocket.TextMessage || string(got) != message {
			t.Fatalf("message = %q, type = %d, err = %v, want %q", got, kind, err, message)
		}
	}

	red, blue := dial("red"), dial("blue")
	waitForRoomCount(t, "red", 1)
	waitForRoomCount(t, "blue", 1)
	// 交替发送标记；若串房，下一次读取将先读到其他房间的消息。
	exchange(red, "red only")
	exchange(blue, "blue only")
	exchange(red, "red again")
	red.Close()
	waitForRoomCount(t, "red", 0)
	waitForRoomCount(t, "blue", 1)
	red = dial("red")
	waitForRoomCount(t, "red", 1)
	exchange(red, "new red room")
	exchange(blue, "blue still connected")
}
func TestFullQueueDisconnects(t *testing.T) {
	// 建立真实 WebSocket，但不启动发送者。
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			accepted <- conn
		},
	))
	defer server.Close()

	remote, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()

	conn := <-accepted
	defer conn.Close()

	client := &Client{
		Conn: conn,
		Send: make(chan Message, 1),
	}

	// 容量只有 1，放一条就满了。
	client.Send <- Message{
		Type: websocket.TextMessage,
		Data: []byte("old"),
	}

	const roomID = "test-full-queue"
	roomsMu.Lock()
	rooms[roomID] = map[*websocket.Conn]*Client{
		conn: client,
	}
	roomsMu.Unlock()

	defer func() {
		roomsMu.Lock()
		delete(rooms, roomID)
		roomsMu.Unlock()
	}()

	broadcast(roomID, websocket.TextMessage, []byte("new"))

	// 唯一成员被移除后，房间应被回收。
	roomsMu.Lock()
	_, exists := rooms[roomID]
	roomsMu.Unlock()
	if exists {
		t.Fatal("队列满后，空房间没有回收")
	}

	// 关闭 channel 不会清空缓冲，先取走旧消息。
	<-client.Send
	select {
	case _, open := <-client.Send:
		if open {
			t.Fatal("发送队列没有关闭")
		}
	default:
		t.Fatal("发送队列仍然打开")
	}

	// 被服务端关闭的连接，应当无法继续写入。
	if err := conn.WriteMessage(
		websocket.TextMessage, []byte("after close"),
	); err == nil {
		t.Fatal("连接没有关闭")
	}
}
func TestRoomNameLimit(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/ws?room="+strings.Repeat("a", 129), nil)
	response := httptest.NewRecorder()
	handleWebSocket(localPublish)(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestHeartbeat(t *testing.T) {
	const interval = 100 * time.Millisecond
	const wait = time.Second
	for _, reply := range []bool{true, false} {
		t.Run(fmt.Sprintf("reply=%v", reply), func(t *testing.T) {
			roomID := fmt.Sprintf("heartbeat-%v", reply)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				serveWebSocket(w, r, interval, wait, localPublish)
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { waitForRoomCount(t, roomID, 0) })
			conn, _, err := websocket.DefaultDialer.Dial(
				"ws"+strings.TrimPrefix(server.URL, "http")+"?room="+roomID, nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			waitForRoomCount(t, roomID, 1)
			if err := conn.SetReadDeadline(time.Now().Add(4 * wait)); err != nil {
				t.Fatal(err)
			}

			pings := make(chan struct{}, 1)
			conn.SetPingHandler(func(data string) error {
				select {
				case pings <- struct{}{}:
				default:
				}
				if !reply {
					return nil // 故意不回复 Pong，模拟心跳无响应。
				}
				return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(wait))
			})

			type readResult struct {
				kind int
				body []byte
				err  error
			}
			result := make(chan readResult, 1)
			go func() {
				kind, body, err := conn.ReadMessage() // 持续读取才能处理 Ping 并回复 Pong。
				result <- readResult{kind, body, err}
			}()

			if reply {
				// 超过初始读期限，连接仍应依靠 Pong 续期存活。
				timer := time.NewTimer(2 * wait)
				defer timer.Stop()
				select {
				case got := <-result:
					t.Fatalf("正常心跳连接提前结束: %+v", got)
				case <-timer.C:
				}
				if err := conn.SetWriteDeadline(time.Now().Add(wait)); err != nil {
					t.Fatal(err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, []byte("still alive")); err != nil {
					t.Fatal(err)
				}
			}

			select {
			case got := <-result:
				if reply {
					if got.err != nil || got.kind != websocket.TextMessage || string(got.body) != "still alive" {
						t.Fatalf("心跳续期后收发失败: %+v", got)
					}
				} else {
					if got.err == nil {
						t.Fatal("没有 Pong 的连接未关闭")
					}
					if timeout, ok := got.err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
						t.Fatalf("等到的是客户端自己的超时，而不是服务端断开: %v", got.err)
					}
					waitForRoomCount(t, roomID, 0)
				}
			case <-time.After(3 * wait):
				t.Fatal("等待心跳结果超时")
			}
			select {
			case <-pings:
			default:
				t.Fatal("未收到服务端 Ping")
			}
		})
	}
}

func TestPublishFailureClosesConnection(t *testing.T) {
	server := httptest.NewServer(handleWebSocket(func(context.Context, RoomEvent) error {
		return errors.New("模拟发布失败")
	}))
	defer server.Close()
	const roomID = "publish-failure"
	defer waitForRoomCount(t, roomID, 0)
	conn, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http")+"?room="+roomID, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitForRoomCount(t, roomID, 1)
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("will fail")); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("发布失败后不应收到本地回声")
	}
	if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
		t.Fatal("连接未被主动关闭，等到了客户端自己的超时")
	}
	waitForRoomCount(t, roomID, 0)
}
