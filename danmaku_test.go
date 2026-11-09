package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testDanmakuPayload(t *testing.T, content string) []byte {
	t.Helper()
	position := int64(1234)
	payload, err := json.Marshal(SendDanmaku{Content: content, PositionMS: &position})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// 只隔离测试协议；真实 /ws 必须通过 Redis 会话检查，不能使用此测试入口。
func handleProtocolWebSocket(publish func(context.Context, RoomEvent) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		serveWebSocketWithDecoder(w, r, pingInterval, pongWait, publish, func(room string, kind int, data []byte) (RoomEvent, error) {
			return decodeDanmaku(room, kind, data, User{ID: "protocol-test-user", Username: "registered_name"})
		})
	}
}

func TestDanmakuProtocol(t *testing.T) {
	accepted := make(chan RoomEvent, 8)
	server := httptest.NewServer(handleProtocolWebSocket(func(ctx context.Context, event RoomEvent) error {
		accepted <- event
		return localPublish(ctx, event)
	}))
	defer server.Close()
	const room = "protocol-room"
	defer waitForRoomCount(t, room, 0)
	dial := func() *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, nil)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}
	sender, peer := dial(), dial()
	defer sender.Close()
	defer peer.Close()
	waitForRoomCount(t, room, 2)
	read := func(conn *websocket.Conn) []byte {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		kind, data, err := conn.ReadMessage()
		if err != nil || kind != websocket.TextMessage {
			t.Fatalf("kind=%d err=%v", kind, err)
		}
		return data
	}
	write := func(kind int, body []byte) {
		t.Helper()
		if err := sender.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := sender.WriteMessage(kind, body); err != nil {
			t.Fatal(err)
		}
	}
	invalid := []struct {
		name string
		kind int
		body []byte
	}{
		{"plain-text", websocket.TextMessage, []byte("hello")},
		{"binary", websocket.BinaryMessage, testDanmakuPayload(t, "hello")},
		{"invalid-utf8", websocket.TextMessage, []byte{0xff}},
		{"array", websocket.TextMessage, []byte(`[]`)},
		{"null", websocket.TextMessage, []byte(`null`)},
		{"missing-content", websocket.TextMessage, []byte(`{"position_ms":0}`)},
		{"empty-content", websocket.TextMessage, []byte(`{"content":"  ","position_ms":0}`)},
		{"too-long", websocket.TextMessage, testDanmakuPayload(t, strings.Repeat("中", 256))},
		{"control-character", websocket.TextMessage, testDanmakuPayload(t, "hello\nworld")},
		{"missing-position", websocket.TextMessage, []byte(`{"content":"hi"}`)},
		{"null-position", websocket.TextMessage, []byte(`{"content":"hi","position_ms":null}`)},
		{"negative-position", websocket.TextMessage, []byte(`{"content":"hi","position_ms":-1}`)},
		{"fractional-position", websocket.TextMessage, []byte(`{"content":"hi","position_ms":1.5}`)},
		{"string-position", websocket.TextMessage, []byte(`{"content":"hi","position_ms":"12"}`)},
		{"overflow-position", websocket.TextMessage, []byte(`{"content":"hi","position_ms":9223372036854775808}`)},
		{"invalid-color", websocket.TextMessage, []byte(`{"content":"hi","position_ms":0,"color":"red"}`)},
		{"forged-room", websocket.TextMessage, []byte(`{"content":"hi","position_ms":0,"room_id":"another-room"}`)},
		{"forged-id", websocket.TextMessage, []byte(`{"content":"hi","position_ms":0,"id":"forged"}`)},
		{"forged-user", websocket.TextMessage, []byte(`{"content":"hi","position_ms":0,"user_id":"forged"}`)},
		{"forged-time", websocket.TextMessage, []byte(`{"content":"hi","position_ms":0,"created_at_ms":1}`)},
		{"trailing-object", websocket.TextMessage, []byte(`{"content":"hi","position_ms":0} {}`)},
		{"trailing-garbage", websocket.TextMessage, []byte(`{"content":"hi","position_ms":0} bad`)},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			write(test.kind, test.body)
			var response ProtocolError
			if err := json.Unmarshal(read(sender), &response); err != nil || response.Type != "error" || response.Code != "invalid_danmaku" || response.Message == "" {
				t.Fatalf("unexpected error response: %+v err=%v", response, err)
			}
			if len(accepted) != 0 {
				t.Fatal("invalid message reached publisher")
			}
		})
	}
	started := time.Now().UnixMilli()
	var previousID string
	for i, content := range []string{"  你好  ", "你好", strings.Repeat("中", 255), strings.Repeat("😀", 255)} {
		position := int64(i) // 包含 position_ms=0，验证起点不是缺失字段。
		payload, err := json.Marshal(SendDanmaku{Content: content, PositionMS: &position, Color: "#AABBCC"})
		if err != nil {
			t.Fatal(err)
		}
		write(websocket.TextMessage, payload)
		body := read(sender)
		if got := read(peer); string(got) != string(body) {
			t.Fatal("peers received different events, or an error leaked to the room")
		}
		var received Danmaku
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatal(err)
		}
		if received.Type != "danmaku" || received.ID == "" || received.ID == previousID || received.RoomID != room || received.UserID != "protocol-test-user" ||
			received.Content != strings.TrimSpace(content) || received.PositionMS != position || received.Color != "#aabbcc" ||
			received.CreatedAtMS < started || received.CreatedAtMS > time.Now().UnixMilli() {
			t.Fatalf("unexpected event: %+v", received)
		}
		event := <-accepted
		if event.ID != received.ID || event.RoomID != received.RoomID || event.Type != websocket.TextMessage || string(event.Data) != string(body) {
			t.Fatal("queued event and broadcast event differ")
		}
		previousID = received.ID
	}
	// 省略颜色时使用默认白色；错误之后同一连接仍然可以继续发送。
	write(websocket.TextMessage, testDanmakuPayload(t, "default color"))
	var received Danmaku
	if err := json.Unmarshal(read(sender), &received); err != nil || received.Color != "#ffffff" {
		t.Fatalf("default color: %+v err=%v", received, err)
	}
	if got := read(peer); string(got) != string((<-accepted).Data) {
		t.Fatal("default-color event mismatch")
	}
}

func TestDanmakuFrameLimit(t *testing.T) {
	accepted := make(chan RoomEvent, 1)
	server := httptest.NewServer(handleProtocolWebSocket(func(_ context.Context, event RoomEvent) error { accepted <- event; return nil }))
	defer server.Close()
	const room = "oversized-protocol"
	defer waitForRoomCount(t, room, 0)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte(strings.Repeat("x", maxMessageSize+1))); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
		t.Fatalf("expected message-too-big close, got %v", err)
	}
	if len(accepted) != 0 {
		t.Fatal("oversized message reached publisher")
	}
}

func TestProtocolErrorAfterConcurrentRemoval(t *testing.T) {
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				t.Errorf("reply after removal panicked: %v", value)
			}
			close(finished)
		}()
		serveWebSocketWithDecoder(w, r, pingInterval, pongWait, localPublish, func(string, int, []byte) (RoomEvent, error) {
			close(entered)
			<-release
			return RoomEvent{}, errors.New("invalid message")
		})
	}))
	defer server.Close()
	const room = "concurrent-protocol-removal"
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("decoder did not start")
	}
	// 模拟广播线程在错误回复前移除慢客户端并关闭其发送队列。
	roomsMu.Lock()
	for connection, client := range rooms[room] {
		delete(rooms[room], connection)
		close(client.Send)
	}
	delete(rooms, room)
	roomsMu.Unlock()
	close(release)
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not exit")
	}
}

func TestDanmakuUsername(t *testing.T) {
	user := User{ID: "internal-user-id", Username: "registered_name"}
	event, err := decodeDanmaku("room", websocket.TextMessage, testDanmakuPayload(t, "hello"), user)
	if err != nil {
		t.Fatal(err)
	}
	var message Danmaku
	if err := json.Unmarshal(event.Data, &message); err != nil {
		t.Fatal(err)
	}
	if message.UserID != user.ID || message.Username != user.Username {
		t.Fatalf("incorrect author: %+v", message)
	}
	_, err = decodeDanmaku("room", websocket.TextMessage, []byte(`{"content":"hello","position_ms":0,"username":"forged"}`), user)
	if err == nil {
		t.Fatal("client-supplied username was accepted")
	}
}
