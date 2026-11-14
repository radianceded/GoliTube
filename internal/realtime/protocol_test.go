package realtime

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
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/testutil"
)

// 只隔离测试协议；真实 /ws 必须通过 Redis 会话检查，不能使用此测试入口。
func handleProtocolWebSocket(publish func(context.Context, danmaku.RoomEvent) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		testHub.ServeWebSocket(w, r, PingInterval, PongWait, publish, func(room string, kind int, data []byte) (danmaku.RoomEvent, error) {
			return danmaku.Decode(room, kind, data, "protocol-test-user", "registered_name")
		})
	}
}

func TestDanmakuProtocol(t *testing.T) {
	accepted := make(chan danmaku.RoomEvent, 8)
	server := httptest.NewServer(handleProtocolWebSocket(func(ctx context.Context, event danmaku.RoomEvent) error {
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
		{"binary", websocket.BinaryMessage, testutil.DanmakuPayload(t, "hello")},
		{"invalid-utf8", websocket.TextMessage, []byte{0xff}},
		{"array", websocket.TextMessage, []byte(`[]`)},
		{"null", websocket.TextMessage, []byte(`null`)},
		{"missing-content", websocket.TextMessage, []byte(`{"position_ms":0}`)},
		{"empty-content", websocket.TextMessage, []byte(`{"content":"  ","position_ms":0}`)},
		{"too-long", websocket.TextMessage, testutil.DanmakuPayload(t, strings.Repeat("中", 256))},
		{"control-character", websocket.TextMessage, testutil.DanmakuPayload(t, "hello\nworld")},
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
			var response danmaku.ProtocolError
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
		payload, err := json.Marshal(danmaku.SendDanmaku{Content: content, PositionMS: &position, Color: "#AABBCC"})
		if err != nil {
			t.Fatal(err)
		}
		write(websocket.TextMessage, payload)
		body := read(sender)
		if got := read(peer); string(got) != string(body) {
			t.Fatal("peers received different events, or an error leaked to the room")
		}
		var received danmaku.Danmaku
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
	write(websocket.TextMessage, testutil.DanmakuPayload(t, "default color"))
	var received danmaku.Danmaku
	if err := json.Unmarshal(read(sender), &received); err != nil || received.Color != "#ffffff" {
		t.Fatalf("default color: %+v err=%v", received, err)
	}
	if got := read(peer); string(got) != string((<-accepted).Data) {
		t.Fatal("default-color event mismatch")
	}
}

func TestDanmakuFrameLimit(t *testing.T) {
	accepted := make(chan danmaku.RoomEvent, 1)
	server := httptest.NewServer(handleProtocolWebSocket(func(_ context.Context, event danmaku.RoomEvent) error { accepted <- event; return nil }))
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
	if err := conn.WriteMessage(websocket.TextMessage, []byte(strings.Repeat("x", danmaku.MaxMessageSize+1))); err != nil {
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
		testHub.ServeWebSocket(w, r, PingInterval, PongWait, localPublish, func(string, int, []byte) (danmaku.RoomEvent, error) {
			close(entered)
			<-release
			return danmaku.RoomEvent{}, errors.New("invalid message")
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
	testHub.mu.Lock()
	for connection, client := range testHub.rooms[room] {
		delete(testHub.rooms[room], connection)
		close(client.Send)
	}
	delete(testHub.rooms, room)
	testHub.mu.Unlock()
	close(release)
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not exit")
	}
}

func TestDanmakuUsername(t *testing.T) {
	user := struct{ ID, Username string }{"internal-user-id", "registered_name"}
	event, err := danmaku.Decode("room", websocket.TextMessage, testutil.DanmakuPayload(t, "hello"), user.ID, user.Username)
	if err != nil {
		t.Fatal(err)
	}
	var message danmaku.Danmaku
	if err := json.Unmarshal(event.Data, &message); err != nil {
		t.Fatal(err)
	}
	if message.UserID != user.ID || message.Username != user.Username {
		t.Fatalf("incorrect author: %+v", message)
	}
	_, err = danmaku.Decode("room", websocket.TextMessage, []byte(`{"content":"hello","position_ms":0,"username":"forged"}`), user.ID, user.Username)
	if err == nil {
		t.Fatal("client-supplied username was accepted")
	}
}

func TestSendLimitsLocalBucket(t *testing.T) {
	accepted := make(chan danmaku.RoomEvent, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testHub.ServeWebSocketLimited(w, r, PingInterval, PongWait, func(_ context.Context, event danmaku.RoomEvent) error {
			accepted <- event
			return nil
		}, func(room string, kind int, data []byte) (danmaku.RoomEvent, error) {
			return danmaku.Decode(room, kind, data, "limited-user", "limited_name")
		}, SendLimits{Rate: 0.001, Burst: 3, MaxStrikes: 2}) // 测试期间几乎不补充令牌。
	}))
	defer server.Close()
	const room = "send-limit-local"
	defer waitForRoomCount(t, room, 0)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	send := func() {
		t.Helper()
		if err := conn.WriteMessage(websocket.TextMessage, testutil.DanmakuPayload(t, "flood")); err != nil {
			t.Fatal(err)
		}
	}
	read := func() (danmaku.ProtocolError, error) {
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		var response danmaku.ProtocolError
		_, body, err := conn.ReadMessage()
		if err != nil {
			return response, err
		}
		return response, json.Unmarshal(body, &response)
	}

	// 桶容量 3：前三条通过，不产生错误回复。
	for range 3 {
		send()
		select {
		case <-accepted:
		case <-time.After(3 * time.Second):
			t.Fatal("限额内的消息没有到达发布器")
		}
	}
	// 之后的消息被拒绝，只回复发送者，不进入发布器；连接保持可用。
	for range 2 {
		send()
		if response, err := read(); err != nil || response.Code != "rate_limited" || response.Type != "error" {
			t.Fatalf("response=%+v err=%v", response, err)
		}
	}
	if len(accepted) != 0 {
		t.Fatal("被限流的消息进入了发布器")
	}
	// 连续超过 MaxStrikes 次后以 1008 关闭连接。
	send()
	_, err = read()
	if !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("expected policy-violation close, got %v", err)
	}
	if len(accepted) != 0 {
		t.Fatal("被限流的消息进入了发布器")
	}
}

// 令牌恢复后，被限流的连接可以继续发送，且成功发送会清零连续超限计数。
func TestSendLimitsRecoverAndResetStrikes(t *testing.T) {
	accepted := make(chan danmaku.RoomEvent, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testHub.ServeWebSocketLimited(w, r, PingInterval, PongWait, func(_ context.Context, event danmaku.RoomEvent) error {
			accepted <- event
			return nil
		}, func(room string, kind int, data []byte) (danmaku.RoomEvent, error) {
			return danmaku.Decode(room, kind, data, "recover-user", "recover_name")
		}, SendLimits{Rate: 20, Burst: 1, MaxStrikes: 2}) // 每 50ms 补充一个令牌。
	}))
	defer server.Close()
	const room = "send-limit-recover"
	defer waitForRoomCount(t, room, 0)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+room, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for round := range 4 { // 超过 MaxStrikes 的总拒绝次数，但每轮之间都有成功发送。
		for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(60 * time.Millisecond) {
			if err := conn.WriteMessage(websocket.TextMessage, testutil.DanmakuPayload(t, "paced")); err != nil {
				t.Fatal(err)
			}
			select {
			case <-accepted:
			case <-time.After(30 * time.Millisecond):
				if time.Now().After(deadline) {
					t.Fatalf("第 %d 轮令牌没有恢复", round)
				}
				continue
			}
			break
		}
		// 紧接着再发一条，必然超限一次。
		if err := conn.WriteMessage(websocket.TextMessage, testutil.DanmakuPayload(t, "too fast")); err != nil {
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, body, err := conn.ReadMessage()
		var response danmaku.ProtocolError
		if err != nil || json.Unmarshal(body, &response) != nil || response.Code != "rate_limited" {
			t.Fatalf("round %d: body=%q err=%v", round, body, err)
		}
	}
}

func TestCloseAllSendsGoingAway(t *testing.T) {
	hub := NewHub()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWebSocket(w, r, PingInterval, PongWait, localPublish, rawTestEvent)
	}))
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	var conns []*websocket.Conn
	for _, room := range []string{"shutdown-a", "shutdown-a", "shutdown-b"} {
		conn, _, err := websocket.DefaultDialer.Dial(url+"?room="+room, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns = append(conns, conn)
	}
	for deadline := time.Now().Add(3 * time.Second); len(hub.LocalRoomCounts()) != 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("连接没有加入房间")
		}
	}

	// 客户端在 Close 回调里按协议回复关闭帧，随后服务端读循环退出并清理成员。
	closed := make(chan int, len(conns))
	for _, conn := range conns {
		conn.SetCloseHandler(func(code int, text string) error {
			closed <- code
			return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), time.Now().Add(time.Second))
		})
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := time.Now()
	hub.CloseAll(ctx)
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("CloseAll 耗时 %v，应在客户端响应后立即返回", elapsed)
	}
	for range conns {
		select {
		case code := <-closed:
			if code != websocket.CloseGoingAway {
				t.Fatalf("close code = %d, want %d", code, websocket.CloseGoingAway)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("客户端没有收到关闭帧")
		}
	}
	if counts := hub.LocalRoomCounts(); len(counts) != 0 {
		t.Fatalf("CloseAll 返回后仍有成员: %v", counts)
	}

	// 关闭后不再接纳新连接：握手成功，但立刻收到 1001。
	late, _, err := websocket.DefaultDialer.Dial(url+"?room=shutdown-late", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	late.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := late.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseGoingAway) {
		t.Fatalf("late connection: %v", err)
	}
	if counts := hub.LocalRoomCounts(); len(counts) != 0 {
		t.Fatalf("关闭后的连接加入了房间: %v", counts)
	}
}

// 客户端不响应关闭帧时，CloseAll 在 ctx 到期后强制断开，不会无限等待。
func TestCloseAllForcesUnresponsiveClients(t *testing.T) {
	hub := NewHub()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWebSocket(w, r, PingInterval, PongWait, localPublish, rawTestEvent)
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room=stuck", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() // 不读取，因此永远不会回复关闭帧。
	for deadline := time.Now().Add(3 * time.Second); len(hub.LocalRoomCounts()) != 1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("连接没有加入房间")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	hub.CloseAll(ctx)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("CloseAll 没有在期限后返回: %v", elapsed)
	}
	for deadline := time.Now().Add(3 * time.Second); len(hub.LocalRoomCounts()) != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("强制断开后成员没有清理")
		}
	}
}
