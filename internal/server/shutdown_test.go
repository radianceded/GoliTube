package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/testutil"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

// 在当前进程内启动真实的 Run，便于用 ctx 触发优雅退出。
func runInProcess(t *testing.T, redisAddr string) (addr string, stop context.CancelFunc, done <-chan error) {
	t.Helper()
	addr = freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- Run(ctx, addr, redisAddr, nil) }()
	t.Cleanup(cancel)
	client := &http.Client{Timeout: 300 * time.Millisecond}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if response, err := client.Get("http://" + addr + "/health"); err == nil {
			response.Body.Close()
			return addr, cancel, result
		}
	}
	t.Fatal("服务未就绪: ", addr)
	return
}

func TestGracefulShutdown(t *testing.T) {
	redisAddr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_ADDR 启用真实 Redis 测试")
	}
	addr, stop, done := runInProcess(t, redisAddr)
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws?room=graceful-shutdown", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	closeCode := make(chan int, 1)
	conn.SetCloseHandler(func(code int, _ string) error {
		closeCode <- code
		return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), time.Now().Add(time.Second))
	})
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	started := time.Now()
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("优雅退出应返回 nil: %v", err)
		}
	case <-time.After(shutdownTimeout + 3*time.Second):
		t.Fatal("Run 没有在期限内返回")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("客户端响应了关闭帧，退出却耗时 %v", elapsed)
	}
	select {
	case code := <-closeCode:
		if code != websocket.CloseGoingAway {
			t.Fatalf("close code = %d, want %d", code, websocket.CloseGoingAway)
		}
	default:
		t.Fatal("客户端没有收到 1001 关闭帧")
	}
	// 端口已释放，新连接被拒绝。
	if c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		c.Close()
		t.Fatal("退出后仍在监听")
	}
}

// 不响应关闭帧的客户端不能拖住退出超过 shutdownTimeout。
func TestShutdownDoesNotHangOnStuckClient(t *testing.T) {
	redisAddr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_ADDR 启用真实 Redis 测试")
	}
	addr, stop, done := runInProcess(t, redisAddr)
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws?room=stuck-shutdown", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() // 从不读取，不会回复关闭帧。
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("返回 %v", err)
		}
	case <-time.After(shutdownTimeout + 5*time.Second):
		t.Fatal("被无响应客户端拖住，超过 shutdownTimeout 仍未退出")
	}
}

// 慢速请求头不能无限占用连接。
func TestReadHeaderTimeout(t *testing.T) {
	redisAddr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_ADDR 启用真实 Redis 测试")
	}
	old := readHeaderTimeout
	readHeaderTimeout = 300 * time.Millisecond
	defer func() { readHeaderTimeout = old }()
	addr, _, _ := runInProcess(t, redisAddr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// 只发送不完整的请求头，然后等待服务端断开。
	if _, err := conn.Write([]byte("GET /health HTTP/1.1\r\nHost: x\r\nX-Slow: ")); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 512)
	n, err := conn.Read(buf)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("慢速请求头没有被服务端超时断开 (read %d bytes)", n)
	}
}

// 同一用户的多个连接共享限额：开多个标签页不能绕过限流。
func TestUserSendLimitAcrossConnections(t *testing.T) {
	redisAddr := os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_ADDR 启用真实 Redis 测试")
	}
	first, second := startTestInstance(t, redisAddr), startTestInstance(t, redisAddr)
	headers := testutil.SessionHeader(t, redisAddr) // 一个用户，一个会话。
	room := fmt.Sprintf("user-limit-%d", time.Now().UnixNano())
	dial := func(addr string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws?room="+room, headers)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	// 两个实例上的三个连接，同一个用户。
	conns := []*websocket.Conn{dial(first), dial(first), dial(second)}
	// 先让每个连接各自确认已就绪，并消耗一次配额（同房间内每条广播所有连接都会收到）。
	counts := map[string]int{}
	deadline := time.Now().Add(6 * time.Second)
	sent := 0
	// 发送节奏低于每连接令牌桶（每秒 2 条、突发 5 条），所以拒绝只可能来自按用户的限额。
	for sent < userSendLimit+4 && time.Now().Before(deadline) {
		conn := conns[sent%len(conns)]
		if err := conn.WriteMessage(websocket.TextMessage, testutil.DanmakuPayload(t, "shared quota")); err != nil {
			t.Fatal(err)
		}
		sent++
		time.Sleep(60 * time.Millisecond)
	}
	// 收集发送者各自收到的私有错误。
	for _, conn := range conns {
		for {
			conn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
			_, body, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var kind struct {
				Type string `json:"type"`
				Code string `json:"code"`
			}
			if json.Unmarshal(body, &kind) == nil {
				switch {
				case kind.Type == "error":
					counts["error:"+kind.Code]++
				case kind.Type == "danmaku":
					counts["danmaku"]++
				}
			}
		}
	}
	// 每条被接受的弹幕会广播给 3 个连接。
	accepted := counts["danmaku"] / len(conns)
	if accepted != userSendLimit {
		t.Fatalf("三个连接合计只应接受 %d 条，实际 %d 条 (counts=%v)", userSendLimit, accepted, counts)
	}
	if counts["error:rate_limited"] != sent-userSendLimit {
		t.Fatalf("被拒绝的 %d 条应各收到一个 rate_limited，实际 counts=%v", sent-userSendLimit, counts)
	}
}
