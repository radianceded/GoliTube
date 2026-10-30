package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// 使用测试二进制启动真实子进程，使两个实例拥有独立的 rooms 和 roomsMu。
func TestRedisInstanceProcess(t *testing.T) {
	if os.Getenv("GOLITUBE_INSTANCE_PROCESS") != "1" {
		t.Skip("仅由跨实例测试作为子进程启动")
	}
	if err := run(os.Getenv("GOLITUBE_INSTANCE_ADDR"), os.Getenv("GOLITUBE_TEST_REDIS_ADDR")); err != nil {
		t.Fatal(err)
	}
}

func TestCrossInstanceBroadcast(t *testing.T) {
	if os.Getenv("GOLITUBE_TEST_REDIS_ADDR") == "" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_ADDR 显式启用真实 Redis 测试")
	}
	startInstance := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := listener.Addr().String()
		listener.Close()
		command := exec.Command(os.Args[0], "-test.run=^TestRedisInstanceProcess$")
		command.Env = append(os.Environ(), "GOLITUBE_INSTANCE_PROCESS=1", "GOLITUBE_INSTANCE_ADDR="+addr)
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			command.Process.Kill()
			command.Wait()
		})
		client := &http.Client{Timeout: 300 * time.Millisecond}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			response, err := client.Get("http://" + addr + "/health")
			if err == nil {
				response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return addr
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("子进程未就绪: ", addr)
		return ""
	}
	first, second := startInstance(), startInstance()
	roomID := fmt.Sprintf("distributed-%d", time.Now().UnixNano())
	dial := func(addr, room string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws?room="+room, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	send := func(conn *websocket.Conn, kind int, body []byte) {
		t.Helper()
		if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(kind, body); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(conn *websocket.Conn, kind int, body []byte) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		gotKind, gotBody, err := conn.ReadMessage()
		if err != nil || gotKind != kind || string(gotBody) != string(body) {
			t.Fatalf("收到 type=%d data=%q err=%v，期望 type=%d data=%q", gotKind, gotBody, err, kind, body)
		}
	}

	// 回声确认成员已登记；后续消息同时检测是否重复投递上一条。
	a := dial(first, roomID)
	send(a, websocket.TextMessage, []byte("ready A"))
	receive(a, websocket.TextMessage, []byte("ready A"))
	b := dial(second, roomID)
	send(b, websocket.TextMessage, []byte("ready B"))
	receive(b, websocket.TextMessage, []byte("ready B"))
	receive(a, websocket.TextMessage, []byte("ready B"))
	c := dial(second, roomID+"-other")
	send(c, websocket.TextMessage, []byte("ready C"))
	receive(c, websocket.TextMessage, []byte("ready C"))

	send(a, websocket.TextMessage, []byte("A to B"))
	receive(a, websocket.TextMessage, []byte("A to B"))
	receive(b, websocket.TextMessage, []byte("A to B"))
	send(b, websocket.BinaryMessage, []byte{0, 255})
	receive(a, websocket.BinaryMessage, []byte{0, 255})
	receive(b, websocket.BinaryMessage, []byte{0, 255})
	send(c, websocket.TextMessage, []byte("only C"))
	receive(c, websocket.TextMessage, []byte("only C"))
	send(a, websocket.TextMessage, []byte("after C"))
	receive(a, websocket.TextMessage, []byte("after C"))
	receive(b, websocket.TextMessage, []byte("after C"))
}
