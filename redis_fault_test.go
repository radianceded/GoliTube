package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func TestRedisRestartRecovery(t *testing.T) {
	if os.Getenv("GOLITUBE_TEST_REDIS_RESTART") != "1" {
		t.Skip("设置 GOLITUBE_TEST_REDIS_RESTART=1 启用独立 Redis 停服恢复实验")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_, port, _ := net.SplitHostPort(addr)
	listener.Close()
	configPath := filepath.Join(t.TempDir(), "redis.conf")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(
		"bind 127.0.0.1\nport %s\nsave \"\"\nappendonly no\ndaemonize no\n", port,
	)), 0600); err != nil {
		t.Fatal(err)
	}
	serverConfig := configPath
	if runtime.GOOS == "windows" {
		volume := filepath.VolumeName(configPath)
		serverConfig = "/mnt/" + strings.ToLower(volume[:1]) + filepath.ToSlash(configPath[len(volume):])
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: addr, MaxRetries: -1,
		DialTimeout: 200 * time.Millisecond, ReadTimeout: 500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond, ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { rdb.Close() })
	var server *exec.Cmd
	var exited chan error
	running := false
	owned := func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		info, err := rdb.Info(ctx, "server").Result()
		return err == nil && strings.Contains(info, "\nconfig_file:"+serverConfig+"\r\n")
	}
	stop := func() {
		t.Helper()
		if !running {
			return
		}
		if !owned() {
			t.Error("拒绝停止 Redis：配置路径与测试专用实例不匹配")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		rdb.ShutdownNoSave(ctx)
		cancel()
		select {
		case err := <-exited:
			running = false
			if err != nil {
				t.Error("Redis 退出失败:", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("测试 Redis 未及时退出")
		}
	}
	t.Cleanup(stop)
	start := func() {
		t.Helper()
		if runtime.GOOS == "windows" {
			server = exec.Command("wsl.exe", "-d", "Ubuntu", "--", "redis-server", serverConfig)
		} else {
			server = exec.Command("redis-server", serverConfig)
		}
		server.Stdout, server.Stderr = os.Stdout, os.Stderr
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		exited = make(chan error, 1)
		command, done := server, exited
		go func() { done <- command.Wait() }()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case err := <-exited:
				t.Fatal("专用 Redis 启动失败:", err)
			default:
			}
			if owned() {
				running = true
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("专用 Redis 未就绪")
	}
	start()
	first, second := startTestInstance(t, addr), startTestInstance(t, addr)
	roomID := fmt.Sprintf("restart-%d", time.Now().UnixNano())
	dial := func(instance string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws://"+instance+"/ws?room="+roomID, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	send := func(conn *websocket.Conn, body string) {
		t.Helper()
		if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(conn *websocket.Conn, body string) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		kind, data, err := conn.ReadMessage()
		if err != nil || kind != websocket.TextMessage || string(data) != body {
			t.Fatalf("收到 %q type=%d err=%v，期望 %q", data, kind, err, body)
		}
	}
	a := dial(first)
	send(a, "ready A")
	receive(a, "ready A")
	b := dial(second)
	send(b, "before-failure")
	receive(a, "before-failure")
	receive(b, "before-failure")
	t.Log("正常阶段：两个独立实例跨实例收发通过")

	stop()
	if running {
		t.Fatal("Redis 未停止，不能继续故障实验")
	}
	send(a, "during-failure")
	if err := a.SetReadDeadline(time.Now().Add(6 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _, err = a.ReadMessage()
	if err == nil {
		t.Fatal("Redis 停服后意外收到本地广播")
	}
	if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
		t.Fatal("发送连接未主动关闭，只等到了客户端读超时")
	}
	t.Log("停服阶段：发布失败后发送连接关闭")

	start()
	// 等待两个仍在运行的 Go 实例重新订阅，而不是固定睡眠。
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		counts, err := rdb.PubSubNumSub(ctx, danmakuChannel).Result()
		cancel()
		if err == nil && counts[danmakuChannel] == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("两个 Go 实例没有恢复订阅: counts=%v err=%v", counts, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	a = dial(first)
	send(a, "after-recovery")
	receive(a, "after-recovery")
	receive(b, "after-recovery")
	send(b, "reverse-after-recovery")
	receive(a, "reverse-after-recovery")
	receive(b, "reverse-after-recovery")
	t.Log("恢复阶段：Go 进程未重启，A 重连、B 保持原连接，双向广播恢复；未收到故障期间消息")
}
