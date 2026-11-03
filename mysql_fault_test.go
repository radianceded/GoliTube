package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

// 显式启用；只停止并清理本测试创建的 MySQL 容器，不操作开发用 MySQL。
func TestMySQLRestartRecovery(t *testing.T) {
	if os.Getenv("GOLITUBE_TEST_MYSQL_RESTART") != "1" {
		t.Skip("设置 GOLITUBE_TEST_MYSQL_RESTART=1 启用专用 MySQL 停服恢复实验")
	}
	redisAddr, brokers := os.Getenv("GOLITUBE_TEST_REDIS_ADDR"), os.Getenv("GOLITUBE_TEST_KAFKA_BROKERS")
	if redisAddr == "" || brokers == "" {
		t.Fatal("需要 GOLITUBE_TEST_REDIS_ADDR 和 GOLITUBE_TEST_KAFKA_BROKERS")
	}
	if runtime.GOOS == "windows" {
		keepAlive := exec.Command("wsl.exe", "-d", "Ubuntu", "--exec", "sleep", "180")
		if err := keepAlive.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { keepAlive.Process.Kill(); keepAlive.Wait() })
	}
	docker := os.Getenv("GOLITUBE_TEST_DOCKER_BIN")
	if docker == "" {
		docker = "docker"
	}
	dockerRun := func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, docker, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("Docker 操作失败: %v: %s", err, output)
		}
		return strings.TrimSpace(string(output))
	}
	token := rand.Text()
	password := rand.Text() // 凭据与会出现在日志中的房间、所有权标记分开。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	boundAddress := listener.Addr().String()
	listener.Close()
	// 先选空闲端口再显式绑定；Docker 自动分配的端口在 stop/start 后可能改变。
	container := dockerRun("run", "-d", "--label", "golitube.mysql-recovery="+token,
		"-p", boundAddress+":3306", "-e", "MYSQL_ROOT_PASSWORD="+password,
		"-e", "MYSQL_DATABASE=golitube_test", "-e", "MYSQL_USER=golitube",
		"-e", "MYSQL_PASSWORD="+password, "mysql:8.4")
	// 严格限定操作目标为 docker run 返回的本次容器 ID。
	if len(container) != 64 || strings.Trim(container, "0123456789abcdef") != "" {
		t.Fatalf("意外的容器 ID: %q", container)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, docker, "rm", "-f", "-v", container).CombinedOutput()
		if err != nil {
			t.Errorf("清理测试容器失败: %v: %s", err, output)
		}
	})
	owned := func() {
		t.Helper()
		label := dockerRun("inspect", "--format", `{{index .Config.Labels "golitube.mysql-recovery"}}`, container)
		if label != token {
			t.Fatal("测试容器所有权校验失败")
		}
	}
	owned()
	address := dockerRun("port", container, "3306/tcp")
	if host, _, err := net.SplitHostPort(address); err != nil || host != "127.0.0.1" || address != boundAddress {
		t.Fatalf("意外的测试 MySQL 地址: %q", address)
	}
	dsn := fmt.Sprintf("golitube:%s@tcp(%s)/golitube_test", password, address)
	var db *sql.DB
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		db, err = openPersistenceDB(context.Background(), dsn)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if db == nil {
		t.Fatal("测试 MySQL 未就绪")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	room := "mysql-recovery-" + token
	reader := newEventReader(strings.Split(brokers, ","), room)
	committed := make(chan string, 8)
	failed := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- consumePersistentEvents(ctx, reader.FetchMessage, func(ctx context.Context, event RoomEvent) error {
			err := saveRoomEvent(ctx, db, event)
			if err != nil && event.RoomID == room {
				select {
				case failed <- struct{}{}:
				default:
				}
			}
			return err
		}, func(ctx context.Context, messages ...kafka.Message) error {
			if err := reader.CommitMessages(ctx, messages...); err != nil {
				return err
			}
			for _, message := range messages {
				var event RoomEvent
				if err := json.Unmarshal(message.Value, &event); err != nil {
					return err
				}
				if event.RoomID == room {
					var danmaku Danmaku
					if err := json.Unmarshal(event.Data, &danmaku); err != nil {
						return err
					}
					committed <- danmaku.Content
				}
			}
			return nil
		})
	}()
	defer func() {
		cancel()
		reader.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("消费者未退出")
		}
	}()
	first, second := startTestInstance(t, redisAddr, brokers), startTestInstance(t, redisAddr, brokers)
	dial := func(addr string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws?room="+room, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	send := func(conn *websocket.Conn, value string) {
		t.Helper()
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, testDanmakuPayload(t, value)); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(conn *websocket.Conn, want string) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		kind, body, err := conn.ReadMessage()
		var event Danmaku
		decodeErr := json.Unmarshal(body, &event)
		if err != nil || decodeErr != nil || kind != websocket.TextMessage || event.Type != "danmaku" || event.Content != want || event.RoomID != room {
			t.Fatalf("收到 kind=%d body=%q err=%v，期望 %q", kind, body, err, want)
		}
	}
	waitCommit := func(want string) {
		t.Helper()
		select {
		case got := <-committed:
			if got != want {
				t.Fatalf("提交 %q，期望 %q", got, want)
			}
		case err := <-done:
			t.Fatalf("消费者提前退出: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatalf("等待提交 %q 超时", want)
		}
	}
	a := dial(first)
	send(a, "ready A")
	receive(a, "ready A")
	b := dial(second)
	send(b, "ready B")
	receive(a, "ready B")
	receive(b, "ready B")
	waitCommit("ready A")
	waitCommit("ready B")
	t.Log("基线：双实例广播和数据库保存、位点提交成功")
	owned()
	dockerRun("stop", "--time", "5", container)
	if dockerRun("inspect", "--format", "{{.State.Running}}", container) != "false" {
		t.Fatal("测试数据库未停止")
	}
	for _, body := range []string{"during outage 1", "during outage 2"} {
		send(a, body)
		receive(a, body)
		receive(b, body)
	}
	select {
	case <-failed:
	case err := <-done:
		t.Fatalf("消费者提前退出: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("未观察到真实写库失败")
	}
	select {
	case got := <-committed:
		t.Fatalf("停库期间不应提交位点: %q", got)
	default:
	}
	t.Log("停库期间：两条弹幕仍跨实例广播，观察到写库失败，尚未提交这两条事件的位点")
	owned()
	dockerRun("start", container)
	if dockerRun("port", container, "3306/tcp") != address {
		t.Fatal("测试 MySQL 重启后端口发生变化")
	}
	waitCommit("during outage 1")
	waitCommit("during outage 2")
	// 相同连接、相同消费者继续工作，不重启 Go 服务。
	send(b, "after recovery")
	receive(a, "after recovery")
	receive(b, "after recovery")
	waitCommit("after recovery")
	rows, err := db.QueryContext(ctx, "SELECT id, data FROM danmaku WHERE room_id = ?", room)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	expected := map[string]bool{"ready A": false, "ready B": false, "during outage 1": false, "during outage 2": false, "after recovery": false}
	ids := make(map[string]bool)
	for rows.Next() {
		var id, body string
		if err := rows.Scan(&id, &body); err != nil {
			t.Fatal(err)
		}
		var event Danmaku
		if err := json.Unmarshal([]byte(body), &event); err != nil {
			t.Fatal(err)
		}
		seen, exists := expected[event.Content]
		if !exists || seen || id == "" || ids[id] || event.ID != id || event.RoomID != room || event.PositionMS != 1234 {
			t.Fatalf("意外或重复记录: id=%q body=%q", id, body)
		}
		expected[event.Content], ids[id] = true, true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != len(expected) {
		t.Fatalf("入库 %d 条，期望 %d 条", len(ids), len(expected))
	}
	page, err := queryHistory(ctx, db, HistoryQuery{RoomID: room, StartMS: 0, EndMS: 2000, Limit: 10})
	if err != nil || len(page.Items) != len(expected) {
		t.Fatalf("恢复后历史查询条数=%d err=%v", len(page.Items), err)
	}
	for _, event := range page.Items {
		if !ids[event.ID] || event.PositionMS != 1234 {
			t.Fatalf("恢复后历史记录不一致: %+v", event)
		}
	}
	t.Log("恢复后：同一消费者自动补存、提交位点；原 WebSocket 连接继续收发；5 条事件各保存一行")
}
