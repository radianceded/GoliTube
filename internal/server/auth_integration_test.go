package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/history"
	"github.com/radianceded/GoliTube/internal/persistence"
	"github.com/radianceded/GoliTube/internal/testutil"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

func TestAuthIntegration(t *testing.T) {
	dsn, addr := os.Getenv("GOLITUBE_TEST_MYSQL_DSN"), os.Getenv("GOLITUBE_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("设置测试 MySQL DSN 和 Redis 地址启用账号集成测试")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasSuffix(cfg.DBName, "_test") {
		t.Fatal("只允许专用 _test 数据库")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := persistence.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	namespace := "golitube:test-auth:" + rand.Text()
	room := "auth-room-" + rand.Text()
	username := strings.ToLower("u_" + rand.Text())
	password := "test-password-123"
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if _, err := db.ExecContext(cleanupCtx, "DELETE FROM users WHERE username = ?", username); err != nil {
			t.Error(err)
		}
		for _, table := range []string{"danmaku_history", "danmaku"} {
			if _, err := db.ExecContext(cleanupCtx, "DELETE FROM "+table+" WHERE room_id = ?", room); err != nil {
				t.Error(err)
			}
		}
		iterator := rdb.Scan(cleanupCtx, 0, namespace+":*", 100).Iterator()
		for iterator.Next(cleanupCtx) {
			if err := rdb.Del(cleanupCtx, iterator.Val()).Err(); err != nil {
				t.Error(err)
			}
		}
		if err := iterator.Err(); err != nil {
			t.Error(err)
		}
	}()
	a := &auth.Handler{DB: db, RDB: rdb, Namespace: namespace}
	accepted := make(chan danmaku.RoomEvent, 4)
	router := newRouter(a, testHub, nil, func(ctx context.Context, event danmaku.RoomEvent) error {
		accepted <- event
		return localPublish(ctx, event)
	})
	first := httptest.NewServer(router)
	second := httptest.NewServer(newRouter(&auth.Handler{DB: db, RDB: rdb, Namespace: namespace}, testHub, nil, localPublish))
	defer first.Close()
	defer second.Close()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 5 * time.Second}
	request := func(client *http.Client, method, target string, body any, want int) ([]byte, []*http.Cookie) {
		t.Helper()
		var data []byte
		if body != nil {
			var err error
			data, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			t.Fatalf("%s %s: status=%d want=%d", method, target, response.StatusCode, want)
		}
		if strings.Contains(string(payload), password) || strings.Contains(string(payload), "password_hash") {
			t.Fatal("credentials leaked into response")
		}
		return payload, response.Cookies()
	}
	credentials := map[string]string{"username": strings.ToUpper(username), "password": password}
	request(client, "GET", first.URL+"/auth/me", nil, 401)
	payload, _ := request(client, "POST", first.URL+"/auth/register", credentials, 201)
	var registered struct {
		User auth.User `json:"user"`
	}
	if err := json.Unmarshal(payload, &registered); err != nil || registered.User.ID == "" || registered.User.Username != username {
		t.Fatal("registration returned wrong identity")
	}
	request(client, "POST", second.URL+"/auth/register", credentials, 409)
	var hash []byte
	if err := db.QueryRowContext(ctx, "SELECT password_hash FROM users WHERE username = ?", username).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(hash, []byte(password)) || bcrypt.CompareHashAndPassword(hash, []byte(password)) != nil {
		t.Fatal("password was not stored as a valid bcrypt hash")
	}
	wrong, _ := request(client, "POST", first.URL+"/auth/login", map[string]string{"username": username, "password": "wrong-password"}, 401)
	missing, _ := request(client, "POST", first.URL+"/auth/login", map[string]string{"username": strings.ToLower("x_" + rand.Text()), "password": "wrong-password"}, 401)
	if !bytes.Equal(wrong, missing) {
		t.Fatal("login errors distinguish nonexistent users")
	}
	_, cookies := request(client, "POST", first.URL+"/auth/login", credentials, 200)
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatal("missing protected session cookie")
	}
	old := *cookies[0]
	defer waitForRoomCount(t, room, 0)
	dial := func(cookie *http.Cookie) *websocket.Conn {
		t.Helper()
		headers := http.Header{}
		if cookie != nil {
			headers.Set("Cookie", cookie.Name+"="+cookie.Value)
		}
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(first.URL, "http")+"/ws?room="+room, headers)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}
	read := func(conn *websocket.Conn) []byte {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, body, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	sendError := func(conn *websocket.Conn, payload []byte, code string) {
		t.Helper()
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			t.Fatal(err)
		}
		var failure danmaku.ProtocolError
		if err := json.Unmarshal(read(conn), &failure); err != nil || failure.Code != code || len(accepted) != 0 {
			t.Fatalf("rejected send: %+v err=%v", failure, err)
		}
	}
	guest, sender := dial(nil), dial(&old)
	defer guest.Close()
	defer sender.Close()
	waitForRoomCount(t, room, 2)
	sendError(guest, testutil.DanmakuPayload(t, "anonymous"), "unauthenticated")
	sendError(sender, []byte(`{"content":"spoof","position_ms":0,"user_id":"another-user"}`), "invalid_danmaku")
	if err := sender.WriteMessage(websocket.TextMessage, testutil.DanmakuPayload(t, "实名弹幕")); err != nil {
		t.Fatal(err)
	}
	broadcastBody := read(sender)
	if !bytes.Equal(read(guest), broadcastBody) {
		t.Fatal("anonymous receiver missed broadcast, or private error leaked")
	}
	event := <-accepted
	var item danmaku.Danmaku
	if json.Unmarshal(broadcastBody, &item) != nil || item.UserID != registered.User.ID || item.Username != username || !bytes.Equal(broadcastBody, event.Data) {
		t.Fatal("session identity was not used consistently")
	}
	// 此测试显式调用保存函数验证真实 MySQL 投影；Kafka 链路由独立测试覆盖。
	if err := persistence.SaveRoomEvent(ctx, db, event); err != nil {
		t.Fatal(err)
	}
	page, err := history.Fetch(ctx, db, history.Query{RoomID: room, StartMS: 0, EndMS: 60000, Limit: 200})
	if err != nil || len(page.Items) != 1 || page.Items[0] != item {
		t.Fatalf("history identity differs: %+v err=%v", page, err)
	}
	if ttl, err := rdb.TTL(ctx, a.SessionKey(old.Value)).Result(); err != nil || ttl < 23*time.Hour || ttl > auth.SessionTTL {
		t.Fatal("incorrect session TTL")
	}
	payload, _ = request(client, "GET", second.URL+"/auth/me", nil, 200)
	var identity struct {
		User auth.User `json:"user"`
	}
	if err := json.Unmarshal(payload, &identity); err != nil || identity.User != registered.User {
		t.Fatal("another instance did not recognize session")
	}
	_, cookies = request(client, "POST", second.URL+"/auth/login", credentials, 200)
	if len(cookies) != 1 || cookies[0].Value == old.Value {
		t.Fatal("login did not rotate session")
	}
	replay := func(cookie *http.Cookie, want int) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, "GET", first.URL+"/auth/me", nil)
		req.AddCookie(cookie)
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("session replay status=%d want=%d", response.StatusCode, want)
		}
	}
	replay(&old, 401)
	sendError(sender, testutil.DanmakuPayload(t, "rotated session"), "unauthenticated")
	current := *cookies[0]
	loggedOut := dial(&current)
	defer loggedOut.Close()
	request(client, "POST", second.URL+"/auth/logout", nil, 204)
	request(client, "GET", first.URL+"/auth/me", nil, 401)
	replay(&current, 401)
	sendError(loggedOut, testutil.DanmakuPayload(t, "logged out"), "unauthenticated")
	request(client, "POST", first.URL+"/auth/logout", nil, 204)
	_, cookies = request(client, "POST", first.URL+"/auth/login", credentials, 200)
	expired := dial(cookies[0])
	defer expired.Close()
	if err := rdb.PExpire(ctx, a.SessionKey(cookies[0].Value), time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if exists, err := rdb.Exists(ctx, a.SessionKey(cookies[0].Value)).Result(); err != nil {
			t.Fatal(err)
		} else if exists == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	request(client, "GET", second.URL+"/auth/me", nil, 401)
	sendError(expired, testutil.DanmakuPayload(t, "expired"), "unauthenticated")
	secure := httptest.NewTLSServer(&auth.Handler{DB: db, RDB: rdb, Namespace: namespace, SecureCookie: true})
	defer secure.Close()
	_, cookies = request(secure.Client(), "POST", secure.URL+"/auth/login", credentials, 200)
	if len(cookies) != 1 || !cookies[0].Secure {
		t.Fatal("HTTPS cookie is not secure")
	}
	// 单独制造测试命名空间限流，不消耗开发实例配额。
	if err := rdb.Set(ctx, a.RateKey("127.0.0.1"), 30, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	request(client, "POST", first.URL+"/auth/login", credentials, 429)
	t.Log("真实 MySQL/Redis：注册登录、共享会话、游客接收、发送鉴权、防伪造、作者入库与历史一致、旧连接轮换/退出/过期失效、Secure Cookie 和限流通过")
}
