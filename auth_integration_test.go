package main

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
	db, err := openPersistenceDB(ctx, dsn)
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
	username := strings.ToLower("u_" + rand.Text())
	password := "test-password-123"
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if _, err := db.ExecContext(cleanupCtx, "DELETE FROM users WHERE username = ?", username); err != nil {
			t.Error(err)
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
	a := &authHandler{db: db, rdb: rdb, namespace: namespace}
	first, second := httptest.NewServer(a), httptest.NewServer(&authHandler{db: db, rdb: rdb, namespace: namespace})
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
		User User `json:"user"`
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
	if ttl, err := rdb.TTL(ctx, a.sessionKey(old.Value)).Result(); err != nil || ttl < 23*time.Hour || ttl > sessionTTL {
		t.Fatal("incorrect session TTL")
	}
	payload, _ = request(client, "GET", second.URL+"/auth/me", nil, 200)
	var identity struct {
		User User `json:"user"`
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
	current := *cookies[0]
	request(client, "POST", second.URL+"/auth/logout", nil, 204)
	request(client, "GET", first.URL+"/auth/me", nil, 401)
	replay(&current, 401)
	request(client, "POST", first.URL+"/auth/logout", nil, 204)
	_, cookies = request(client, "POST", first.URL+"/auth/login", credentials, 200)
	if err := rdb.PExpire(ctx, a.sessionKey(cookies[0].Value), time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if exists, err := rdb.Exists(ctx, a.sessionKey(cookies[0].Value)).Result(); err != nil {
			t.Fatal(err)
		} else if exists == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	request(client, "GET", second.URL+"/auth/me", nil, 401)
	secure := httptest.NewTLSServer(&authHandler{db: db, rdb: rdb, namespace: namespace, secureCookie: true})
	defer secure.Close()
	_, cookies = request(secure.Client(), "POST", secure.URL+"/auth/login", credentials, 200)
	if len(cookies) != 1 || !cookies[0].Secure {
		t.Fatal("HTTPS cookie is not secure")
	}
	// 单独制造测试命名空间限流，不消耗开发实例配额。
	if err := rdb.Set(ctx, a.rateKey("127.0.0.1"), 30, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	request(client, "POST", first.URL+"/auth/login", credentials, 429)
	t.Log("真实 MySQL/Redis：注册、重复用户名、密码哈希、登录、两个 HTTP 服务共享会话、令牌轮换、退出重放、过期、Secure Cookie 和限流通过")
}
