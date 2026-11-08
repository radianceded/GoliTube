package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

const sessionCookie = "golitube_session"
const sessionTTL = 24 * time.Hour

var usernamePattern = regexp.MustCompile(`^[a-z0-9_]{3,32}$`)
var dummyPasswordHash = func() []byte {
	hash, err := bcrypt.GenerateFromPassword([]byte("not-a-real-account"), bcrypt.DefaultCost)
	if err != nil {
		panic(err)
	}
	return hash
}()

type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type authHandler struct {
	db                 *sql.DB
	rdb                *redis.Client
	namespace          string
	secureCookie       bool
	trustLoopbackProxy bool
}

func authJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func authError(w http.ResponseWriter, status int, code, message string) {
	authJSON(w, status, ProtocolError{Type: "error", Code: code, Message: message})
}

func (a *authHandler) unavailable(w http.ResponseWriter, err error) {
	// 不输出请求正文、密码、Cookie 或会话令牌。
	log.Println("账号服务暂不可用:", err)
	authError(w, 503, "auth_unavailable", "账号服务暂不可用")
}

func (a *authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	method := http.MethodPost
	if r.URL.Path == "/auth/me" {
		method = http.MethodGet
	}
	switch r.URL.Path {
	case "/auth/register", "/auth/login", "/auth/me", "/auth/logout":
	default:
		authError(w, 404, "not_found", "接口不存在")
		return
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		authError(w, 405, "method_not_allowed", "请求方法不支持")
		return
	}
	if method == http.MethodPost && !sameOrigin(r, a.secureCookie) {
		authError(w, 403, "origin_rejected", "不接受跨站账号操作")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	if a.rdb == nil {
		a.unavailable(w, errors.New("Redis 未配置"))
		return
	}
	switch r.URL.Path {
	case "/auth/me":
		user, err := a.currentUser(ctx, r)
		if errors.Is(err, redis.Nil) {
			authError(w, 401, "unauthenticated", "请先登录")
			return
		}
		if err != nil {
			a.unavailable(w, err)
			return
		}
		authJSON(w, 200, map[string]User{"user": user})
	case "/auth/logout":
		token := requestSession(r)
		if token != "" {
			if err := a.rdb.Del(ctx, a.sessionKey(token)).Err(); err != nil {
				a.unavailable(w, err)
				return
			}
		}
		a.setCookie(w, r, "", -1)
		w.WriteHeader(http.StatusNoContent)
	default:
		if a.db == nil {
			a.unavailable(w, errors.New("MySQL 未配置"))
			return
		}
		if !a.allowAttempt(w, r) {
			return
		}
		input, err := readCredentials(w, r)
		if err != nil {
			authError(w, 400, "invalid_credentials", err.Error())
			return
		}
		if r.URL.Path == "/auth/register" {
			a.register(w, r, input)
		} else {
			a.login(w, r, input)
		}
	}
}

func sameOrigin(r *http.Request, secureCookie bool) bool {
	if r.Header.Get("Origin") == "" {
		return true // 允许没有 Origin 的原生客户端。
	}
	origin, err := url.Parse(r.Header.Get("Origin"))
	scheme := "http"
	if secureCookie || r.TLS != nil {
		scheme = "https"
	}
	return err == nil && origin.Scheme == scheme && strings.EqualFold(origin.Host, r.Host) &&
		origin.User == nil && (origin.Path == "" || origin.Path == "/") && origin.RawQuery == "" && origin.Fragment == ""
}

func readCredentials(w http.ResponseWriter, r *http.Request) (credentials, error) {
	var input credentials
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil || !utf8.Valid(data) {
		return input, errors.New("请求须为不超过 4096 字节的 UTF-8 JSON")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return input, errors.New("仅接受 username 和 password")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return input, errors.New("只接受一个 JSON 对象")
	}
	input.Username = strings.ToLower(strings.TrimSpace(input.Username))
	if !usernamePattern.MatchString(input.Username) {
		return input, errors.New("用户名须为 3～32 位字母、数字或下划线")
	}
	if utf8.RuneCountInString(input.Password) < 8 || len(input.Password) > 72 || strings.TrimSpace(input.Password) == "" {
		return input, errors.New("密码至少 8 个字符且不超过 72 个 UTF-8 字节")
	}
	return input, nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func (a *authHandler) sessionKey(token string) string {
	return a.namespace + ":session:" + digest(token)
}
func (a *authHandler) rateKey(ip string) string { return a.namespace + ":auth-rate:" + digest(ip) }

func (a *authHandler) allowAttempt(w http.ResponseWriter, r *http.Request) bool {
	ip := a.clientIP(r)
	count, err := a.rdb.Eval(r.Context(), `local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],60) end; return n`, []string{a.rateKey(ip)}).Int64()
	if err != nil {
		a.unavailable(w, err)
		return false
	}
	if count > 30 {
		w.Header().Set("Retry-After", "60")
		authError(w, 429, "rate_limited", "账号请求过于频繁，请稍后重试")
		return false
	}
	return true
}

func (a *authHandler) clientIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	if a.trustLoopbackProxy && net.ParseIP(ip).IsLoopback() {
		if forwarded := net.ParseIP(r.Header.Get("X-Real-IP")); forwarded != nil {
			return forwarded.String()
		}
	}
	return ip
}

func (a *authHandler) register(w http.ResponseWriter, r *http.Request, input credentials) {
	hash, err := bcrypt.GenerateFromPassword([]byte(input.Password), bcrypt.DefaultCost)
	if err != nil {
		a.unavailable(w, err)
		return
	}
	user := User{ID: rand.Text(), Username: input.Username}
	_, err = a.db.ExecContext(r.Context(), "INSERT INTO users (id,username,password_hash) VALUES (?,?,?)", user.ID, user.Username, hash)
	var conflict *mysql.MySQLError
	if errors.As(err, &conflict) && conflict.Number == 1062 {
		authError(w, 409, "username_taken", "用户名已存在")
		return
	}
	if err != nil {
		a.unavailable(w, err)
		return
	}
	authJSON(w, 201, map[string]User{"user": user})
}

func (a *authHandler) login(w http.ResponseWriter, r *http.Request, input credentials) {
	var user User
	var hash []byte
	err := a.db.QueryRowContext(r.Context(), "SELECT id,username,password_hash FROM users WHERE username = ?", input.Username).Scan(&user.ID, &user.Username, &hash)
	found := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		a.unavailable(w, err)
		return
	}
	if !found {
		hash = dummyPasswordHash
	}
	passwordErr := bcrypt.CompareHashAndPassword(hash, []byte(input.Password))
	if !found || passwordErr != nil {
		authError(w, 401, "login_failed", "用户名或密码错误")
		return
	}
	token := rand.Text()
	payload, _ := json.Marshal(user)
	keys := []string{a.sessionKey(token)}
	if old := requestSession(r); old != "" {
		keys = append(keys, a.sessionKey(old))
	}
	err = a.rdb.Eval(r.Context(), `redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2]); if #KEYS>1 then redis.call('DEL',KEYS[2]) end; return 1`, keys, payload, int(sessionTTL/time.Second)).Err()
	if err != nil {
		a.unavailable(w, err)
		return
	}
	a.setCookie(w, r, token, int(sessionTTL/time.Second))
	authJSON(w, 200, map[string]User{"user": user})
}

func requestSession(r *http.Request) string {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil || len(cookie.Value) < 26 || len(cookie.Value) > 128 {
		return ""
	}
	return cookie.Value
}

func (a *authHandler) currentUser(ctx context.Context, r *http.Request) (User, error) {
	token := requestSession(r)
	if token == "" {
		return User{}, redis.Nil
	}
	data, err := a.rdb.Get(ctx, a.sessionKey(token)).Bytes()
	if err != nil {
		return User{}, err
	}
	var user User
	if err := json.Unmarshal(data, &user); err != nil || user.ID == "" || len(user.ID) > 128 || !utf8.ValidString(user.ID) || !usernamePattern.MatchString(user.Username) {
		return User{}, errors.New("会话记录无效")
	}
	return user, nil
}

func (a *authHandler) setCookie(w http.ResponseWriter, r *http.Request, token string, maxAge int) {
	cookie := &http.Cookie{Name: sessionCookie, Value: token, Path: "/", HttpOnly: true, Secure: a.secureCookie || r.TLS != nil, SameSite: http.SameSiteLaxMode, MaxAge: maxAge}
	if maxAge < 0 {
		cookie.Expires = time.Unix(1, 0)
	} else {
		cookie.Expires = time.Now().Add(sessionTTL)
	}
	http.SetCookie(w, cookie)
}
