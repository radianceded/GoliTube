package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/history"
	"github.com/radianceded/GoliTube/internal/persistence"
	"github.com/radianceded/GoliTube/internal/realtime"
	"github.com/redis/go-redis/v9"
)

func handleWebSocket(hub *realtime.Hub, publish func(context.Context, danmaku.RoomEvent) error, a *auth.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Cookie 固定来自本次握手；每次发送重新查 Redis，使退出、过期和轮换生效。
		hub.ServeWebSocket(w, r, realtime.PingInterval, realtime.PongWait, publish, func(room string, kind int, data []byte) (danmaku.RoomEvent, error) {
			if a == nil || a.RDB == nil {
				return danmaku.RoomEvent{}, &danmaku.ProtocolError{Type: "error", Code: "auth_unavailable", Message: "账号服务暂不可用"}
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			user, err := a.CurrentUser(ctx, r)
			cancel()
			if errors.Is(err, redis.Nil) {
				return danmaku.RoomEvent{}, &danmaku.ProtocolError{Type: "error", Code: "unauthenticated", Message: "请先登录，并重新建立弹幕连接"}
			}
			if err != nil {
				log.Println("弹幕发送身份查询失败:", err)
				return danmaku.RoomEvent{}, &danmaku.ProtocolError{Type: "error", Code: "auth_unavailable", Message: "账号服务暂不可用"}
			}
			return danmaku.Decode(room, kind, data, user.ID, user.Username)
		})
	}
}

func health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, "ccb")
}

// enqueue 为 nil 时仅通过 Redis 实时广播，不写入 Kafka。
func Run(addr, redisAddr string, enqueue func(context.Context, danmaku.RoomEvent) error) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:                  redisAddr,
		DialTimeout:           3 * time.Second,
		ReadTimeout:           3 * time.Second,
		WriteTimeout:          3 * time.Second,
		ContextTimeoutEnabled: true,
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := rdb.Ping(ctx).Err()
	cancel()
	if err != nil {
		return fmt.Errorf("连接 Redis 失败: %w", err)
	}
	log.Println("Redis 连接成功")

	hub := realtime.NewHub()
	subCtx, stopSubscription := context.WithCancel(context.Background())
	sub := rdb.Subscribe(subCtx, realtime.Channel)

	// Redis 确认订阅成功后，才开放 HTTP 服务。
	confirmCtx, cancelConfirm := context.WithTimeout(subCtx, 3*time.Second)
	_, err = sub.Receive(confirmCtx)
	cancelConfirm()
	if err != nil {
		stopSubscription()
		sub.Close()
		return fmt.Errorf("订阅 Redis 频道失败: %w", err)
	}

	subscriptionDone := make(chan struct{})
	go func() {
		defer close(subscriptionDone)
		hub.ConsumeEvents(subCtx, sub)
	}()
	defer func() {
		stopSubscription()
		sub.Close()
		<-subscriptionDone
	}()
	log.Println("Redis 频道订阅成功:", realtime.Channel)
	presenceDone := make(chan struct{})
	go func() {
		defer close(presenceDone)
		hub.ReportPresence(subCtx, rdb)
	}()
	defer func() {
		stopSubscription()
		<-presenceDone
	}()

	publish := func(ctx context.Context, event danmaku.RoomEvent) error {
		return realtime.Publish(ctx, rdb, event)
	}
	if enqueue != nil {
		publish = persistence.DurablePublish(enqueue, publish)
	}
	var query func(context.Context, history.Query) (history.Page, error)
	var appDB *sql.DB
	if dsn := os.Getenv("GOLITUBE_MYSQL_DSN"); dsn != "" {
		db, err := persistence.NewDB(dsn)
		if err != nil {
			return err
		}
		defer db.Close()
		appDB = db
		query = history.Cached(rdb, func(ctx context.Context, q history.Query) (history.Page, error) {
			return history.Fetch(ctx, db, q)
		})
	}
	a := &auth.Handler{DB: appDB, RDB: rdb, Namespace: "golitube", SecureCookie: os.Getenv("GOLITUBE_COOKIE_SECURE") == "1",
		TrustLoopbackProxy: os.Getenv("GOLITUBE_TRUST_LOOPBACK_PROXY") == "1"}
	router := newRouter(a, hub, query, publish)
	log.Printf("准备监听 http://%s", addr)

	return http.ListenAndServe(addr, router)
}
