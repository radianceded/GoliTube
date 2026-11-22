package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/history"
	"github.com/radianceded/GoliTube/internal/metrics"
	"github.com/radianceded/GoliTube/internal/persistence"
	"github.com/radianceded/GoliTube/internal/ratelimit"
	"github.com/radianceded/GoliTube/internal/realtime"
	"github.com/redis/go-redis/v9"
)

// 发送限流参数。本地令牌桶挡住单连接洪水；按用户的窗口限流跨连接、跨实例生效，
// 防止同一账号开多个标签页或多个连接绕过本地限制。
const (
	connectionRate     = 2 // 每个连接每秒补充的令牌数
	connectionBurst    = 5 // 每个连接允许的瞬时突发
	connectionStrikes  = 50
	userSendLimit      = 10   // 每个用户每个窗口的最大发送次数
	userSendWindowMS   = 5000 // 窗口长度，毫秒
	userLimitKeyPrefix = "golitube:send-rate:"
)

func handleWebSocket(hub *realtime.Hub, publish func(context.Context, danmaku.RoomEvent) error, a *auth.Handler) http.HandlerFunc {
	limits := realtime.SendLimits{Rate: connectionRate, Burst: connectionBurst, MaxStrikes: connectionStrikes}
	return func(w http.ResponseWriter, r *http.Request) {
		// Cookie 固定来自本次握手；每次发送重新查 Redis，使退出、过期和轮换生效。
		hub.ServeWebSocketLimited(w, r, realtime.PingInterval, realtime.PongWait, publish, func(room string, kind int, data []byte) (danmaku.RoomEvent, error) {
			if a == nil || a.RDB == nil {
				return danmaku.RoomEvent{}, &danmaku.ProtocolError{Type: "error", Code: "auth_unavailable", Message: "账号服务暂不可用"}
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			user, err := a.CurrentUser(ctx, r)
			if errors.Is(err, redis.Nil) {
				return danmaku.RoomEvent{}, &danmaku.ProtocolError{Type: "error", Code: "unauthenticated", Message: "请先登录，并重新建立弹幕连接"}
			}
			if err != nil {
				log.Println("弹幕发送身份查询失败:", err)
				return danmaku.RoomEvent{}, &danmaku.ProtocolError{Type: "error", Code: "auth_unavailable", Message: "账号服务暂不可用"}
			}
			// 在解码之前计数：非法消息同样让服务端做了查询和解析，不应绕过限制。
			allowed, err := ratelimit.Allow(ctx, a.RDB, userLimitKeyPrefix+auth.Digest(user.ID), userSendLimit, userSendWindowMS)
			if err != nil {
				log.Println("弹幕发送限流检查失败:", err)
				return danmaku.RoomEvent{}, &danmaku.ProtocolError{Type: "error", Code: "auth_unavailable", Message: "账号服务暂不可用"}
			}
			if !allowed {
				metrics.RateLimited.Add(1)
				return danmaku.RoomEvent{}, realtime.RateLimitedError()
			}
			return danmaku.Decode(room, kind, data, user.ID, user.Username)
		}, limits)
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

// 收到退出信号后，等待在途 HTTP 请求和 WebSocket 收尾的最长时间。
const shutdownTimeout = 10 * time.Second

// 限制慢速请求头攻击和空闲的 keep-alive 连接长期占用文件描述符。
// readHeaderTimeout 是变量，只为让测试不必等待 5 秒。
var readHeaderTimeout = 5 * time.Second

const idleTimeout = 2 * time.Minute

// enqueue 为 nil 时仅通过 Redis 实时广播，不写入 Kafka。
// ctx 取消后优雅退出：停止接收新连接，等待在途请求，再向 WebSocket 客户端发送关闭帧。
func Run(ctx context.Context, addr, redisAddr string, enqueue func(context.Context, danmaku.RoomEvent) error) error {
	// 压测时关闭逐连接日志；错误日志和周期汇总保留。
	realtime.SetConnectionLogging(os.Getenv("GOLITUBE_QUIET") != "1")

	rdb := redis.NewClient(&redis.Options{
		Addr:                  redisAddr,
		DialTimeout:           3 * time.Second,
		ReadTimeout:           3 * time.Second,
		WriteTimeout:          3 * time.Second,
		ContextTimeoutEnabled: true,
	})
	defer rdb.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 3*time.Second)
	err := rdb.Ping(pingCtx).Err()
	cancelPing()
	if err != nil {
		return fmt.Errorf("连接 Redis 失败: %w", err)
	}
	log.Println("Redis 连接成功")

	hub := realtime.NewHub()
	hub.EnableBroadcastBatching(realtime.DefaultBroadcastBatchSize, realtime.DefaultBroadcastBatchWindow)
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
	// 计时包住 Kafka 接收确认与 Redis 发布，不含 MySQL 落库。
	measuredPublish := publish
	publish = func(ctx context.Context, event danmaku.RoomEvent) error {
		started := time.Now()
		err := measuredPublish(ctx, event)
		metrics.PublishLatency.Record(time.Since(started))
		if err != nil {
			metrics.PublishErrors.Add(1)
		} else {
			metrics.Publishes.Add(1)
		}
		return err
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
	metricsEnabled := os.Getenv("GOLITUBE_METRICS") == "1"
	registerDebugRoutes(router, hub, metricsEnabled, os.Getenv("GOLITUBE_PPROF") == "1")
	if metricsEnabled {
		metrics.StartReporter(ctx, 30*time.Second, metrics.Sources{Rooms: hub.LocalRoomCounts, Queued: hub.Queued})
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("准备监听 http://%s", addr)

	// 不设置 WriteTimeout：它会切断长时间运行的响应。已升级的 WebSocket 不受这些超时约束，
	// 连接存活由 Ping/Pong 期限管理。
	srv := &http.Server{Handler: router, ReadHeaderTimeout: readHeaderTimeout, IdleTimeout: idleTimeout}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	log.Println("收到退出信号，开始优雅退出")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	// Shutdown 立即关闭监听并等待在途请求，包括尚未完成升级的 WebSocket 握手；
	// 它不跟踪已升级的连接，所以之后由 Hub 通知这些客户端关闭，且不会再有新成员加入。
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Println("HTTP 服务未能在期限内完成退出，强制关闭:", err)
		srv.Close()
	}
	hub.CloseAll(shutdownCtx)
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
