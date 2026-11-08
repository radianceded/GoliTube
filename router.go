package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// 集中管理 HTTP 路由，业务处理函数继续接收标准 Request 和 ResponseWriter。
func newRouter(auth *authHandler, query func(context.Context, HistoryQuery) (HistoryPage, error), publish func(context.Context, RoomEvent) error) *gin.Engine {
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.HandleMethodNotAllowed = true
	// 部署代理的真实 IP 信任范围后续显式配置，账号限流继续使用直连地址。
	_ = router.SetTrustedProxies(nil)
	// 默认 Recovery 会转储请求头。这里丢弃原始转储，避免 Cookie 等信息进入日志。
	router.Use(gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) {
		log.Printf("HTTP 请求处理异常 method=%s route=%q", c.Request.Method, c.FullPath())
		if c.Writer.Written() {
			c.Abort()
			return
		}
		c.Header("Cache-Control", "no-store")
		c.AbortWithStatusJSON(http.StatusInternalServerError, ProtocolError{
			Type: "error", Code: "internal_error", Message: "服务暂时无法处理请求",
		})
	}))

	for _, path := range []string{"/", "/app.js", "/app.css"} {
		router.GET(path, gin.WrapF(serveWebClient))
		router.HEAD(path, gin.WrapF(serveWebClient))
	}
	router.GET("/health", gin.WrapF(health))
	websocketHandler := handleWebSocket(publish, auth)
	router.GET("/ws", gin.WrapF(websocketHandler))

	accounts := router.Group("/auth")
	accounts.POST("/register", gin.WrapH(auth))
	accounts.POST("/login", gin.WrapH(auth))
	accounts.GET("/me", gin.WrapH(auth))
	accounts.POST("/logout", gin.WrapH(auth))

	historyHandler := handleHistory(query)
	danmaku := router.Group("/danmaku")
	danmaku.GET("/history", gin.WrapF(historyHandler))
	onlineHandler := handleOnline(auth.rdb)
	router.GET("/rooms/online", gin.WrapF(onlineHandler))
	likesHandler := handleRoomLikes(auth)
	router.GET("/rooms/likes", gin.WrapF(likesHandler))
	router.PUT("/rooms/likes", gin.WrapF(likesHandler))

	// 复用原处理函数的 404/405、Allow 和 JSON 错误格式，兼容已有客户端。
	fallback := func(c *gin.Context) {
		c.Writer.Header().Del("Allow") // 由对应处理函数决定，覆盖 Gin 自动生成的值。
		switch path := c.Request.URL.Path; {
		case path == "/auth":
			target := "/auth/"
			if c.Request.URL.RawQuery != "" {
				target += "?" + c.Request.URL.RawQuery
			}
			http.Redirect(c.Writer, c.Request, target, http.StatusTemporaryRedirect)
		case strings.HasPrefix(path, "/auth/"):
			auth.ServeHTTP(c.Writer, c.Request)
		case path == "/health":
			health(c.Writer, c.Request)
		case path == "/ws":
			websocketHandler(c.Writer, c.Request)
		case path == "/danmaku/history":
			historyHandler(c.Writer, c.Request)
		case path == "/rooms/online":
			onlineHandler(c.Writer, c.Request)
		case path == "/rooms/likes":
			likesHandler(c.Writer, c.Request)
		default:
			serveWebClient(c.Writer, c.Request)
		}
	}
	router.NoRoute(fallback)
	router.NoMethod(fallback)
	return router
}
