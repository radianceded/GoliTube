package server

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/radianceded/GoliTube/internal/auth"
	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/history"
	"github.com/radianceded/GoliTube/internal/likes"
	"github.com/radianceded/GoliTube/internal/realtime"
	"github.com/radianceded/GoliTube/internal/web"
)

// 集中管理 HTTP 路由，业务处理函数继续接收标准 Request 和 ResponseWriter。
func newRouter(a *auth.Handler, hub *realtime.Hub, query func(context.Context, history.Query) (history.Page, error), publish func(context.Context, danmaku.RoomEvent) error) *gin.Engine {
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
		c.AbortWithStatusJSON(http.StatusInternalServerError, danmaku.ProtocolError{
			Type: "error", Code: "internal_error", Message: "服务暂时无法处理请求",
		})
	}))

	for _, path := range []string{"/", "/app.js", "/app.css"} {
		router.GET(path, gin.WrapF(web.ServeClient))
		router.HEAD(path, gin.WrapF(web.ServeClient))
	}
	router.GET("/health", gin.WrapF(health))
	websocketHandler := handleWebSocket(hub, publish, a)
	router.GET("/ws", gin.WrapF(websocketHandler))

	accounts := router.Group("/auth")
	accounts.POST("/register", gin.WrapH(a))
	accounts.POST("/login", gin.WrapH(a))
	accounts.GET("/me", gin.WrapH(a))
	accounts.POST("/logout", gin.WrapH(a))

	historyHandler := history.Handler(query)
	danmakuRoutes := router.Group("/danmaku")
	danmakuRoutes.GET("/history", gin.WrapF(historyHandler))
	onlineHandler := realtime.OnlineHandler(a.RDB)
	router.GET("/rooms/online", gin.WrapF(onlineHandler))
	likesHandler := likes.Handler(a)
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
			a.ServeHTTP(c.Writer, c.Request)
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
			web.ServeClient(c.Writer, c.Request)
		}
	}
	router.NoRoute(fallback)
	router.NoMethod(fallback)
	return router
}
