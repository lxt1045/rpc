package http

import (
	"context"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/lxt1045/rpc/test/socks/service/http/middleware"
)

func NewWsRouter(ctx context.Context, f func(conn *websocket.Conn)) (router *gin.Engine, err error) {
	gin.SetMode(gin.ReleaseMode)

	router = gin.Default()
	ginconf := cors.DefaultConfig()
	ginconf.ExposeHeaders = []string{"Authorization"}
	ginconf.AllowCredentials = true
	ginconf.AllowAllOrigins = true
	ginconf.AllowHeaders = []string{"Origin", "token", " X-Requested-With", "Content-Length", "Content-Type", "Authorization"}
	router.Use(cors.New(ginconf))

	wsGroup := router.Group("/ws")
	wsGroup.Use(middleware.RateLimitMiddleware())
	wsGroup.Use(middleware.BaseAuthMiddleware())

	wsGroup.GET("/test", NewHandler(f))

	return
}

func NewHandler(f func(conn *websocket.Conn)) func(c *gin.Context) {
	return func(c *gin.Context) {
		// gin 处理 websocket handler
		upGrader := websocket.Upgrader{
			// cross origin domain
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			// 处理 Sec-WebSocket-Protocol Header
			Subprotocols: []string{c.GetHeader("Sec-WebSocket-Protocol")},
		}

		// token := ctx.Query("token")

		//建立连接
		conn, err := upGrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			c.JSON(http.StatusUnauthorized, &middleware.Resp[struct{}]{
				Code:    -1,
				Message: err.Error(),
				Data:    struct{}{},
			})
			return
		}

		go f(conn)
	}
}
