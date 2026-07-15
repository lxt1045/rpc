package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

var BaseAuthToken = "56a290a0f397a832d4ba01b571a2742e53f8a6c5a3a229e7c095d6e2"

func BaseAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {

		bear := c.Request.Header.Get("Authorization")
		token := strings.Replace(bear, "Bearer ", "", 1)
		if token == "" {
			token, _ = c.GetQuery("Authorization")
			if token != BaseAuthToken {
				c.JSON(http.StatusUnauthorized, &Resp[struct{}]{
					Code:    -1,
					Message: "用户未授权，请重新登录!",
					Data:    struct{}{},
				})
				return
			}
		}
		c.Next()
	}
}

type Resp[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}
