package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

var (
	defaultRatePerSec = 10000 // 每秒个数
	tokenCapacity     = 10000 // 总token容量

	limiterDefault = rate.NewLimiter(rate.Limit(defaultRatePerSec), tokenCapacity)
)

func RateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		limiter := limiterDefault
		allow := limiter.Allow()

		if !allow {
			c.JSON(http.StatusUnauthorized, &Resp[struct{}]{
				Code:    -1,
				Message: "请求过于频繁，请稍后再试!",
				Data:    struct{}{},
			})
			return
		}
		c.Next()
	}
}
