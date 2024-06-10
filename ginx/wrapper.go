package ginx

import (
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"strconv"
)

// L 包变量
var L accesslog.Logger

var vector *prometheus.CounterVec

func InitCounter(opt prometheus.CounterOpts) {
	vector = prometheus.NewCounterVec(opt, []string{"code"})
	prometheus.MustRegister(vector)
}

func Wrap(fn func(ctx *gin.Context) (Result, error)) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		res, err := fn(ctx)
		if err != nil {
			// 记录日志
			L.Error("处理业务逻辑出错",
				// http 地址
				accesslog.String("path", ctx.Request.URL.Path),
				// 命中路由
				accesslog.String("route", ctx.FullPath()),
				accesslog.Error(err))
		}
		vector.WithLabelValues(strconv.Itoa(res.Code)).Inc()
		ctx.JSON(http.StatusOK, res)
	}
}

// WrapBody 返回 gin.HandlerFunc
// 用于包装 requestBody，
// 统一处理日志打印
func WrapBody[T any](l accesslog.Logger, fn func(ctx *gin.Context, req T) (Result, error)) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var req T
		// Bind 方法会根据Content-Type 来解析 到结构体里面
		if err := ctx.Bind(&req); err != nil {
			return
		}
		// 下半段业务逻辑
		res, err := fn(ctx, req)
		if err != nil {
			// 打日志
			l.Error("处理业务逻辑出错",
				accesslog.String("path", ctx.Request.URL.Path),
				accesslog.String("rout", ctx.FullPath()),
				accesslog.Error(err))
		}
		vector.WithLabelValues(strconv.Itoa(res.Code)).Inc()
		ctx.JSONP(http.StatusOK, res)
	}
}

// WrapBodyV1 返回 gin.HandlerFunc
// 用于包装requestBody
// 统一处理日志打印
// 统一处理日志打印（logger 使用包变量）
func WrapBodyV1[T any](fn func(ctx *gin.Context, req T) (Result, error)) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var req T
		// Bind 方法会根据Content-Type 来解析 到结构体里面
		if err := ctx.Bind(&req); err != nil {
			return
		}
		// 下半段业务逻辑
		res, err := fn(ctx, req)
		if err != nil {
			// 打日志
			L.Error("处理业务逻辑出错",
				accesslog.String("path", ctx.Request.URL.Path),
				accesslog.String("rout", ctx.FullPath()),
				accesslog.Error(err))
		}
		vector.WithLabelValues(strconv.Itoa(res.Code)).Inc()

		ctx.JSONP(http.StatusOK, res)
	}
}

// WrapToken 返回 gin.HandlerFunc
// 统一处理日志打印
// 统一获取Token 解析的 Claims
func WrapToken[C jwt.Claims](fn func(ctx *gin.Context, uc C) (Result, error)) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// 确保前面一定有 ctx.Set("user", jwt.Claims) 写入

		val, ok := ctx.Get("user")
		if !ok {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		c, ok := val.(C)
		if !ok {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		// 下半段业务逻辑
		res, err := fn(ctx, c)
		if err != nil {
			// 打日志
			L.Error("处理业务逻辑出错",
				accesslog.String("path", ctx.Request.URL.Path),
				accesslog.String("rout", ctx.FullPath()),
				accesslog.Error(err))
		}
		vector.WithLabelValues(strconv.Itoa(res.Code)).Inc()
		ctx.JSONP(http.StatusOK, res)
	}
}

// WrapBodyAndToken 返回 gin.HandlerFunc
// 用于包装 requestBody
// 统一获取Token 解析的 Claims
// 统一处理日志打印
func WrapBodyAndToken[Req any, C jwt.Claims](fn func(ctx *gin.Context, req Req, uc C) (Result, error)) gin.HandlerFunc {
	return func(ctx *gin.Context) {

		var req Req
		// Bind 方法会根据Content-Type 来解析 到结构体里面
		if err := ctx.Bind(&req); err != nil {
			return
		}

		val, ok := ctx.Get("user")
		if !ok {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		c, ok := val.(C)
		if !ok {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		// 下半段业务逻辑
		// 业务逻辑有可能要操作 ctx
		res, err := fn(ctx, req, c)
		if err != nil {
			// 打日志
			L.Error("处理业务逻辑出错",
				accesslog.String("path", ctx.Request.URL.Path),
				accesslog.String("rout", ctx.FullPath()),
				accesslog.Error(err))
		}
		vector.WithLabelValues(strconv.Itoa(res.Code)).Inc()
		ctx.JSONP(http.StatusOK, res)
	}
}
