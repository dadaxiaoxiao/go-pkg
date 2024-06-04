package logger

import (
	"bytes"
	"context"
	"github.com/gin-gonic/gin"
	"go.uber.org/atomic"
	"io"
	"time"
)

// AccessLog 访问日志
type AccessLog struct {
	// Http 请问方法
	Method string `json:"method"`
	// 请求url
	Url string `json:"url"`
	// 请求体
	ReqBody string `json:"req_body"`
	// 响应体
	RespBody string `json:"resp_body"`
	// 响应时间
	Duration string `json:"duration"`
	// 状态码
	Status int `json:"status"`
}

// Builder 注意点：
// 1. 防止日志内容过多。URL 可能很长，请求体，响应体都可能很大，要考虑是不是完全输出到日志里面
// 2. 考虑 1 的问题，以及用户可能换用不同的日志框架，所以要有足够的灵活性
// 3. 考虑动态开关，结合监听配置文件，要小心并发安全
type Builder struct {
	allowReqBody  *atomic.Bool
	allowRespBody *atomic.Bool
	// 自己确认日志级别
	loggerFunc func(ctx context.Context, al *AccessLog)
	maxLength  *atomic.Int64
}

func NewBuilder(fn func(ctx context.Context, al *AccessLog)) *Builder {
	return &Builder{
		allowReqBody:  atomic.NewBool(false),
		allowRespBody: atomic.NewBool(false),
		loggerFunc:    fn,
		maxLength:     atomic.NewInt64(1024),
	}
}

// AllowReqBody 是否打印请求体
func (b *Builder) AllowReqBody() *Builder {
	b.allowReqBody.Store(true)
	return b
}

// AllowRespBody 是否打印响应体
func (b *Builder) AllowRespBody() *Builder {
	b.allowRespBody.Store(true)
	return b
}

// MaxLength 打印的最大长度
func (b *Builder) MaxLength(maxLength int64) *Builder {
	b.maxLength.Store(maxLength)
	return b
}

func (b *Builder) Builder() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var (
			//请求处理开始时间
			start         = time.Now()
			url           = ctx.Request.URL.String()
			urlLen        = int64(len(url))
			maxLength     = b.maxLength.Load()
			allowReqBody  = b.allowReqBody.Load()
			allowRespBody = b.allowRespBody.Load()
		)
		if urlLen > maxLength {
			url = url[:maxLength]
		}
		accessLog := &AccessLog{
			Method: ctx.Request.Method,
			Url:    url,
		}
		if allowReqBody && ctx.Request.Body != nil {
			body, _ := ctx.GetRawData()
			// Request.Body 是 io.ReadCloser，steam(流)对象，所以只能读取一次，因此读取后，要放回去
			ctx.Request.Body = io.NopCloser(bytes.NewReader(body))

			if int64(len(body)) >= maxLength {
				body = body[:maxLength]
			}
			//注意资源的消耗,因为会引起复制
			accessLog.ReqBody = string(body)
		}

		if allowRespBody {
			ctx.Writer = responseWriter{
				ResponseWriter: ctx.Writer,
				al:             accessLog,
				maxLength:      maxLength,
			}
		}

		defer func() {
			accessLog.Duration = time.Since(start).String()
			//日志打印
			b.loggerFunc(ctx, accessLog)
		}()

		// 执行到业务逻辑
		ctx.Next()
	}
}

type responseWriter struct {
	al *AccessLog
	gin.ResponseWriter
	maxLength int64
}

func (r responseWriter) WriteHeader(statusCode int) {
	r.al.Status = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r responseWriter) Write(data []byte) (int, error) {
	curLen := int64(len(data))
	respData := data
	if curLen > r.maxLength {
		respData = data[:r.maxLength]
	}
	r.al.RespBody = string(respData)
	return r.ResponseWriter.Write(data)
}
