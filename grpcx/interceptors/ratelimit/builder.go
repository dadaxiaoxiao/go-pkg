package ratelimit

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InterceptorBuilder 限流拦截器
// key-"limiter:service:xxx" 整个应用、集群限流
// key-"limiter:client:xxx"
type InterceptorBuilder struct {
	limiter ratelimit.Limiter
	key     string
	l       accesslog.Logger
}

func NewInterceptorBuilder(limiter ratelimit.Limiter, key string, l accesslog.Logger) *InterceptorBuilder {
	return &InterceptorBuilder{
		limiter: limiter,
		key:     key,
		l:       l,
	}
}

func (i *InterceptorBuilder) BuildUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any,
		info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		limited, err := i.limiter.Limit(ctx, i.key)
		if err != nil {
			i.l.Error("判定限流出现问题", accesslog.Error(err))
			return nil, status.Errorf(codes.ResourceExhausted, "触发限流")
		}
		if limited {
			return nil, status.Errorf(codes.ResourceExhausted, "触发限流")
		}
		return handler(ctx, req)
	}
}

func (i *InterceptorBuilder) BuildUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		limited, err := i.limiter.Limit(ctx, i.key)
		if err != nil {
			i.l.Error("判定限流出现问题", accesslog.Error(err))
			return status.Errorf(codes.ResourceExhausted, "触发限流")
		}
		if limited {
			return status.Errorf(codes.ResourceExhausted, "触发限流")
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
