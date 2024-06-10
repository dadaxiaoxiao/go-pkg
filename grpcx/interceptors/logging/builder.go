package logging

import (
	"context"
	"fmt"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/grpcx/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"runtime"
	"time"
)

// InterceptorBuilder 日志
type InterceptorBuilder struct {
	interceptors.Builder
	l accesslog.Logger
}

func NewInterceptorBuilder(l accesslog.Logger) *InterceptorBuilder {
	return &InterceptorBuilder{
		l: l,
	}
}

func (i *InterceptorBuilder) BuildServer() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		var event = "normal"
		defer func() {
			duration := time.Since(start)
			// 为了防止 panicking
			if rec := recover(); rec != nil {
				// 这里是捕获 panicking 信息
				switch recType := rec.(type) {
				case error:
					err = recType
				default:
					err = fmt.Errorf("%v", rec)
				}
				stack := make([]byte, 4096)
				stack = stack[:runtime.Stack(stack, true)]
				event = "recover"
				err = status.New(codes.Internal, "panic, err "+err.Error()).Err()
			}

			fields := []accesslog.Field{
				accesslog.String("type", "unary"),
				accesslog.Int64("cost", duration.Milliseconds()),
				accesslog.String("method", info.FullMethod),
				accesslog.String("event", event),

				// 这里要记录对于的服务节点
				accesslog.String("peer", i.PeerName(ctx)),
				accesslog.String("peer_ip", i.PeerIp(ctx)),
			}

			if err != nil {
				st, _ := status.FromError(err)
				fields = append(fields,
					accesslog.String("code", st.Code().String()),
					accesslog.String("code_msg", st.Message()))
			}

			i.l.Info("RPC 请求", fields...)

		}()
		resp, err = handler(ctx, req)
		return resp, err
	}
}
