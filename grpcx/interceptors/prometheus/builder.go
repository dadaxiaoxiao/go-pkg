package prometheus

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/grpcx/interceptors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"strings"
	"time"
)

type InterceptorBuilder struct {
	interceptors.Builder
	Namespace string
	Subsystem string
}

func NewInterceptorBuilder(Namespace string, Subsystem string) *InterceptorBuilder {
	return &InterceptorBuilder{
		Namespace: Namespace,
		Subsystem: Subsystem,
	}
}

func (i *InterceptorBuilder) BuildServer() grpc.UnaryServerInterceptor {
	summary := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: i.Namespace,
		Subsystem: i.Subsystem,
		Name:      "server_handle_seconds",
		Objectives: map[float64]float64{
			0.5:   0.01,
			0.9:   0.01,
			0.95:  0.01,
			0.99:  0.001,
			0.999: 0.0001,
		},
	}, []string{
		"type", "service", "method", "peer", "code",
	})
	prometheus.MustRegister(summary)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		defer func() {
			// 处理完 handler 执行,最终上报prometheus
			s, m := i.splitMethodName(info.FullMethod)
			duration := float64(time.Since(start).Milliseconds())
			if err != nil {
				summary.WithLabelValues("unary", s, m, i.PeerName(ctx), "OK").Observe(duration)
			} else {
				st, _ := status.FromError(err)
				summary.WithLabelValues("unary", s, m, i.PeerName(ctx), st.Code().String()).Observe(duration)
			}
		}()
		resp, err = handler(ctx, req)
		return
	}
}

func (i *InterceptorBuilder) splitMethodName(fullMethodName string) (service string, method string) {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/")
	if i := strings.Index(fullMethodName, "/"); i >= 0 {
		return fullMethodName[:i], fullMethodName[i+1:]
	}
	return "unknown", "unknown"
}
