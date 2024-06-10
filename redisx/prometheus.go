package redisx

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"net"
	"strconv"
	"time"
)

// PrometheusHook 实现redis hook ，提供执行命令的可观测性
type PrometheusHook struct {
	vector *prometheus.SummaryVec
}

func NewPrometheusHook(
	namespace string,
	subsystem string,
	instanceId string,
	name string,
) *PrometheusHook {
	vector := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      name,
		ConstLabels: map[string]string{
			"instance_id": instanceId,
		},
	}, []string{"cmd", "key_exist"})
	return &PrometheusHook{
		vector: vector,
	}
}

// DialHook redis 连接执行
func (p PrometheusHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook redis 命令执行之前
func (p PrometheusHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		// 在Redis执行之前
		startTime := time.Now()
		var err error
		defer func() {
			duration := time.Since(startTime).Milliseconds()
			keyExist := err == redis.Nil
			p.vector.WithLabelValues(
				cmd.Name(),

				strconv.FormatBool(keyExist),
			).Observe(float64(duration))
		}()
		err = next(ctx, cmd)
		return err
	}
}

func (p PrometheusHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}
