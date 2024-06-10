package metric

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"time"
)

type MiddlewareBuilder struct {
	Namespace  string
	Subsystem  string
	Name       string
	Help       string
	InstanceID string
}

func NewBuilder(Namespace string,
	Subsystem string,
	Name string,
	Help string,
	InstanceID string) *MiddlewareBuilder {
	return &MiddlewareBuilder{
		Namespace:  Namespace,
		Subsystem:  Subsystem,
		Name:       Name,
		Help:       Help,
		InstanceID: InstanceID,
	}
}

func (m *MiddlewareBuilder) Build() gin.HandlerFunc {
	labels := []string{"method", "pattern", "status"}
	vector := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: m.Namespace,
		Subsystem: m.Subsystem,
		Name:      m.Name + "_resp_time",
		Help:      m.Help,
		ConstLabels: map[string]string{
			"instance_id": m.InstanceID,
		},
		Objectives: map[float64]float64{
			0.5:   0.01,
			0.75:  0.01,
			0.90:  0.01,
			0.99:  0.001,
			0.999: 0.0001,
		},
	}, labels)
	prometheus.MustRegister(vector)

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: m.Namespace,
		Subsystem: m.Subsystem,
		Name:      m.Name + "_active_req",
		Help:      m.Help,
		ConstLabels: map[string]string{
			"instance_id": m.InstanceID,
		},
	})
	prometheus.MustRegister(gauge)

	return func(ctx *gin.Context) {
		method := ctx.Request.Method
		start := time.Now()
		gauge.Inc()
		defer func() {
			duration := time.Since(start)
			gauge.Desc()
			// 404????
			pattern := ctx.FullPath()
			if pattern == "" {
				pattern = "unknown"
			}
			vector.WithLabelValues(method, pattern,
				strconv.Itoa(ctx.Writer.Status())).Observe(float64(duration.Milliseconds()))
		}()
		// 最终就会执行到业务里面
		ctx.Next()
	}
}
