package jobx

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"strconv"
	"time"
)

// CronJobBuilder 封装cron.job
type CronJobBuilder struct {
	l      accesslog.Logger
	vector *prometheus.SummaryVec
	tracer trace.Tracer
}

func (c *CronJobBuilder) Build(job Job) cron.Job {
	name := job.Name()
	return cronJobFuncAdapter(func() error {
		// tracer 追踪
		_, span := c.tracer.Start(context.Background(), name)
		defer span.End()
		start := time.Now()
		c.l.Info("任务开始",
			accesslog.String("job", name))
		var success bool
		defer func() {
			c.l.Info("任务结束",
				accesslog.String("job", name))

			duration := time.Since(start).Milliseconds()
			c.vector.WithLabelValues(name,
				strconv.FormatBool(success)).Observe(float64(duration))
		}()

		err := job.Run()
		success = err == nil
		if err != nil {
			// tracer 记录错误
			span.RecordError(err)
			// 日志打印
			c.l.Error("执行任务失败",
				accesslog.Error(err), accesslog.String("job", name))
		}
		return nil
	})
}

func NewCronJobBuilder(tracer string, l accesslog.Logger, opts prometheus.SummaryOpts) *CronJobBuilder {
	vector := prometheus.NewSummaryVec(opts, []string{"name", "success"})
	prometheus.MustRegister(vector)
	return &CronJobBuilder{
		l:      l,
		vector: vector,
		tracer: otel.GetTracerProvider().Tracer(tracer),
	}
}

// cronJobFuncAdapter 利用适配器 适配 cron.Job
type cronJobFuncAdapter func() error

func (c cronJobFuncAdapter) Run() {
	_ = c()
}
