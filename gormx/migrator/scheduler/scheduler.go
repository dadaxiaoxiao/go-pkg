package scheduler

import (
	"context"
	"fmt"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/ginx"
	"github.com/dadaxiaoxiao/go-pkg/gormx/connpool"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator/events"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator/validator"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"sync"
	"time"
)

type Scheduler[T migrator.Entity] struct {
	lock sync.Mutex
	// 源
	src *gorm.DB
	// 目标
	dst *gorm.DB
	// 双写连接池
	pool *connpool.DoubleWritePool
	l    accesslog.Logger
	// 模式
	pattern    string
	cancelFull func()
	cancelIncr func()
	// 消费者
	producer events.Producer
}

func NewScheduler[T migrator.Entity](
	src *gorm.DB,
	dst *gorm.DB,
	pool *connpool.DoubleWritePool,
	l accesslog.Logger,
	pattern string,
	producer events.Producer) *Scheduler[T] {
	return &Scheduler[T]{
		src:     src,
		dst:     dst,
		pool:    pool,
		l:       l,
		pattern: pattern,
		cancelFull: func() {
			// 初始
		},
		cancelIncr: func() {
			// 初始
		},
		producer: producer,
	}
}

// RegisterRoutes 注册路由
// 通过http 接口的方式暴露出去
// 不同的调度隶属不同的分组
func (s *Scheduler[T]) RegisterRoutes(server *gin.RouterGroup) {
	server.POST("/src_only", ginx.Wrap(s.SrcOnly))
	server.POST("/src_first", ginx.Wrap(s.SrcFirst))
	server.POST("/dst_first", ginx.Wrap(s.DstFirst))
	server.POST("/dst_only", ginx.Wrap(s.DstOnly))
	server.POST("/full/start", ginx.Wrap(s.StartFullValidator))
	server.POST("/full/stop", ginx.Wrap(s.StopFullValidator))
	server.POST("/incr/start", ginx.WrapBodyV1[StartIncrRequest](s.StartIncrementValidator))
	server.POST("/incr/stop", ginx.Wrap(s.StopIncrementValidator))
}

// SrcOnly 只读写源表
func (s *Scheduler[T]) SrcOnly(ctx *gin.Context) (ginx.Result, error) {
	// 这里防止并发操作
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternSrcOnly
	// 更改双写connpool 的pattern
	s.pool.ChangePattern(connpool.PatternSrcOnly)
	return ginx.Result{
		Msg: "OK",
	}, nil
}

// SrcFirst 双写，读源表
func (s *Scheduler[T]) SrcFirst(ctx *gin.Context) (ginx.Result, error) {
	// 这里防止并发操作
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternSrcFirst
	// 更改双写connpool 的pattern
	s.pool.ChangePattern(connpool.PatternSrcFirst)
	return ginx.Result{
		Msg: "OK",
	}, nil
}

// DstFirst 双写，读目标表
func (s *Scheduler[T]) DstFirst(ctx *gin.Context) (ginx.Result, error) {
	// 这里防止并发操作
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternDstFirst
	// 更改双写connpool 的pattern
	s.pool.ChangePattern(connpool.PatternDstFirst)
	return ginx.Result{
		Msg: "OK",
	}, nil
}

// DstOnly 只读目标表
func (s *Scheduler[T]) DstOnly(ctx *gin.Context) (ginx.Result, error) {
	// 这里防止并发操作
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternDstOnly
	// 更改双写connpool 的pattern
	s.pool.ChangePattern(connpool.PatternDstOnly)
	return ginx.Result{
		Msg: "OK",
	}, nil
}

// StartFullValidator 开启全量校验
func (s *Scheduler[T]) StartFullValidator(ctx *gin.Context) (ginx.Result, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// 重启：取消上一次+ 新启动

	cancel := s.cancelFull
	// 创建新校验实例
	v, err := s.newValidator()
	if err != nil {
		return ginx.Result{Msg: "系统异常"}, err
	}

	// 新建链路控制
	var vCtx context.Context
	vCtx, cancelFull := context.WithCancel(context.Background())
	s.cancelFull = cancelFull

	go func() {
		// 先取消上一次的校验
		cancel()
		// 启动校验
		err := v.Validate(vCtx)
		if err != nil {
			s.l.Warn("退出全量校验", accesslog.Error(err))
		}
	}()

	return ginx.Result{
		Msg: "启动全量校验成功",
	}, nil
}

// StopFullValidator 关闭全量校验
func (s *Scheduler[T]) StopFullValidator(ctx *gin.Context) (ginx.Result, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cancelFull()
	return ginx.Result{
		Msg: "停止全量校验成功",
	}, nil
}

// newValidator 新建校验实例
func (s *Scheduler[T]) newValidator() (*validator.Validator[T], error) {
	switch s.pattern {
	case connpool.PatternSrcOnly, connpool.PatternSrcFirst:
		return validator.NewValidator[T](s.src, s.dst, s.l, s.producer, "SRC", 100)
	case connpool.PatternDstFirst, connpool.PatternDstOnly:
		return validator.NewValidator[T](s.dst, s.src, s.l, s.producer, "DST", 100)
	default:
		return nil, fmt.Errorf("未知的 pattern %s", s.pattern)
	}
}

// StartIncrementValidator 启动增量校验
func (s *Scheduler[T]) StartIncrementValidator(ctx *gin.Context, req StartIncrRequest) (ginx.Result, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 重启：取消上一次+ 新启动
	cancel := s.cancelIncr
	// 创建新校验实例
	v, err := s.newValidator()
	if err != nil {
		return ginx.Result{Msg: "系统异常"}, err
	}

	v.Incr().Utime(req.Utime).
		SleepInterval(time.Duration(req.Interval) * time.Millisecond)

	// 新建链路控制
	var vCtx context.Context
	vCtx, cancelIncr := context.WithCancel(context.Background())
	s.cancelIncr = cancelIncr

	go func() {
		// 先取消上一次的校验
		cancel()
		// 启动校验
		err := v.Validate(vCtx)
		if err != nil {
			s.l.Warn("退出增量校验", accesslog.Error(err))
		}
	}()
	return ginx.Result{
		Msg: "启动增量校验成功",
	}, nil
}

// StopIncrementValidator 关闭增量校验
func (s *Scheduler[T]) StopIncrementValidator(ctx *gin.Context) (ginx.Result, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cancelIncr()
	return ginx.Result{
		Msg: "停止增量校验成功",
	}, nil
}

type StartIncrRequest struct {
	// 更新时间，用于增量校验
	Utime int64 `json:"utime"`
	// 毫秒数
	Interval int64 `json:"interval"`
}
