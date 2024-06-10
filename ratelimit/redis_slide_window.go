package ratelimit

import (
	"context"
	_ "embed"
	"github.com/redis/go-redis/v9"
	"time"
)

//go:embed slide_window.lua
var luaSlideWindow string

type RedisSlideWindowLimiterOption func(r *RedisSlideWindowLimiter)

type RedisSlideWindowLimiter struct {
	cmd redis.Cmdable
	// 窗口大小
	interval time.Duration
	// 阈值
	rate int
}

// NewRedisSlideWindowLimiter
// 新增redis 滑动窗口
func NewRedisSlideWindowLimiter(cmd redis.Cmdable, opts ...RedisSlideWindowLimiterOption) Limiter {
	res := &RedisSlideWindowLimiter{
		cmd: cmd,
	}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

func WithInterval(interval time.Duration) RedisSlideWindowLimiterOption {
	return func(r *RedisSlideWindowLimiter) {
		r.interval = interval
	}
}

func WithRate(rate int) RedisSlideWindowLimiterOption {
	return func(r *RedisSlideWindowLimiter) {
		r.rate = rate
	}
}

func (r *RedisSlideWindowLimiter) Limit(ctx context.Context, key string) (bool, error) {
	return r.cmd.Eval(ctx, luaSlideWindow, []string{key},
		r.interval.Milliseconds(), r.rate, time.Now().UnixMilli()).Bool()
}
