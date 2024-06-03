package ratelimit

import (
	"context"
	"sync"
	"time"
)

type TokenBucketLimiter struct {
	// 桶
	buckets chan struct{}
	// 时间间隔
	interval time.Duration
	// 关闭通道，使用 通道共享关闭信息
	closeCh chan struct{}
	// 用来判断 关闭closeCh的标记位
	closeOnce sync.Once
	ticker    *time.Ticker
}

// NewTokenBucketLimiter 新建令牌桶
func NewTokenBucketLimiter(interval time.Duration, capacity int) *TokenBucketLimiter {
	ticker := time.NewTicker(interval)
	res := &TokenBucketLimiter{
		interval: interval,
		buckets:  make(chan struct{}, capacity),
	}
	defer func() {
		go func() {
			for {
				select {
				case <-res.closeCh:
					// 结束发令牌
					return
				case <-ticker.C:
					// 这里间隔时间发送令牌
					select {
					case res.buckets <- struct{}{}:
					default:

					}
				}
			}
		}()
	}()
	return res
}

func (t *TokenBucketLimiter) Limit(ctx context.Context, key string) (bool, error) {
	select {
	case <-t.buckets:
		return true, nil
	case <-ctx.Done():
		// 超时
		return false, ctx.Err()
	}
}

func (l *TokenBucketLimiter) Close() error {
	l.closeOnce.Do(func() {
		close(l.closeCh)
	})
	return nil
}
