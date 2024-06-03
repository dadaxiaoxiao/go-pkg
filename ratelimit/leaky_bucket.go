package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

// LeakyBucket 漏桶
type LeakyBucket struct {
	interval time.Duration
	// 关闭通道，使用 通道共享关闭信息
	closeCh chan struct{}
	// 用来判断 关闭closeCh的标记位
	closeOnce sync.Once
	ticker    *time.Ticker
}

// NewLeakyBucketLimiter 新建漏桶
func NewLeakyBucketLimiter(interval time.Duration) *LeakyBucket {
	ticker := time.NewTicker(interval)
	return &LeakyBucket{
		interval: interval,
		ticker:   ticker,
	}
}

func (l *LeakyBucket) Limit(ctx context.Context, key string) (bool, error) {
	select {
	case <-l.ticker.C:
		// 这里代表灯标获取到令牌
		return true, nil
	case <-ctx.Done():
		return false, errors.New("超时")
	case <-l.closeCh:
		// 主动取消
		return false, errors.New("限流器被关了")
	}
}

func (l *LeakyBucket) Close() error {
	l.closeOnce.Do(func() {
		close(l.closeCh)
	})
	return nil
}
