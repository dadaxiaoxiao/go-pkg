package ratelimit

import "context"

type Limiter interface {
	//Limit 限流 ，key 限流对象
	// bool 代表是否限流，true 就是要限流
	// err 限流器本身是否有错误
	Limit(ctx context.Context, key string) (bool, error)
}
