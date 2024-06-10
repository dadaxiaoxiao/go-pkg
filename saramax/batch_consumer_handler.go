package saramax

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"time"
)

type BatchHandlerOption[T any] func(b *BatchHandler[T])

// BatchHandler 批量消费
// 实现 ConsumerGroupHandler
type BatchHandler[T any] struct {
	l         accesslog.Logger
	fn        func(msg []*sarama.ConsumerMessage, t []T) error
	batchSize int
	duration  time.Duration
}

func NewBatchHandler[T any](l accesslog.Logger,
	fn func(msg []*sarama.ConsumerMessage, t []T) error, opts ...BatchHandlerOption[T]) *BatchHandler[T] {
	res := &BatchHandler[T]{
		l:  l,
		fn: fn,
	}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

func WithBatchSize[T any](batchSize int) BatchHandlerOption[T] {
	return func(b *BatchHandler[T]) {
		b.batchSize = batchSize
	}
}

func WithDuration[T any](duration time.Duration) BatchHandlerOption[T] {
	return func(b *BatchHandler[T]) {
		b.duration = duration
	}
}

func (b *BatchHandler[T]) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (b *BatchHandler[T]) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (b *BatchHandler[T]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCh := claim.Messages()
	batchSize := b.batchSize
	for {
		ctx, cancel := context.WithTimeout(context.Background(), b.duration)
		done := false
		msgs := make([]*sarama.ConsumerMessage, 0, batchSize)
		ts := make([]T, 0, batchSize)
		for i := 0; i < batchSize && !done; i++ {
			select {
			case <-ctx.Done():
				// 这一批次已经超时了, 不再尝试凑够一批了
				// 或者整个consumer 被关闭
				done = true
			case msg, ok := <-msgCh:
				// 从切片读取消息
				if !ok {
					// 代表消费者被中断关闭了
					cancel()
					return nil
				}

				var t T
				err := json.Unmarshal(msg.Value, &t)
				if err != nil {
					// 日志打印
					b.l.Error("反序列消息失败",
						accesslog.Error(err),
						accesslog.String("topic", msg.Topic),
						accesslog.Int32("partition", msg.Partition),
						accesslog.Int64("offset", msg.Offset))
					continue
				}
				msgs = append(msgs, msg)
				ts = append(ts, t)
			}
		}
		cancel()
		if len(msgs) == 0 {
			continue
		}

		err := b.fn(msgs, ts)
		if err != nil {
			b.l.Error("调用业务批量接口失败",
				accesslog.Error(err))
		}

		// 统一提交
		for _, msg := range msgs {
			session.MarkMessage(msg, "")
		}
	}
}
