package saramax

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
)

// Handler 单个消费
// 实现 ConsumerGroupHandler
type Handler[T any] struct {
	l  accesslog.Logger
	fn func(msg *sarama.ConsumerMessage, t T) error
}

func NewHandler[T any](l accesslog.Logger,
	fn func(msg *sarama.ConsumerMessage, t T) error) *Handler[T] {
	return &Handler[T]{
		l:  l,
		fn: fn,
	}
}

func (h *Handler[T]) Setup(session sarama.ConsumerGroupSession) error {
	// 初始化
	return nil
}

func (h *Handler[T]) Cleanup(session sarama.ConsumerGroupSession) error {
	// 清理
	return nil
}

// ConsumeClaim 单个消费，单个提交
func (h *Handler[T]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgs := claim.Messages()
	// 循环获取channel 消息
	for msg := range msgs {
		var t T
		err := json.Unmarshal(msg.Value, &t)
		if err != nil {
			// 日志打印
			h.l.Error("反序列消息失败",
				accesslog.Error(err),
				accesslog.String("topic", msg.Topic),
				accesslog.Int32("partition", msg.Partition),
				accesslog.Int64("offset", msg.Offset))
			continue
		}

		//执行业务逻辑 (引入重试)
		for i := 0; i < 3; i++ {
			err = h.fn(msg, t)
			if err == nil {
				break
			}
			h.l.Error("处理消息失败",
				accesslog.Error(err),
				accesslog.String("topic", msg.Topic),
				accesslog.Int32("partition", msg.Partition),
				accesslog.Int64("offset", msg.Offset))
		}

		if err != nil {
			h.l.Error("处理消息失败-重试次数上限",
				accesslog.Error(err),
				accesslog.String("topic", msg.Topic),
				accesslog.Int32("partition", msg.Partition),
				accesslog.Int64("offset", msg.Offset))
		} else {
			// 标记消费
			session.MarkMessage(msg, "")
		}
	}
	return nil
}
