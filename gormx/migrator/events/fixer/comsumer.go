package fixer

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator/events"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator/fixer"
	"github.com/dadaxiaoxiao/go-pkg/saramax"
	"gorm.io/gorm"
	"time"
)

type Consumer[T migrator.Entity] struct {
	client   sarama.Client
	l        accesslog.Logger
	srcFirst *fixer.OverrideFixer[T]
	dstFirst *fixer.OverrideFixer[T]
	topic    string
}

func NewConsumer[T migrator.Entity](
	client sarama.Client,
	l accesslog.Logger,
	topic string,
	src *gorm.DB,
	dst *gorm.DB) (*Consumer[T], error) {

	srcFirst, err := fixer.NewOverrideFixer[T](src, dst)
	if err != nil {
		return nil, err
	}

	dstFirst, err := fixer.NewOverrideFixer[T](dst, src)
	if err != nil {
		return nil, err
	}

	return &Consumer[T]{
		client:   client,
		l:        l,
		topic:    topic,
		srcFirst: srcFirst,
		dstFirst: dstFirst,
	}, nil
}

func (c *Consumer[T]) Start() error {
	cg, err := sarama.NewConsumerGroupFromClient("migrator-fixer", c.client)
	if err != nil {
		return err
	}
	//消费
	go func() {
		er := cg.Consume(context.Background(),
			[]string{c.topic},
			saramax.NewHandler[events.InconsistentEvent](c.l, c.Consume))
		if er != nil {
			c.l.Error("退出了消费循环异常", accesslog.Error(err))
		}
	}()
	return err
}

func (c *Consumer[T]) Consume(msg *sarama.ConsumerMessage, t events.InconsistentEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	switch t.Direction {
	case "SRC":
		return c.srcFirst.Fix(ctx, t)
	case "DST":
		return c.dstFirst.Fix(ctx, t)
	}
	return errors.New("未知的校验方向")
}
