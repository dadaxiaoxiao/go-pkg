package validator

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	events2 "github.com/dadaxiaoxiao/go-pkg/gormx/migrator/events"
	"gorm.io/gorm"
	"time"
)

type baseValidator struct {
	base   *gorm.DB
	target *gorm.DB

	// 取值为 SRC 以源表为准; 取值为 DST，以目标表为准
	direction string

	l        accesslog.Logger
	producer events2.Producer
}

// notify 通知kafka
// 上报不一致的数据
func (v *baseValidator) notify(id int64, typ string) {
	// 这里我们要单独控制超时时间
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	evt := events2.InconsistentEvent{
		Direction: v.direction,
		ID:        id,
		Type:      typ,
	}

	err := v.producer.ProduceInconsistentEvent(ctx, evt)
	if err != nil {
		v.l.Error("发送数据不一致的消息失败", accesslog.Error(err),
			accesslog.Field{Key: "event", Value: evt})
	}
}
