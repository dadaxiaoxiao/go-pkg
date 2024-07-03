package validator

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator"
	events2 "github.com/dadaxiaoxiao/go-pkg/gormx/migrator/events"
	"gorm.io/gorm"
	"time"
)

type CanalIncrValidator[T migrator.Entity] struct {
	baseValidator
}

func NewCanalIncrValidator[T migrator.Entity](
	base *gorm.DB,
	target *gorm.DB,
	direction string,
	l accesslog.Logger,
	producer events2.Producer,
) *CanalIncrValidator[T] {
	return &CanalIncrValidator[T]{
		baseValidator: baseValidator{
			base:      base,
			target:    target,
			direction: direction,
			l:         l,
			producer:  producer,
		},
	}
}

func (v *CanalIncrValidator[T]) Validate(ctx context.Context, id int64) error {
	var base T
	dbCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := v.base.WithContext(dbCtx).Where("id = ?", id).First(&base).Error
	switch err {
	case nil:
		// 找到了
		// 这种情况代表，base 新增或修改
		var target T
		err1 := v.target.WithContext(ctx).Where("id = ?", id).First(&target).Error
		switch err1 {
		case nil:
			if !base.CompareTo(target) {
				v.notify(id, events2.InconsistentEventTypeNEQ)
			}
		case gorm.ErrRecordNotFound:
			v.notify(id, events2.InconsistentEventTypeTargetMissing)
		default:
			return err
		}
	case gorm.ErrRecordNotFound:
		// 没找到
		// 这种情况，是base 删除操作
		var target T
		err1 := v.target.WithContext(ctx).Where("id = ?", id).First(&target).Error
		switch err1 {
		case nil:
			v.notify(id, events2.InconsistentEventTypeBaseMissing)
		case gorm.ErrRecordNotFound:
		default:
			return err
		}
	default:
		return err
	}
	return nil
}
