package fixer

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator/events"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OverrideFixer[T migrator.Entity] struct {
	base    *gorm.DB
	target  *gorm.DB
	columns []string
}

func NewOverrideFixer[T migrator.Entity](base *gorm.DB, target *gorm.DB) (*OverrideFixer[T], error) {
	// 查询数据库 T 对应的列
	var t T
	rows, err := target.Model(&t).Limit(1).Rows()
	if err != nil {
		return nil, err
	}
	// 得到所有列
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	return &OverrideFixer[T]{
		base:    base,
		target:  target,
		columns: columns,
	}, nil
}

// Fix 修复程序
func (f *OverrideFixer[T]) Fix(ctx context.Context, evt events.InconsistentEvent) error {
	var t T
	// 这里查询由 ctx 链路控制查询超时
	err := f.base.WithContext(ctx).Where("id =?", evt.ID).First(&t).Error
	switch err {
	case gorm.ErrRecordNotFound:
		// base 库已经删除这条条数据
		return f.target.
			Where("id=?", evt.ID).Delete(&t).Error
	case nil:
		// 避免并发冲突，这里是使用upset 语义
		// 使用OnConflict子句指定在唯一键冲突时的行为,希望更新特定的字段而不是引发错误
		return f.target.Clauses(
			clause.OnConflict{
				DoUpdates: clause.AssignmentColumns(f.columns),
			}).Create(&t).Error
	default:
		return err
	}
}
