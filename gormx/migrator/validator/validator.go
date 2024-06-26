package validator

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/gormx/events"
	"github.com/dadaxiaoxiao/go-pkg/gormx/migrator"
	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/syncx/atomicx"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"

	"time"
)

type Validator[T migrator.Entity] struct {
	baseValidator
	batchSize int

	highLoad *atomicx.Value[bool]

	utime int64

	// 睡眠间隔
	// <=0 直接退出校验循环
	// > 0 sleep 间隔
	sleepInterval time.Duration

	fromBase func(ctx context.Context, offset int) (T, error)
}

func NewValidator[T migrator.Entity](
	base *gorm.DB,
	target *gorm.DB,
	l accesslog.Logger,
	producer events.Producer,
	direction string,
	batchSize int) (*Validator[T], error) {

	highLoad := atomicx.NewValueOf[bool](false)
	res := &Validator[T]{
		baseValidator: baseValidator{
			base:      base,
			target:    target,
			direction: direction,
			l:         l,
			producer:  producer,
		},
		batchSize:     batchSize,
		highLoad:      highLoad,
		sleepInterval: 0,
	}
	// 默认全量数据校验
	res.fromBase = res.fullFromBase
	return res, nil
}

func (v *Validator[T]) Utime(utime int64) *Validator[T] {
	// 这里不考虑原子操作，对并发要求不高
	v.utime = utime
	return v
}

func (v *Validator[T]) SleepInterval(i time.Duration) *Validator[T] {
	v.sleepInterval = i
	return v
}

// Incr 切换为增量数据
func (v *Validator[T]) Incr() *Validator[T] {
	v.fromBase = v.incrFromBase
	return v
}

func (v *Validator[T]) HighLoad(flag bool) *Validator[T] {
	v.highLoad.Store(flag)
	return v
}

func (v *Validator[T]) Validate(ctx context.Context) error {
	var eg errgroup.Group
	eg.Go(func() error {
		v.validateBaseToTarget(ctx)
		return nil
	})

	eg.Go(func() error {
		v.validateTargetToBase(ctx)
		return nil
	})

	return eg.Wait()

}

// fullFromBase 全量数据库
func (v *Validator[T]) fullFromBase(ctx context.Context, offset int) (T, error) {
	// 数据库查询超时控制
	dbCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var src T
	// 找到了 base 中的数据
	// 例如 .Order("id DESC")，每次插入数据，就会导致你的 offset 不准了。所以使用 .Order("id")
	// 如果 表没有 id 这个列怎么办？ 找一个类似的列，比如说 ctime (创建时间）
	err := v.base.WithContext(dbCtx).Offset(offset).
		Order("id").First(&src).Error
	return src, err
}

func (v *Validator[T]) incrFromBase(ctx context.Context, offset int) (T, error) {
	// 数据库查询超时控制
	dbCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var src T
	err := v.base.WithContext(dbCtx).
		// 这里如果不建议取等号，会造成  offset不准
		Where("utime > ?", v.utime).
		Offset(offset).
		Order("utime ASC,id ASC").First(&src).Error
	return src, err

}

// validateBaseToTarget 校验base库到target库
// 校验Base中有数据，但target 无数据 or 不一致
// 这里是避免 base 存在新增数据或者变更数据
func (v *Validator[T]) validateBaseToTarget(ctx context.Context) {
	offset := 0
	for {
		if v.highLoad.Load() {
			// 等待挂起
			continue
		}
		// 查询数据库
		src, err := v.fromBase(ctx, offset)

		switch err {
		case context.Canceled, context.DeadlineExceeded:
			// 被取消或超时
			return
		case gorm.ErrRecordNotFound:
			// 没有数据，全量校验结束
			if v.sleepInterval <= 0 {
				return
			}
			time.Sleep(v.sleepInterval)
			continue
		case nil:
			// 查到了数据
			// target 库查找对应的数据
			var dst T
			err = v.target.WithContext(ctx).Where("id =?", src.ID()).First(&dst).Error
			switch err {
			case context.Canceled, context.DeadlineExceeded:
				// 超时或者被人取消了
				return
			case gorm.ErrRecordNotFound:
				// target 里面少了这条数据
				// 上报给 Kafka
				v.notify(src.ID(), events.InconsistentEventTypeTargetMissing)
			case nil:
				if !src.CompareTo(dst) {
					// 不相等
					// 上报 kafka ，告知数据不一致
					v.notify(src.ID(), events.InconsistentEventTypeNEQ)
				}
			default:
				/*
					两种做法
					1.假设大概率数据是一致，日志记录 下一条
					2.保险做法
						2.1 如果真的不一致，进行数据修复
						2.2 如果假的不一致，多进行修复异常
						不方便使用具体  InconsistentEventType
				*/
				v.l.Error("validateBaseToTarget: base > target 查询 target 失败", accesslog.Error(err))
				continue
			}
		default:
			// 数据库错误
			v.l.Error("validateBaseToTarget: base > target 查询 base 失败", accesslog.Error(err))
			// 等待重新查询
			continue
		}
		offset++
	}
}

// validateTargetToBase 校验target库到base库
// 校验target中有数据，base 无数据 or 不一致
// 这里是避免 base 存在了物理删除造成的 target 比 base 多数据的情况
func (v *Validator[T]) validateTargetToBase(ctx context.Context) {
	// 先找target ,再找base ,从base 找出已经被物理删除的
	offset := 0
	for {
		if v.highLoad.Load() {
			// 等待挂起
			continue
		}

		// 数据库查询超时控制
		dbCtx, cancel := context.WithTimeout(ctx, time.Second)

		var dstTs []T
		err := v.target.WithContext(dbCtx).
			Where("utime > ?", v.utime).
			Select("id").
			Offset(offset).Limit(v.batchSize).
			Order("utime").Find(&dstTs).Error
		cancel()

		switch err {
		case context.Canceled, context.DeadlineExceeded:
			// 超时或者被取消
			return
		case gorm.ErrRecordNotFound:
			// 正常情况，gorm 在 Find 方法接收的是切片的时候，不会返回 gorm.ErrRecordNotFound
			return
		case nil:
			// 获取ids
			ids := slice.Map[T, int64](dstTs, func(idx int, t T) int64 {
				return t.ID()
			})

			// 查找 base 数据，进行比对，哪些已经物理删除
			var srcTs []T
			err = v.base.WithContext(ctx).Where("id IN ?", ids).Find(&srcTs).Error
			switch err {
			case context.Canceled, context.DeadlineExceeded:
				// 超时或者被取消
				return
			case nil:
				//target 数据可能部分不存在于base数据库
				srcIds := slice.Map[T, int64](srcTs, func(idx int, t T) int64 {
					return t.ID()
				})
				// 计算差集
				// 也就是，src 里面的没有的
				diff := slice.DiffSet(ids, srcIds)
				v.notifyBaseMissing(diff)
				// 把diff 上报kafka
			case gorm.ErrRecordNotFound:
				// target 数据全不存在于base数据库， base库已经物理删除，上报kafa
				v.notifyBaseMissing(ids)
			default:
				v.l.Error("validateTargetToBase: base > target 查询 target 失败", accesslog.Error(err))
			}
		default:
			v.l.Error("validateTargetToBase: target > base 查询 target 失败", accesslog.Error(err))

		}
		if len(dstTs) == 0 {
			// 没数据
			if v.sleepInterval <= 0 {
				return
			}
			time.Sleep(v.sleepInterval)
			continue
		}

		// 添加偏移量
		offset += len(dstTs)

		if len(dstTs) < v.batchSize {
			// 不够一个批次
			if v.sleepInterval <= 0 {
				return
			}
			time.Sleep(v.sleepInterval)
		}
	}
}

// notifyBaseMissing 通知 base 数据库数据缺失
func (v *Validator[T]) notifyBaseMissing(ids []int64) {
	for _, id := range ids {
		v.notify(id, events.InconsistentEventTypeBaseMissing)
	}
}
