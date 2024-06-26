package connpool

import (
	"context"
	"database/sql"
	"errors"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/ecodeclub/ekit/syncx/atomicx"
	"gorm.io/gorm"
)

var errUnknownPool = errors.New("未知的双写模式")

const (
	PatternDstOnly  = "DST_ONLY"
	PatternSrcOnly  = "SRC_ONLY"
	PatternDstFirst = "DST_FIRST"
	PatternSrcFirst = "SRC_FIRST"
)

// DoubleWritePool 双写连接池
type DoubleWritePool struct {
	src     gorm.ConnPool
	dst     gorm.ConnPool
	pattern *atomicx.Value[string]
	l       accesslog.Logger
}

func NewDoubleWritePool(src gorm.ConnPool, dst gorm.ConnPool, pattern string, l accesslog.Logger) *DoubleWritePool {
	return &DoubleWritePool{
		src:     src,
		dst:     dst,
		pattern: atomicx.NewValueOf[string](pattern),
		l:       l,
	}
}

// ChangePattern 更新 pattern
func (d *DoubleWritePool) ChangePattern(pattern string) {
	d.pattern.Store(pattern)
}

// BeginTx 事务开启
func (d *DoubleWritePool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	// 事务开启
	// 这里的 pattern 避免有其他goroutine 修改的情况
	pattern := d.pattern.Load()
	switch pattern {
	case PatternSrcOnly:
		tx, err := d.src.(gorm.TxBeginner).BeginTx(ctx, opts)
		return &DoubleWritePoolTx{
			src:     tx,
			pattern: PatternSrcOnly,
			l:       d.l,
		}, err
	case PatternSrcFirst:
		return d.startTwoTx(d.src, d.src, pattern, ctx, opts)
	case PatternDstFirst:
		return d.startTwoTx(d.src, d.src, pattern, ctx, opts)
	case PatternDstOnly:
		tx, err := d.dst.(gorm.TxBeginner).BeginTx(ctx, opts)
		return &DoubleWritePoolTx{
			dst:     tx,
			pattern: PatternSrcOnly,
			l:       d.l,
		}, err
	default:
		return nil, errUnknownPool
	}
}

// startTwoTx 开启两个事务
// 这里的 pattern 通过传参的形式，避免在事务开启时，有其他goroutine 修改的情况
func (d *DoubleWritePool) startTwoTx(
	first gorm.ConnPool,
	second gorm.ConnPool,
	pattern string,
	ctx context.Context,
	opts *sql.TxOptions) (*DoubleWritePoolTx, error) {
	src, err := first.(gorm.TxBeginner).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	dst, err := second.(gorm.TxBeginner).BeginTx(ctx, opts)
	if err != nil {
		d.l.Error("开启 dst 事务失败，回滚 src 事务")
		// 容错：回滚 src 事务
		_ = src.Rollback()
	}
	return &DoubleWritePoolTx{src: src, dst: dst, pattern: pattern, l: d.l}, nil
}

// PrepareContext 上下文内容
func (d *DoubleWritePool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	// sql.Stmt 是一个结构体，这里无法返回一个代表双写的 Stmt
	panic("implement me")
}

func (d *DoubleWritePool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch d.pattern.Load() {
	case PatternSrcOnly:
		return d.src.ExecContext(ctx, query, args...)
	case PatternSrcFirst:
		res, err := d.src.ExecContext(ctx, query, args...)
		if err != nil {
			return res, err
		}
		_, err1 := d.dst.ExecContext(ctx, query, args...)
		if err1 != nil {
			d.l.Error("SRC_FIRST，写入DST 失败", accesslog.Error(err))
		}
		return res, err
	case PatternDstFirst:
		res, err := d.dst.ExecContext(ctx, query, args...)
		if err != nil {
			return res, err
		}
		_, err1 := d.src.ExecContext(ctx, query, args...)
		if err1 != nil {
			d.l.Error("DST_FIRST，写入SRC 失败", accesslog.Error(err))
		}
		return res, err
	case PatternDstOnly:
		return d.dst.ExecContext(ctx, query, args...)
	default:
		return nil, errUnknownPool
	}
}

func (d *DoubleWritePool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch d.pattern.Load() {
	case PatternSrcOnly, PatternSrcFirst:
		return d.src.QueryContext(ctx, query, args...)
	case PatternDstFirst, PatternDstOnly:
		return d.dst.QueryContext(ctx, query, args...)
	default:
		return nil, errUnknownPool
	}
}

func (d *DoubleWritePool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	switch d.pattern.Load() {
	case PatternSrcOnly, PatternSrcFirst:
		return d.src.QueryRowContext(ctx, query, args...)
	case PatternDstFirst, PatternDstOnly:
		return d.dst.QueryRowContext(ctx, query, args...)
	default:
		// 因为返回值里面没有 error，只能 panic 掉
		panic(errUnknownPool)
	}
}

// DoubleWritePoolTx 双写连接池事务
type DoubleWritePoolTx struct {
	src     *sql.Tx
	dst     *sql.Tx
	pattern string
	l       accesslog.Logger
}

func (d *DoubleWritePoolTx) Commit() error {
	switch d.pattern {
	case PatternSrcOnly:
		return d.src.Commit()
	case PatternSrcFirst:
		err := d.src.Commit()
		if err != nil {
			return err
		}
		if d.dst != nil {
			err1 := d.dst.Commit()
			if err1 != nil {
				// 记录日志
				d.l.Error("事务 SRC_FIRST，dst 提交失败", accesslog.Error(err1))
			}
		}
		return nil

	case PatternDstFirst:
		err := d.dst.Commit()
		if err != nil {
			return err
		}
		if d.src != nil {
			err1 := d.src.Commit()
			if err1 != nil {
				// 记日志
				d.l.Error("事务 DST_FIRST，SRC 提交失败", accesslog.Error(err1))
			}
		}
		return nil
	case PatternDstOnly:
		return d.dst.Commit()
	default:
		return errUnknownPool
	}
}

func (d *DoubleWritePoolTx) Rollback() error {
	switch d.pattern {
	case PatternSrcOnly:
		return d.src.Rollback()
	case PatternSrcFirst:
		err := d.src.Rollback()
		if err != nil {
			return err
		}
		if d.dst != nil {
			err1 := d.dst.Rollback()
			if err1 != nil {
				// 记日志
				d.l.Error("事务 SRC_FIRST，dst 回滚失败", accesslog.Error(err1))
			}
		}
		return nil
	case PatternDstFirst:
		err := d.dst.Rollback()
		if err != nil {
			return err
		}
		if d.src != nil {
			err1 := d.src.Rollback()
			if err1 != nil {
				// 记日志
				d.l.Error("事务 DST_FIRST，SRC 回滚失败", accesslog.Error(err1))
			}
		}
		return nil
	case PatternDstOnly:
		return d.dst.Rollback()
	default:
		return errUnknownPool
	}
}

func (d *DoubleWritePoolTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DoubleWritePoolTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch d.pattern {
	case PatternSrcOnly:
		return d.src.ExecContext(ctx, query, args...)
	case PatternSrcFirst:
		res, err := d.src.ExecContext(ctx, query, args...)
		if err != nil {
			return res, err
		}
		_, err1 := d.dst.ExecContext(ctx, query, args...)
		if err1 != nil {
			// 记日志
			// 容错：dst 写失败，不被认为是失败；等待后续的校验与修复程序
			d.l.Error("事务 SRC_FIRST，写入DST 失败", accesslog.Error(err))
		}
		return res, err
	case PatternDstFirst:
		res, err := d.dst.ExecContext(ctx, query, args...)
		if err != nil {
			return res, err
		}
		_, err1 := d.src.ExecContext(ctx, query, args...)
		if err1 != nil {
			// 记日志
			// dst 写失败，不被认为是失败
			d.l.Error("事务 DST_FIRST，写入SRC 失败", accesslog.Error(err))
		}
		return res, err
	case PatternDstOnly:
		return d.dst.ExecContext(ctx, query, args...)
	default:
		return nil, errUnknownPool
	}
}

func (d *DoubleWritePoolTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch d.pattern {
	case PatternSrcOnly, PatternSrcFirst:
		return d.src.QueryContext(ctx, query, args...)
	case PatternDstFirst, PatternDstOnly:
		return d.dst.QueryContext(ctx, query, args...)
	default:
		return nil, errUnknownPool
	}
}

func (d *DoubleWritePoolTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	switch d.pattern {
	case PatternSrcOnly, PatternSrcFirst:
		return d.src.QueryRowContext(ctx, query, args...)
	case PatternDstFirst, PatternDstOnly:
		return d.dst.QueryRowContext(ctx, query, args...)
	default:
		// 因为返回值里面没有 error，只能 panic 掉
		panic(errUnknownPool)
	}
}
