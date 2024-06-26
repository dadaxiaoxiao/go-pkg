package connpool

import (
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
)

type DoubleWriteTestSuite struct {
	suite.Suite
	db *gorm.DB
}

// SetupSuite 设置配置
func (d *DoubleWriteTestSuite) SetupSuite() {
	t := d.T()
	src, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/demo"))
	require.NoError(t, err)
	err = src.AutoMigrate(&Interactive{})
	require.NoError(t, err)
	dst, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/demo_intr"))
	require.NoError(t, err)
	err = dst.AutoMigrate(&Interactive{})
	require.NoError(t, err)

	// 实现一个由自定义connpool open 的db
	dobleWriteDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: NewDoubleWritePool(src.ConnPool, dst.ConnPool,
			PatternSrcFirst, accesslog.NewNopLogger()),
	}))
	require.NoError(t, err)
	d.db = dobleWriteDB
}

func (d *DoubleWriteTestSuite) TearDownTest() {
	d.db.Exec("TRUNCATE TABLE interactives")
}

func (d *DoubleWriteTestSuite) TestDoubleWrite() {
	t := d.T()
	err := d.db.Create(&Interactive{
		Biz:   "test",
		BizId: 10086,
	}).Error
	assert.NoError(t, err)
}

func (d *DoubleWriteTestSuite) TestDoubleWriteTransaction() {
	t := d.T()
	err := d.db.Transaction(func(tx *gorm.DB) error {
		err := tx.Create(&Interactive{
			Biz:   "test",
			BizId: 10086,
		}).Error
		return err
	})
	assert.NoError(t, err)
}

func TestDoubleWrite(t *testing.T) {
	suite.Run(t, &DoubleWriteTestSuite{})
}

type Interactive struct {
	Id    int64 `gorm:"primaryKey,autoIncrement"`
	BizId int64 `gorm:"uniqueIndex:biz_id_type"`
	// 默认是 BLOB/TEXT 类型
	Biz        string `gorm:"uniqueIndex:biz_id_type;type:varchar(128)"`
	ReadCnt    int64  `gorm:"not null;default:0;comment:阅读数"`
	LikeCnt    int64  `gorm:"not null;default:0;comment:点赞数"`
	CollectCnt int64  `gorm:"not null;default:0;comment:收藏数"`
	Ctime      int64
	Utime      int64
}
