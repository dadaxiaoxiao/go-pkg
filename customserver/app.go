package customserver

import (
	"github.com/dadaxiaoxiao/go-pkg/ginx"
	"github.com/dadaxiaoxiao/go-pkg/grpcx"
	"github.com/dadaxiaoxiao/go-pkg/saramax"
	"github.com/robfig/cron/v3"
)

type App struct {
	GRPCServer *grpcx.Server
	GinServer  *ginx.Server
	Consumers  []saramax.Consumer
	Crons      []*cron.Cron
}
