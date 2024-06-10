package accesslog

import "go.uber.org/zap"

type ZapLogger struct {
	log *zap.Logger
}

// NewZapLogger 实现自定义 Logger
func NewZapLogger(log *zap.Logger) Logger {
	return &ZapLogger{
		log: log,
	}
}

func (z *ZapLogger) Debug(msg string, args ...Field) {
	z.log.Debug(msg, z.toZapFileds(args)...)
}

func (z *ZapLogger) Info(msg string, args ...Field) {
	z.log.Info(msg, z.toZapFileds(args)...)
}

func (z *ZapLogger) Warn(msg string, args ...Field) {
	z.log.Warn(msg, z.toZapFileds(args)...)
}

func (z *ZapLogger) Error(msg string, args ...Field) {
	z.log.Error(msg, z.toZapFileds(args)...)
}

func (z *ZapLogger) toZapFileds(args []Field) []zap.Field {
	res := make([]zap.Field, 0, len(args))
	for _, arg := range args {
		res = append(res, zap.Any(arg.Key, arg.Value))
	}
	return res
}
