package jobx

// Job 为了便于拓展，自定义接口
// 使用 重试、监控和告警等扩展实现
type Job interface {
	// Name Job 名称便于区分
	Name() string
	// Run Job 执行
	Run() error
}
