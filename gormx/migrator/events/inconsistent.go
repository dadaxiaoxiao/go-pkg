package events

// InconsistentEvent 不一致事件
type InconsistentEvent struct {
	ID int64
	// 取值为 SRC 以源表为准; 取值为 DST，以目标表为准
	Direction string
	// 引起不一致的原因，便于第三方观测
	// 可选
	Type string
}

const (
	// InconsistentEventTypeTargetMissing  校验的目标数据缺失
	InconsistentEventTypeTargetMissing = "target_missing"

	// InconsistentEventTypeNEQ 不相等
	InconsistentEventTypeNEQ = "neq"

	// InconsistentEventTypeBaseMissing  校验的源数据缺失
	InconsistentEventTypeBaseMissing = "base_missing"
)
