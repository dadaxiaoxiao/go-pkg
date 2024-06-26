package migrator

// Entity 实体接口
type Entity interface {
	// ID 要求放回ID
	ID() int64
	// CompareTo 具体的比较逻辑 （）
	CompareTo(t Entity) bool
}
