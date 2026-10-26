package knocidb

// KVEngine 是 Raft 状态机依赖的最小存储接口。
// 单机 DB 和测试替身都可以实现它，避免状态机直接依赖具体 *DB。
type KVEngine interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	NewBatch(options BatchOptions) *Batch
	GetOptions() Options
	ExportSnapshot() error
	ImportSnapshot(path string) error
	Close() error
}

var _ KVEngine = (*DB)(nil)
