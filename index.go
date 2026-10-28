package knocidb

const (
	// indexFileExt 是索引文件的扩展名
	indexFileExt = "INDEX.%d"
)

// Index 是索引实现的接口。
// 当前只保留 B+Tree 索引；diskhash 已废弃。
type Index interface {
	// PutBatch 将批量记录写入索引
	PutBatch(keyPositions []*KeyPosition) ([]*KeyPosition, error)

	// Get 通过键获取块位置
	Get(key []byte) (*KeyPosition, error)

	// DeleteBatch 从索引中删除批量记录
	DeleteBatch(keys [][]byte) ([]*KeyPosition, error)

	// Sync 将索引数据同步到磁盘
	Sync() error

	// Close 关闭索引
	Close() error
}

// openIndex 打开 B+Tree 索引。
func openIndex(options indexOptions) (Index, error) {
	return openBTreeIndex(options)
}

type indexOptions struct {
	dirPath string // 索引目录路径

	partitionNum int // 用于分片的索引分区数量

	keyHashFunction func([]byte) uint64 // 用于分片的哈希函数
}

func (io *indexOptions) getKeyPartition(key []byte) int {
	hashFn := io.keyHashFunction
	return int(hashFn(key) % uint64(io.partitionNum))
}
