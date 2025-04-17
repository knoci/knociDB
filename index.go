package knocidb

import "knocidb/diskhash"

const (
	// indexFileExt 是索引文件的扩展名
	indexFileExt = "INDEX.%d"
)

// Index 是索引实现的接口。
// 索引是一个键值存储，将键映射到块位置。
type Index interface {
	// PutBatch 将批量记录写入索引
	PutBatch(keyPositions []*KeyPosition, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error)

	// Get 通过键获取块位置
	Get(key []byte, matchKeyFunc ...diskhash.MatchKeyFunc) (*KeyPosition, error)

	// DeleteBatch 从索引中删除批量记录
	DeleteBatch(keys [][]byte, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error)

	// Sync 将索引数据同步到磁盘
	Sync() error

	// Close 关闭索引
	Close() error
}

// openIndex 根据索引类型打开指定的索引
// 目前，我们支持两种索引类型：BTree 和 Hash，
// 它们都是基于磁盘的索引。
func openIndex(options indexOptions) (Index, error) {
	switch options.indexType {
	case BTree:
		return openBTreeIndex(options)
	case Hash:
		// TODO
		return nil, nil
	default:
		panic("unknown index type")
	}
}

type IndexType int8

const (
	// BTree 是 BoltDB 索引类型
	BTree IndexType = iota
	// Hash 是 diskhash 索引类型
	Hash
)

type indexOptions struct {
	indexType IndexType

	dirPath string // 索引目录路径

	partitionNum int // 用于分片的索引分区数量

	keyHashFunction func([]byte) uint64 // 用于分片的哈希函数
}

func (io *indexOptions) getKeyPartition(key []byte) int {
	hashFn := io.keyHashFunction
	return int(hashFn(key) % uint64(io.partitionNum))
}
