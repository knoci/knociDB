package knocidb

import (
	"github.com/cespare/xxhash/v2"
	"os"
	"time"
)

type Options struct {
	// DirPath 指定存储所有数据库文件的目录路径
	DirPath string

	// MemtableSize 表示内存表的最大字节大小 默认值为 64MB
	MemtableSize uint32

	// MemtableNums 表示在刷新到磁盘之前内存中保留的最大内存表数量 默认值为 15
	MemtableNums int

	// Sync 表示是否通过操作系统缓冲区缓存同步写入到实际磁盘
	Sync bool

	// BytesPerSync 指定在调用 fsync 之前要写入的字节数
	BytesPerSync uint32

	// PartitionNum 指定用于索引和值日志的分区数
	PartitionNum int

	// KeyHashFunction 指定用于分片的哈希函数 用于确定键属于哪个分区 默认值为 xxhash
	KeyHashFunction func([]byte) uint64

	// ValueLogFileSize 单个值日志文件的大小 默认值为 1GB
	ValueLogFileSize int64

	// indexType 索引类型 默认值为 bptree
	IndexType IndexType

	// 在读取指定内存容量的条目后将条目写入磁盘
	CompactBatchCapacity int

	// 废弃表推荐压缩率
	AdvisedCompactionRate float32

	// 废弃表强制压缩率
	ForceCompactionRate float32

	// 是否启用磁盘监控
	EnableDiskIO bool

	// 磁盘IO采样间隔，单位为毫秒
	DiskIOSamplingInterval int

	// 用于检索一段时间内IO繁忙状态的窗口
	DiskIOSamplingWindow int

	// 采样时间内IO时间的比率，用于表示IO的繁忙状态
	DiskIOBusyRate float32

	// 是否支持自动压缩
	AutoCompactSupport bool

	// WaitMemSpaceTimeout 指定等待内存表空间的超时时间
	// 当所有内存表都已满时，将由后台协程刷新到磁盘 但如果刷新速度慢于写入速度，内存表中可能没有空间
	// 因此写入操作将等待内存表中的空间，超时时间由 WaitMemSpaceTimeout 指定
	// 如果超过超时时间，写入操作将失败，默认值为 100ms
	WaitMemSpaceTimeout time.Duration
}

// BatchOptions 指定创建批处理的选项
type BatchOptions struct {
	WriteOptions
	// ReadOnly 指定批处理是否为只读
	ReadOnly bool
}

// WriteOptions 为 PutWithOptions 和 DeleteWithOptions 设置可选参数
// 如果使用 Put 和 Delete（不带选项），则表示使用默认值
type WriteOptions struct {
	// Sync 表示是否通过操作系统缓冲区缓存同步写入到实际磁盘
	Sync bool

	// DisableWal 如果为 true，写入将不会首先进入预写日志，在崩溃后可能会丢失写入 默认值为 false
	DisableWal bool
}

// IteratorOptions 是迭代器的选项
type IteratorOptions struct {
	// Prefix 按前缀过滤键
	Prefix []byte

	// Reverse 表示迭代器是否反向 false 是正向，true 是反向
	Reverse bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// 默认选项
var DefaultOptions = Options{
	DirPath:                tempDBDir(),
	MemtableSize:           64 * MB,
	MemtableNums:           15,
	Sync:                   false,
	BytesPerSync:           0,
	PartitionNum:           3,
	KeyHashFunction:        xxhash.Sum64,
	ValueLogFileSize:       1 * GB,
	IndexType:              BTree,
	CompactBatchCapacity:   1 << 30,
	AdvisedCompactionRate:  0.3,
	ForceCompactionRate:    0.5,
	EnableDiskIO:           false,
	DiskIOSamplingInterval: 100,
	DiskIOSamplingWindow:   10,
	DiskIOBusyRate:         0.5,
	AutoCompactSupport:     false,
	WaitMemSpaceTimeout:    100 * time.Millisecond,
}

var DefaultBatchOptions = BatchOptions{
	WriteOptions: DefaultWriteOptions,
	ReadOnly:     false,
}

var DefaultWriteOptions = WriteOptions{
	Sync:       false,
	DisableWal: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "knocidb-temp")
	return dir
}
