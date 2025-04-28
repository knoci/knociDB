package wal

import (
	"os"
	"time"
)

// Options 表示预写式日志（WAL）的配置选项。
type Options struct {
	// DirPath 指定存储 WAL 段文件的目录路径。
	DirPath string

	// SegmentSize 指定每个段文件的最大字节大小。
	SegmentSize int64

	// SegmentFileExt 指定段文件的扩展名。
	// 文件扩展名必须以点"."开头，默认值为".SEG"。
	SegmentFileExt string

	// Sync 决定是否通过操作系统缓冲区缓存将写入同步到实际磁盘。
	// 启用同步对于单个写操作的持久性是必需的，但也会导致写入速度变慢。
	//
	// 如果设为 false，当机器崩溃时，某些最近的写入可能会丢失。 注意，如果仅仅是进程崩溃（机器未崩溃），则不会丢失任何写入。
	//
	// 换句话说，Sync 为 false 时与写入系统调用具有相同的语义。 Sync 为 true 时意味着写入后跟随 fsync。
	Sync bool

	// BytesPerSync 指定在调用 fsync 之前要写入的字节数。
	BytesPerSync uint32

	// SyncInterval 是执行显式同步的时间间隔。 如果 SyncInterval 为零，则不执行定期同步。
	SyncInterval time.Duration

	// ObjectStorage 指定对象存储的配置。如果不为nil，WAL将使用对象存储。
	ObjectStorage *ObjectStorageConfig
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        os.TempDir(),
	SegmentSize:    GB,
	SegmentFileExt: ".SEG",
	Sync:           false,
	BytesPerSync:   0,
	SyncInterval:   0,
	ObjectStorage:  nil,
}
