package knocidb

import "time"

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

	// 废弃表推荐压缩率
	AdvisedCompactionRate float32

	// 废弃表强制压缩率
	ForceCompactionRate float32

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
