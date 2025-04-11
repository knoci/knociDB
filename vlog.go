package knocidb

import (
	"knocidb/wal"
)

const (
	valueLogFileExt     = ".VLOG.%d"
	tempValueLogFileExt = ".VLOG.%d.temp"
)

// valueLog 值日志是基于 Wisckey 论文中的概念命名的
// 详情可以看doc中的Wisckey论文
type valueLog struct {
	walFiles      []*wal.WAL
	dpTables      []*discardtable
	discardNumber uint32
	totalNumber   uint32
	options       valueLogOptions
}

type valueLogOptions struct {
	// dirPath 指定 WAL 段文件存储的目录路径
	dirPath string

	// segmentSize 指定每个段文件的最大字节大小
	segmentSize int64

	// 值日志被分成多个部分用于并发读写
	partitionNum uint32

	// 用于分片的哈希函数
	hashKeyFunction func([]byte) uint64

	// 在读取指定内存容量的条目后将有效条目写入磁盘
	compactBatchCapacity int

	// 已废弃的数量
	discardtableNumber uint32

	// 总数量
	totalNumber uint32
}
