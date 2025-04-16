package diskhash

import "os"

type Options struct {
	// DirPath是存储哈希表文件的目录路径。
	DirPath string

	// SlotValueLength是每个槽位值的长度。
	SlotValueLength uint32

	// LoadFactor是哈希表的负载因子。
	// 负载因子是哈希表中元素数量与表大小的比率。
	// 如果比率大于负载因子，哈希表将自动扩展。
	// 默认值为0.7。
	LoadFactor float64
}

// 默认
var DefaultOptions = Options{
	DirPath:         os.TempDir(),
	SlotValueLength: 0,
	LoadFactor:      0.7,
}
