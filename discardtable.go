package knocidb

import (
	"github.com/google/uuid"
)

type (
	// Discardtable 用于存储已删除/更新的键的旧信息。
	// 对于每次写入/更新生成的 uuid，我们将其存储在表中。
	// 这在压缩时很有用，它允许我们在不访问索引的情况下
	// 知道值日志中的键值对是否是最新的。
	discardtable struct {
		partition int                    // vlog 中的分片
		table     map[uuid.UUID]struct{} // 在内存中存储已废弃的键的 uuid
		size      uint32                 // 当前废弃条目的数量
	}
)

// 创建一个新的 discardtable
func newDiscardTable(partition int) *discardtable {
	return &discardtable{
		partition: partition,
		table:     make(map[uuid.UUID]struct{}),
		size:      0,
	}
}

// 添加一个新的废弃条目
func (dt *discardtable) addEntry(id uuid.UUID) {
	dt.table[id] = struct{}{}
	dt.size++
}

// 查询一个废弃条目是否存在
func (dt *discardtable) existEntry(id uuid.UUID) bool {
	_, exists := dt.table[id]
	return exists
}

// 清理废弃表
func (dt *discardtable) clean() {
	dt.table = make(map[uuid.UUID]struct{})
	dt.size = 0
}
