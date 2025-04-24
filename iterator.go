package knocidb

import (
	"bytes"
	"container/heap"
	"github.com/dgraph-io/badger/v4/y"
	"knocidb/wal"
)

type iterType uint8

const (
	BptreeItr iterType = iota
	MemItr
)

// baseIterator 基础迭代器接口
type baseIterator interface {
	// Rewind 将迭代器定位到第一个键
	Rewind()
	// Seek 将迭代器移动到大于等于（当reverse为true时为小于等于）指定键的位置
	Seek(key []byte)
	// Next 将迭代器移动到下一个键
	Next()
	// Key 获取当前键
	Key() []byte
	// Value 获取当前值
	Value() any
	// Valid 返回迭代器是否可用
	Valid() bool
	// Close 关闭迭代器
	Close() error
}

// singleIter 用于构建堆的元素，实现了container.heap接口
type singleIter struct {
	iType   iterType
	options IteratorOptions
	version int // 更高的version表示更新的数据
	idx     int // 在堆中的索引
	iter    baseIterator
}
type iterHeap []*singleIter

// Len 返回集合中元素的数量
func (ih *iterHeap) Len() int {
	return len(*ih)
}

// Less 判断索引i的元素是否必须排在索引j的元素之前
func (ih *iterHeap) Less(i int, j int) bool {
	ki, kj := (*ih)[i].iter.Key(), (*ih)[j].iter.Key()
	if bytes.Equal(ki, kj) {
		return (*ih)[i].version > (*ih)[j].version
	}
	if (*ih)[i].options.Reverse {
		return bytes.Compare(ki, kj) == 1
	}
	return bytes.Compare(ki, kj) == -1
}

// Swap 交换索引i和j的元素
func (ih *iterHeap) Swap(i int, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
	(*ih)[i].idx, (*ih)[j].idx = i, j
}

// Push 添加x作为第Len()个元素
func (ih *iterHeap) Push(x any) {
	*ih = append(*ih, x.(*singleIter))
}

// Pop 移除并返回第Len()-1个元素
func (ih *iterHeap) Pop() any {
	old := *ih
	n := len(old)
	x := old[n-1]
	*ih = old[0 : n-1]
	return x
}

// Iterator 持有一个堆和一组实现了baseIterator接口的迭代器
type Iterator struct {
	h          iterHeap
	itrs       []*singleIter       // 用于重建堆
	versionMap map[int]*singleIter // 映射 version->singleIter
	db         *DB
}

// Rewind 将迭代器定位到第一个键
func (mi *Iterator) Rewind() {
	for _, v := range mi.itrs {
		v.iter.Rewind()
	}
	h := iterHeap(mi.itrs)
	heap.Init(&h)
	mi.h = h
}

// Seek 将迭代器移动到大于等于（当reverse为true时为小于等于）指定键的位置
func (mi *Iterator) Seek(key []byte) {
	seekItrs := make([]*singleIter, 0)
	for _, v := range mi.itrs {
		v.iter.Seek(key)
		if v.iter.Valid() {
			// 重置索引
			v.idx = len(seekItrs)
			seekItrs = append(seekItrs, v)
		}
	}
	h := iterHeap(seekItrs)
	heap.Init(&h)
	mi.h = h
}

// cleanKey 从所有迭代器中移除一个键。
// 如果迭代器在清理后变为空，则从堆中移除它们。
func (mi *Iterator) cleanKey(oldKey []byte, version int) {
	defer func() {
		if r := recover(); r != nil {
			mi.db.mu.Unlock()
		}
	}()
	for i := 0; i < len(mi.itrs); i++ {
		// 版本相同
		if i == version {
			continue
		}
		itr := mi.versionMap[i]
		// iter不可用
		if !itr.iter.Valid() {
			continue
		}

		for itr.iter.Valid() && bytes.Equal(itr.iter.Key(), oldKey) {
			if itr.version > version {
				panic("version error")
			}
			itr.iter.Next()
		}
		if itr.iter.Valid() {
			heap.Fix(&mi.h, itr.idx)
		} else {
			heap.Remove(&mi.h, itr.idx)
		}
	}
}

// Next 将迭代器移动到下一个键
func (mi *Iterator) Next() {
	if mi.h.Len() == 0 {
		return
	}

	// 从堆顶取出最小的键
	topIter := mi.h[0]
	mi.cleanKey(topIter.iter.Key(), topIter.version)

	// 转到下一个键并且更新堆
	topIter.iter.Next()
	if !topIter.iter.Valid() {
		heap.Remove(&mi.h, topIter.idx)
	} else {
		heap.Fix(&mi.h, topIter.idx)
	}
}

// Key 获取当前键
func (mi *Iterator) Key() []byte {
	return mi.h[0].iter.Key()
}

// Value 获取当前值
func (mi *Iterator) Value() []byte {
	topIter := mi.h[0]
	switch topIter.iType {
	// 磁盘B+树迭代器
	case BptreeItr:
		keyPos := new(KeyPosition)
		keyPos.key = topIter.iter.Key()
		keyPos.partition = uint32(mi.db.vlog.getKeyPartition(topIter.iter.Key()))
		keyPos.position = wal.DecodeChunkPosition(topIter.iter.Value().([]byte))
		record, err := mi.db.vlog.read(keyPos)
		if err != nil {
			panic(err)
		}
		return record.value
	// memtable迭代器
	case MemItr:
		return topIter.iter.Value().(y.ValueStruct).Value
	default:
		panic("iType not support")
	}
}

// Valid 返回迭代器是否已耗尽
func (mi *Iterator) Valid() bool {
	if mi.h.Len() == 0 {
		return false
	}
	topIter := mi.h[0]
	// 如果topIter是memtable迭代器，并且当前值被删除
	if topIter.iType == MemItr && topIter.iter.Value().(y.ValueStruct).Meta == LogRecordDeleted {
		// 删除键
		mi.cleanKey(topIter.iter.Key(), topIter.version)
		// 转到下一个
		topIter.iter.Next()
		if topIter.iter.Valid() {
			heap.Fix(&mi.h, topIter.idx)
		} else {
			heap.Remove(&mi.h, topIter.idx)
		}
		return mi.Valid()
	}
	return true
}

// Close 关闭迭代器
func (mi *Iterator) Close() error {
	for _, itr := range mi.itrs {
		err := itr.iter.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// NewIterator 返回一个新的迭代器
// 该迭代器将遍历数据库中的所有键
// 调用者有责任在不再使用迭代器时调用Close，否则会导致资源泄漏
// 迭代器不是线程安全的，不应该从多个goroutine并发使用同一个迭代器
func (db *DB) NewIterator(options IteratorOptions) (*Iterator, error) {
	if db.options.IndexType == Hash {
		return nil, ErrDBIteratorUnsupportedTypeHASH
	}
	db.mu.Lock()
	defer func() {
		if r := recover(); r != nil {
			db.mu.Unlock()
		}
	}()

	itrs := make([]*singleIter, 0, db.options.PartitionNum+len(db.immuMems)+1)
	itrsM := make(map[int]*singleIter)
	version := 0
	index, ok := db.index.(*BPTree)
	if !ok {
		panic("index type not support")
	}

	for i := 0; i < db.options.PartitionNum; i++ {
		tx, err := index.trees[i].Begin(false)
		if err != nil {
			return nil, err
		}
		itr := newBptreeIterator(
			tx,
			options,
		)
		itr.Rewind()
		// is empty
		if !itr.Valid() {
			_ = itr.Close()
			continue
		}
		itrs = append(itrs, &singleIter{
			iType:   BptreeItr,
			options: options,
			version: version,
			idx:     version,
			iter:    itr,
		})
		itrsM[version] = itrs[len(itrs)-1]
		version++
	}
	memtableList := make([]*memtable, len(db.immuMems)+1)
	copy(memtableList, append(db.immuMems, db.activeMem))
	for i := 0; i < len(memtableList); i++ {
		itr := newMemtableIterator(options, memtableList[i])
		itr.Rewind()
		// is empty
		if !itr.Valid() {
			_ = itr.Close()
			continue
		}
		itrs = append(itrs, &singleIter{
			iType:   MemItr,
			options: options,
			version: version,
			idx:     version,
			iter:    itr,
		})
		itrsM[version] = itrs[len(itrs)-1]
		version++
	}

	h := iterHeap(itrs)
	heap.Init(&h)

	return &Iterator{
		h:          h,
		itrs:       itrs,
		versionMap: itrsM,
		db:         db,
	}, nil
}
