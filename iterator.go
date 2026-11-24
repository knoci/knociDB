package knocidb

import (
	"bytes"
	"container/heap"
	"time"

	"github.com/knoci/knocidb/internal/arenaskl"
	"github.com/knoci/knocidb/wal"
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
	locked     bool
	startKey   []byte
	endKey     []byte
	err        error
}

// Err 返回迭代过程中遇到的首次 I/O 或解码错误。
func (mi *Iterator) Err() error {
	return mi.err
}

// Rewind 将迭代器定位到第一个键
func (mi *Iterator) Rewind() {
	if len(mi.startKey) > 0 {
		mi.Seek(mi.startKey)
		return
	}
	h := mi.h[:0]
	for _, v := range mi.itrs {
		v.iter.Rewind()
		if v.iter.Valid() {
			v.idx = len(h)
			h = append(h, v)
		}
	}
	heap.Init(&h)
	mi.h = h
}

// Seek 将迭代器移动到大于等于（当reverse为true时为小于等于）指定键的位置
func (mi *Iterator) Seek(key []byte) {
	if len(mi.startKey) > 0 && bytes.Compare(key, mi.startKey) < 0 {
		key = mi.startKey
	}
	h := mi.h[:0]
	for _, v := range mi.itrs {
		v.iter.Seek(key)
		if v.iter.Valid() {
			v.idx = len(h)
			h = append(h, v)
		}
	}
	heap.Init(&h)
	mi.h = h
}

// cleanKey 从所有迭代器中移除一个键。
// 如果迭代器在清理后变为空，则从堆中移除它们。
func (mi *Iterator) cleanKey(oldKey []byte, version int) {
	defer func() {
		if r := recover(); r != nil {
			mi.unlock()
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

// Value 获取当前值；若底层 VLog 读取失败则记录到 mi.err 并返回 nil（不再吞错 panic）。
func (mi *Iterator) Value() []byte {
	if mi.h.Len() == 0 {
		return nil
	}
	topIter := mi.h[0]
	switch topIter.iType {
	// 磁盘B+树迭代器
	case BptreeItr:
		raw, ok := topIter.iter.Value().([]byte)
		if !ok {
			return nil
		}
		keyPos := &KeyPosition{
			key:       topIter.iter.Key(),
			partition: uint32(mi.db.vlog.getKeyPartition(topIter.iter.Key())),
			position:  wal.DecodeChunkPosition(raw),
		}
		record, err := mi.db.vlog.read(keyPos)
		if err != nil {
			if mi.err == nil {
				mi.err = err
			}
			return nil
		}
		return record.value
	// memtable迭代器
	case MemItr:
		return topIter.iter.Value().(arenaskl.ValueStruct).Value
	default:
		return nil
	}
}

// Valid 返回迭代器是否仍有有效键（采用迭代循环跳过墓碑与过期键，避免深递归栈溢出）。
func (mi *Iterator) Valid() bool {
	now := time.Now().UnixMilli()
	for mi.h.Len() > 0 {
		topIter := mi.h[0]
		if len(mi.endKey) > 0 && bytes.Compare(topIter.iter.Key(), mi.endKey) > 0 {
			return false
		}
		if topIter.iType == MemItr {
			vs := topIter.iter.Value().(arenaskl.ValueStruct)
			if vs.Meta == LogRecordDeleted || (vs.ExpireAt > 0 && vs.ExpireAt <= now) {
				mi.cleanKey(topIter.iter.Key(), topIter.version)
				topIter.iter.Next()
				if topIter.iter.Valid() {
					heap.Fix(&mi.h, topIter.idx)
				} else {
					heap.Remove(&mi.h, topIter.idx)
				}
				continue
			}
			return true
		}
		if bi, ok := topIter.iter.(*bptreeIterator); ok {
			exp := bi.ExpireAt()
			if exp > 0 && exp <= now {
				mi.cleanKey(topIter.iter.Key(), topIter.version)
				topIter.iter.Next()
				if topIter.iter.Valid() {
					heap.Fix(&mi.h, topIter.idx)
				} else {
					heap.Remove(&mi.h, topIter.idx)
				}
				continue
			}
		}
		return true
	}
	return false
}

// Close 关闭迭代器
func (mi *Iterator) Close() error {
	defer mi.unlock()
	var firstErr error
	for _, itr := range mi.itrs {
		if err := itr.iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (mi *Iterator) unlock() {
	if !mi.locked {
		return
	}
	mi.locked = false
	mi.db.activeIters.Add(-1)
	mi.db.vlogGuard.RUnlock()
}

// snapshotMemtablesForIterator 采用 MVCC Sequence 零冻结快照：
// 仅短暂获取各分片读锁拷贝 memtable 指针并捕获当前 readSeq，
// 彻底消除每次 NewIterator 冻结轮转全部分片 96MB Memtable 与 WAL/Flush 风暴。
func (db *DB) snapshotMemtablesForIterator() ([]*memtable, uint64, error) {
	if db.closed.Load() {
		return nil, 0, ErrDBClosed
	}
	var tables []*memtable
	for _, s := range db.shards {
		s.mu.RLock()
		if db.closed.Load() {
			s.mu.RUnlock()
			return nil, 0, ErrDBClosed
		}
		// oldest-first：先放 s.immu，最后放 s.active，保证版本号递增覆盖旧值。
		tables = append(tables, s.immu...)
		if s.active != nil {
			tables = append(tables, s.active)
		}
		s.mu.RUnlock()
	}
	readSeq := db.seq.Load()
	return tables, readSeq, nil
}

// NewIterator 返回一个新的 MVCC 快照迭代器。
// 调用者有责任在不再使用迭代器时调用 Close，否则会导致资源泄漏。
// 迭代器不是线程安全的，不可从多个 goroutine 并发使用同一个迭代器。
func (db *DB) NewIterator(options IteratorOptions) (*Iterator, error) {
	db.vlogGuard.RLock()
	if db.closed.Load() {
		db.vlogGuard.RUnlock()
		return nil, ErrDBClosed
	}
	// 先递增活跃迭代器计数，再获取分片读锁捕获 readSeq：保证任何并发写若观察到 activeIters==0，必在 readSeq 之前完成。
	db.activeIters.Add(1)
	memtableList, readSeq, err := db.snapshotMemtablesForIterator()
	if err != nil {
		db.activeIters.Add(-1)
		db.vlogGuard.RUnlock()
		return nil, err
	}
	itrs := make([]*singleIter, 0, len(memtableList)+db.options.PartitionNum)
	success := false
	defer func() {
		if !success {
			for _, sItr := range itrs {
				_ = sItr.iter.Close()
			}
			db.activeIters.Add(-1)
			db.vlogGuard.RUnlock()
		}
	}()

	itrsM := make(map[int]*singleIter)
	version := 0
	index, ok := db.index.(*BPTree)
	if !ok {
		return nil, ErrUnsupportedIndex
	}

	for i := 0; i < db.options.PartitionNum; i++ {
		tx, err := index.trees[i].Begin(false)
		if err != nil {
			return nil, err
		}
		itr := newBptreeIterator(tx, options)
		itr.Rewind()
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
	for i := 0; i < len(memtableList); i++ {
		itr := newMemtableIterator(options, memtableList[i], readSeq)
		itr.Rewind()
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

	h := append(iterHeap(nil), itrs...)
	heap.Init(&h)

	success = true
	return &Iterator{
		h:          h,
		itrs:       itrs,
		versionMap: itrsM,
		db:         db,
		locked:     true,
	}, nil
}
