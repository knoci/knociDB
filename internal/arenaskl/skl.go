package arenaskl

import (
	"bytes"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight = 20
	branching = 4

	// 分块扁平 Arena 参数：每块 2 MiB（2^21），最多 2048 块 = 4 GiB/跳表。
	// 所有节点、Tower 偏移数组、Key 与普通 Value 均编码在无指针 []byte 块内，彻底消除 Go GC 指针扫描。
	chunkShift       = 21
	chunkSize        = 1 << chunkShift // 2 MiB
	chunkMask        = chunkSize - 1
	maxChunks        = 2048
	maxInlinePayload = 1 << 20 // 单条 Key/Value 超过 1 MiB 时自动走 bigVals 旁路表，永不 panic

	valFlagNil uint32 = 1 << 31
	valFlagBig uint32 = 1 << 30
	valLenMask uint32 = (1 << 30) - 1

	keyFlagBig uint32 = 1 << 31
	keyLenMask uint32 = (1 << 31) - 1

	nodeHeaderSize = 32 // seq(8) + expireAt(8) + valPack(8) + metaHeight(4) + keySize(4)
)

// ValueStruct 保存跳表中某一条记录的值、类型标记和过期时间（UnixMilli，0 表示永不过期）。
type ValueStruct struct {
	Value    []byte
	Meta     byte
	ExpireAt int64
}

// node 为 8 字节对齐的无指针紧凑节点头，直接存放在扁平字节 Arena 块中。
// 实际仅按 randomHeight() 分配 height*4 字节 tower，且 Key 字节紧跟在 tower[:height] 之后，
// 使常见 1 层节点 + Key 整体落入单条 64 字节 CPU Cache Line。
type node struct {
	seq        uint64
	expireAt   atomic.Int64
	valPack    atomic.Uint64 // 高 32 位 = valOff（或 bigVals 下标），低 32 位 = valLen | valFlagNil | valFlagBig
	metaHeight atomic.Uint32 // byte0 = Meta，byte1 = height (1..maxHeight)
	keySize    uint32        // 低 31 位 = key 长度，最高位 = keyFlagBig
	tower      [maxHeight]atomic.Uint32
}

const fullNodeStructSize = uint32(unsafe.Sizeof(node{}))

// Skiplist 是面向嵌入式 KV 引擎的零 GC 指针、并发安全 MVCC 扁平 Arena 跳表：
// - 读与迭代完全无锁（依赖 atomic.Uint32/Uint64/Pointer 的 release/acquire 语义）；
// - 键按 (userKey ASC, seq DESC) 排序，同一 userKey 最新版本排在最前；
// - 节点与键值全部编码在无指针 []byte 分块 Arena 中，支持动态扩容且永不触发越界 panic。
type Skiplist struct {
	headOff uint32
	head    *node
	height  atomic.Int32
	memSize atomic.Int64

	// 固定长度分块基址表：读路径通过 chunks[off>>chunkShift].Load() O(1) 无锁寻址。
	chunks [maxChunks]atomic.Pointer[byte]

	// 仅保护写入分配与大对象旁路表；读路径对普通节点完全无锁。
	writeMu   sync.Mutex
	chunkBufs [][]byte
	curChunk  uint32
	curOff    uint32
	curCap    uint32

	bigMu   sync.RWMutex
	bigVals [][]byte
}

// NewSkiplist 创建容量提示为 arenaSize 字节的新跳表（chunk 0 按需起步，最大 2 MiB，避免多分区冷启动预零数百 MB）。
func NewSkiplist(arenaSize int64) *Skiplist {
	if arenaSize < 4096 {
		arenaSize = 4096
	}
	initCap := uint32(chunkSize)
	if arenaSize < int64(chunkSize) {
		initCap = uint32(arenaSize)
	}
	// 确保 chunk 0 至少能放得下哨兵 head 节点
	minHeadCap := uint32(16) + fullNodeStructSize
	if initCap < minHeadCap {
		initCap = minHeadCap
	}

	first := make([]byte, initCap)
	s := &Skiplist{
		chunkBufs: [][]byte{first},
		curChunk:  0,
		curOff:    8, // offset 0 保留为空指针哨兵
		curCap:    initCap,
	}
	s.chunks[0].Store(&first[0])
	s.height.Store(1)

	headOff, headPtr := s.allocNodeRaw(fullNodeStructSize)
	h := (*node)(headPtr)
	h.metaHeight.Store(uint32(maxHeight) << 8)
	s.headOff = headOff
	s.head = h
	return s
}

func (s *Skiplist) nodeAt(off uint32) *node {
	base := s.chunks[off>>chunkShift].Load()
	return (*node)(unsafe.Add(unsafe.Pointer(base), uintptr(off&chunkMask)))
}

func (s *Skiplist) sliceAt(off uint32, length uint32) []byte {
	base := s.chunks[off>>chunkShift].Load()
	return unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(base), uintptr(off&chunkMask))), length)
}

func (s *Skiplist) allocChunkLocked() {
	s.curChunk++
	buf := make([]byte, chunkSize)
	s.chunkBufs = append(s.chunkBufs, buf)
	s.chunks[s.curChunk].Store(&buf[0])
	s.curOff = 0
	s.curCap = chunkSize
}

// allocNodeRaw 分配 8 字节对齐的节点空间，并保证 [ptr, ptr+sizeof(node)] 严格落在当前 chunk 内以兼容 -race/checkptr。
func (s *Skiplist) allocNodeRaw(actualSize uint32) (uint32, unsafe.Pointer) {
	actualSize = (actualSize + 7) &^ 7
	s.curOff = (s.curOff + 7) &^ 7
	need := actualSize
	if need < fullNodeStructSize {
		need = fullNodeStructSize
	}
	if s.curOff+need > s.curCap {
		s.allocChunkLocked()
	}
	off := (s.curChunk << chunkShift) | s.curOff
	base := s.chunks[s.curChunk].Load()
	ptr := unsafe.Add(unsafe.Pointer(base), uintptr(s.curOff))
	s.curOff += actualSize
	return off, ptr
}

func (s *Skiplist) allocBytesLocked(src []byte) (uint32, bool) {
	n := uint32(len(src))
	if n > maxInlinePayload || (s.curOff+n > s.curCap && s.curChunk+1 >= maxChunks) {
		cp := make([]byte, len(src))
		copy(cp, src)
		s.bigMu.Lock()
		idx := uint32(len(s.bigVals))
		s.bigVals = append(s.bigVals, cp)
		s.bigMu.Unlock()
		return idx, true
	}
	if s.curOff+n > s.curCap {
		s.allocChunkLocked()
	}
	off := (s.curChunk << chunkShift) | s.curOff
	base := s.chunks[s.curChunk].Load()
	dst := unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(base), uintptr(s.curOff))), n)
	copy(dst, src)
	s.curOff += n
	return off, false
}

func packVal(off uint32, lenFlags uint32) uint64 {
	return (uint64(off) << 32) | uint64(lenFlags)
}

func (s *Skiplist) allocValLocked(val []byte) uint64 {
	if val == nil {
		return packVal(0, valFlagNil)
	}
	if len(val) == 0 {
		return packVal(0, 0)
	}
	off, isBig := s.allocBytesLocked(val)
	if isBig {
		return packVal(off, valFlagBig)
	}
	return packVal(off, uint32(len(val)))
}

func (s *Skiplist) getBigBytes(idx uint32) []byte {
	s.bigMu.RLock()
	b := s.bigVals[idx]
	s.bigMu.RUnlock()
	return b
}

func (s *Skiplist) nodeKey(n *node) []byte {
	ks := n.keySize
	if ks == 0 {
		return nil
	}
	h := (n.metaHeight.Load() >> 8) & 0xFF
	keyPtr := unsafe.Add(unsafe.Pointer(n), uintptr(nodeHeaderSize+h*4))
	if ks&keyFlagBig != 0 {
		idx := *(*uint32)(keyPtr)
		return s.getBigBytes(idx)
	}
	return unsafe.Slice((*byte)(keyPtr), ks)
}

func (s *Skiplist) readValue(n *node) ValueStruct {
	vp := n.valPack.Load()
	mh := n.metaHeight.Load()
	exp := n.expireAt.Load()

	lenFlags := uint32(vp)
	var val []byte
	switch {
	case lenFlags&valFlagNil != 0:
		val = nil
	case lenFlags&valFlagBig != 0:
		val = s.getBigBytes(uint32(vp >> 32))
	case lenFlags == 0:
		val = []byte{}
	default:
		val = s.sliceAt(uint32(vp>>32), lenFlags&valLenMask)
	}
	return ValueStruct{
		Value:    val,
		Meta:     byte(mh),
		ExpireAt: exp,
	}
}

// MemSize 返回当前跳表估算占用字节数。
func (s *Skiplist) MemSize() int64 {
	return s.memSize.Load()
}

// compareKeySeq 比较 (k1, seq1) 与 (k2, seq2)：userKey 升序，同 key 时 seq 降序（更新的版本在前）。
func compareKeySeq(k1 []byte, seq1 uint64, k2 []byte, seq2 uint64) int {
	if c := bytes.Compare(k1, k2); c != 0 {
		return c
	}
	if seq1 > seq2 {
		return -1
	}
	if seq1 < seq2 {
		return 1
	}
	return 0
}

func randomHeight() uint32 {
	h := uint32(1)
	for h < maxHeight && rand.Uint32N(branching) == 0 {
		h++
	}
	return h
}

// Put 写入 (key, seq) -> val。
// 当 (key, seq) 已存在时（例如 seq==0 的兼容覆写路径）原子替换其值；
// 正常 MVCC 写入每次使用严格递增的 seq，生成不可变新节点。
func (s *Skiplist) Put(key []byte, seq uint64, val ValueStruct) {
	s.PutOrReplace(key, seq, val, false)
}

// PutOrReplace 写入 (key, seq) -> val。
// 当 replace == true（调用方确保当前无活跃 MVCC 快照迭代器）且 key 的最新节点已存在时，
// 在现有节点上原子原位更新值与过期元数据，避免无谓的多版本节点堆积。
func (s *Skiplist) PutOrReplace(key []byte, seq uint64, val ValueStruct, replace bool) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var prev [maxHeight]*node
	curr := s.head
	top := int(s.height.Load()) - 1
	for level := top; level >= 0; level-- {
		for {
			nextOff := curr.tower[level].Load()
			if nextOff == 0 {
				break
			}
			next := s.nodeAt(nextOff)
			cmp := compareKeySeq(s.nodeKey(next), next.seq, key, seq)
			if cmp < 0 {
				curr = next
				continue
			}
			if cmp == 0 {
				// 精确同 (key, seq) 原位原子替换
				vp := s.allocValLocked(val.Value)
				next.expireAt.Store(val.ExpireAt)
				next.metaHeight.Store((next.metaHeight.Load() & 0xFFFFFF00) | uint32(val.Meta))
				next.valPack.Store(vp)
				s.memSize.Add(int64(len(val.Value)))
				return
			}
			break
		}
		prev[level] = curr
	}

	// 无活跃迭代器时，若同 key 最新节点紧跟在 prev[0] 后（seq 更小），直接原位原子更新。
	if replace {
		if candOff := prev[0].tower[0].Load(); candOff != 0 {
			cand := s.nodeAt(candOff)
			if bytes.Equal(s.nodeKey(cand), key) {
				vp := s.allocValLocked(val.Value)
				cand.expireAt.Store(val.ExpireAt)
				cand.metaHeight.Store((cand.metaHeight.Load() & 0xFFFFFF00) | uint32(val.Meta))
				cand.valPack.Store(vp)
				s.memSize.Add(int64(len(val.Value)))
				return
			}
		}
	}

	h := randomHeight()
	curHeight := uint32(s.height.Load())
	if h > curHeight {
		for level := curHeight; level < h; level++ {
			prev[level] = s.head
		}
	}

	// 先写入 Value，再分配并填充紧凑节点（Key 内联在 tower[:h] 后）
	vp := s.allocValLocked(val.Value)

	var keyField uint32
	var inlineKeyBytes uint32
	var bigKeyIdx uint32
	if len(key) > maxInlinePayload {
		idx, _ := s.allocBytesLocked(key)
		bigKeyIdx = idx
		keyField = keyFlagBig | 4
		inlineKeyBytes = 4
	} else {
		keyField = uint32(len(key))
		inlineKeyBytes = uint32(len(key))
	}

	nodeBytes := nodeHeaderSize + h*4 + inlineKeyBytes
	nOff, nPtr := s.allocNodeRaw(nodeBytes)
	n := (*node)(nPtr)
	n.seq = seq
	n.expireAt.Store(val.ExpireAt)
	n.valPack.Store(vp)
	n.metaHeight.Store((h << 8) | uint32(val.Meta))
	n.keySize = keyField
	if inlineKeyBytes > 0 {
		keyDst := unsafe.Add(nPtr, uintptr(nodeHeaderSize+h*4))
		if keyField&keyFlagBig != 0 {
			*(*uint32)(keyDst) = bigKeyIdx
		} else {
			copy(unsafe.Slice((*byte)(keyDst), inlineKeyBytes), key)
		}
	}

	// 从底层到顶层发布节点偏移（release store 保证节点数据对无锁读者可见）
	for level := uint32(0); level < h; level++ {
		n.tower[level].Store(prev[level].tower[level].Load())
		prev[level].tower[level].Store(nOff)
	}
	if h > curHeight {
		s.height.Store(int32(h))
	}
	s.memSize.Add(int64(len(key) + len(val.Value) + int(nodeHeaderSize+h*4)))
}

// Get 查找 userKey 在 readSeq 快照下可见的最新版本（seq <= readSeq）。
// 若 readSeq == 0，则匹配任意 seq（返回最新版本）。
func (s *Skiplist) Get(key []byte, readSeq uint64) (ValueStruct, bool) {
	curr := s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		for {
			nextOff := curr.tower[level].Load()
			if nextOff == 0 {
				break
			}
			next := s.nodeAt(nextOff)
			c := bytes.Compare(s.nodeKey(next), key)
			if c < 0 {
				curr = next
				continue
			}
			if c == 0 {
				if readSeq == 0 {
					if level == 0 {
						return s.readValue(next), true
					}
				} else if next.seq > readSeq {
					curr = next
					continue
				}
			}
			break
		}
	}
	candOff := curr.tower[0].Load()
	for candOff != 0 {
		cand := s.nodeAt(candOff)
		if !bytes.Equal(s.nodeKey(cand), key) {
			break
		}
		if readSeq == 0 || cand.seq <= readSeq {
			return s.readValue(cand), true
		}
		candOff = cand.tower[0].Load()
	}
	return ValueStruct{}, false
}

// findGE 查找首个满足 bytes.Compare(nodeKey, key) >= 0 的节点。
func (s *Skiplist) findGE(key []byte) *node {
	curr := s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		for {
			nextOff := curr.tower[level].Load()
			if nextOff == 0 {
				break
			}
			next := s.nodeAt(nextOff)
			if bytes.Compare(s.nodeKey(next), key) < 0 {
				curr = next
			} else {
				break
			}
		}
	}
	nextOff := curr.tower[0].Load()
	if nextOff == 0 {
		return nil
	}
	return s.nodeAt(nextOff)
}

// findLastBefore 查找 userKey 严格小于 target 的最后一个节点（若 target == nil 则返回整个跳表最后一个节点）。
func (s *Skiplist) findLastBefore(target []byte) *node {
	curr := s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		for {
			nextOff := curr.tower[level].Load()
			if nextOff == 0 {
				break
			}
			next := s.nodeAt(nextOff)
			if target == nil || bytes.Compare(s.nodeKey(next), target) < 0 {
				curr = next
			} else {
				break
			}
		}
	}
	if curr == s.head {
		return nil
	}
	return curr
}

// newestVisibleForUserKey 返回 userKey 在 readSeq 下最新的可见节点（若该 key 所有版本均 > readSeq 返回 nil）。
func (s *Skiplist) newestVisibleForUserKey(userKey []byte, readSeq uint64) *node {
	n := s.findGE(userKey)
	for n != nil && bytes.Equal(s.nodeKey(n), userKey) {
		if readSeq == 0 || n.seq <= readSeq {
			return n
		}
		nextOff := n.tower[0].Load()
		if nextOff == 0 {
			return nil
		}
		n = s.nodeAt(nextOff)
	}
	return nil
}

// Iterator 提供无锁 MVCC 快照迭代（正向或反向），每个 userKey 仅返回 <= readSeq 的最新版本。
type Iterator struct {
	skl     *Skiplist
	reverse bool
	readSeq uint64
	curr    *node
}

// NewIterator 创建一个 MVCC 迭代器；readSeq == 0 表示读取所有可见最新版本。
func (s *Skiplist) NewIterator(reverse bool, readSeq uint64) *Iterator {
	return &Iterator{
		skl:     s,
		reverse: reverse,
		readSeq: readSeq,
	}
}

func (it *Iterator) advanceForwardToVisible(n *node) *node {
	for n != nil {
		if it.readSeq == 0 || n.seq <= it.readSeq {
			return n
		}
		nextOff := n.tower[0].Load()
		if nextOff == 0 {
			return nil
		}
		n = it.skl.nodeAt(nextOff)
	}
	return nil
}

func (it *Iterator) stepBackwardFrom(last *node) *node {
	for last != nil {
		userKey := it.skl.nodeKey(last)
		if vis := it.skl.newestVisibleForUserKey(userKey, it.readSeq); vis != nil {
			return vis
		}
		last = it.skl.findLastBefore(userKey)
	}
	return nil
}

// Rewind 定位到首个（或反向最后一个）可见键。
func (it *Iterator) Rewind() {
	if !it.reverse {
		firstOff := it.skl.head.tower[0].Load()
		if firstOff == 0 {
			it.curr = nil
			return
		}
		it.curr = it.advanceForwardToVisible(it.skl.nodeAt(firstOff))
		return
	}
	it.curr = it.stepBackwardFrom(it.skl.findLastBefore(nil))
}

// Seek 正向定位到 >= key 的首个可见键；反向定位到 <= key 的最大可见键。
func (it *Iterator) Seek(key []byte) {
	if !it.reverse {
		it.curr = it.advanceForwardToVisible(it.skl.findGE(key))
		return
	}
	if vis := it.skl.newestVisibleForUserKey(key, it.readSeq); vis != nil {
		it.curr = vis
		return
	}
	it.curr = it.stepBackwardFrom(it.skl.findLastBefore(key))
}

// Next 移动到下一个（或反向前一个）不同的可见 userKey。
func (it *Iterator) Next() {
	if it.curr == nil {
		return
	}
	currKey := it.skl.nodeKey(it.curr)
	if !it.reverse {
		nextOff := it.curr.tower[0].Load()
		for nextOff != 0 {
			n := it.skl.nodeAt(nextOff)
			if !bytes.Equal(it.skl.nodeKey(n), currKey) {
				it.curr = it.advanceForwardToVisible(n)
				return
			}
			nextOff = n.tower[0].Load()
		}
		it.curr = nil
		return
	}
	it.curr = it.stepBackwardFrom(it.skl.findLastBefore(currKey))
}

// Valid 返回当前位置是否有效。
func (it *Iterator) Valid() bool {
	return it.curr != nil
}

// Key 返回当前用户键。
func (it *Iterator) Key() []byte {
	if it.curr == nil {
		return nil
	}
	return it.skl.nodeKey(it.curr)
}

// Value 返回当前 ValueStruct。
func (it *Iterator) Value() ValueStruct {
	if it.curr == nil {
		return ValueStruct{}
	}
	return it.skl.readValue(it.curr)
}

// Close 释放迭代器状态。
func (it *Iterator) Close() error {
	it.curr = nil
	return nil
}
