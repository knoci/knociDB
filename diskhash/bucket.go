package diskhash

import (
	"encoding/binary"
	"io"
	"knocidb/diskhash/fs"
)

// bucket是diskhash中文件的基本单位，每个文件最多包含31个槽位。
type bucket struct {
	slots      [slotsPerBucket]Slot // 31个槽位
	offset     int64                // bucket在文件中的偏移量
	nextOffset int64                // 下一个溢出bucket的偏移量
	file       fs.File              // 包含该bucket的文件
	bucketSize uint32
}

// bucketIterator用于遍历哈希表中的所有bucket。
type bucketIterator struct {
	currentFile  fs.File
	overflowFile fs.File
	offset       int64
	slotValueLen uint32
	bucketSize   uint32
}

// Slot是bucket的基本单位，每个槽位包含一个键的哈希值和一个值。
type Slot struct {
	Hash  uint32 // 键的哈希值
	Value []byte // 原始值
}

// 槽的写入器。
type slotWriter struct {
	currentBucket    *bucket
	currentSlotIndex int
	prevBuckets      []*bucket
	overwrite        bool
}

// 获取哈希表中指定位bucket的偏移
func (t *Table) bucketOffset(bucketIndex uint32) int64 {
	return int64((bucketIndex + 1) * t.meta.BucketSize)
}

// 创建一个新的bucketIterator
func (t *Table) newBucketIterator(startBucket uint32) *bucketIterator {
	return &bucketIterator{
		currentFile:  t.primaryFile,
		overflowFile: t.overflowFile,
		offset:       t.bucketOffset(startBucket),
		slotValueLen: t.options.SlotValueLength,
		bucketSize:   t.meta.BucketSize,
	}
}

// next获取当前bucket然后移动到下一个bucket。
func (bi *bucketIterator) next() (*bucket, error) {
	// 因为bucket的size初始化时从文件的第一个bucket之后计算，所以永不为0。
	if bi.offset == 0 {
		return nil, io.EOF
	}

	// 读取bucket和它的所有槽。
	bucket, err := bi.readBucket()
	if err != nil {
		return nil, err
	}

	// 转到下一个溢出bucket。
	bi.offset = bucket.nextOffset
	bi.currentFile = bi.overflowFile
	return bucket, nil
}

// readBucket从当前文件中读取一个bucket。
func (bi *bucketIterator) readBucket() (*bucket, error) {
	// 通过offset从currentFile读取整个bucket，包括他的所有槽
	bucketBuf := make([]byte, bi.bucketSize)
	if _, err := bi.currentFile.ReadAt(bucketBuf, bi.offset); err != nil {
		return nil, err
	}

	b := &bucket{file: bi.currentFile, offset: bi.offset, bucketSize: bi.bucketSize}
	// 获取bucket内的所有槽
	for i := 0; i < slotsPerBucket; i++ {
		_ = bucketBuf[hashLen+bi.slotValueLen]
		// 前4字节为哈希
		b.slots[i].Hash = binary.LittleEndian.Uint32(bucketBuf[:hashLen])
		if b.slots[i].Hash != 0 {
			// 后slotValueLen为值
			b.slots[i].Value = bucketBuf[hashLen : hashLen+bi.slotValueLen]
		}
		// 移动到下一个槽位
		bucketBuf = bucketBuf[hashLen+bi.slotValueLen:]
	}

	// 最后8字节是下一个溢出bucket的偏移量。
	b.nextOffset = int64(binary.LittleEndian.Uint64(bucketBuf[:nextOffLen]))

	return b, nil
}

// insertSlot向槽写入器插入一个槽。
func (sw *slotWriter) insertSlot(sl Slot, t *Table) error {
	// 如果槽数量超过了31，创建一个新的溢出bucket并且连接到current bucket
	if sw.currentSlotIndex == slotsPerBucket {
		nextBucket, err := t.createOverflowBucket()
		if err != nil {
			return err
		}
		sw.currentBucket.nextOffset = nextBucket.offset
		sw.prevBuckets = append(sw.prevBuckets, sw.currentBucket)
		sw.currentBucket = nextBucket
		sw.currentSlotIndex = 0
	}

	sw.currentBucket.slots[sw.currentSlotIndex] = sl
	sw.currentSlotIndex++
	return nil
}

// writeSlots将槽写入器的所有slots写入bucket对应文件，即flush。
func (sw *slotWriter) writeSlots() error {
	for i := len(sw.prevBuckets) - 1; i >= 0; i-- {
		if err := sw.prevBuckets[i].write(); err != nil {
			return err
		}
	}
	return sw.currentBucket.write()
}

// write将bucket中的所有槽位写入文件。
func (b *bucket) write() error {
	buf := make([]byte, b.bucketSize)
	// 把所有槽写入buffer
	var index = 0
	for i := 0; i < slotsPerBucket; i++ {
		slot := b.slots[i]

		binary.LittleEndian.PutUint32(buf[index:index+hashLen], slot.Hash)
		copy(buf[index+hashLen:index+hashLen+len(slot.Value)], slot.Value)

		index += hashLen + len(slot.Value)
	}

	// 写入下一个溢出桶的offset
	binary.LittleEndian.PutUint64(buf[len(buf)-nextOffLen:], uint64(b.nextOffset))

	_, err := b.file.WriteAt(buf, b.offset)
	return err
}

// removeSlot从bucket中移除一个槽位，并将其后的所有槽位向前移动以填补空槽位。
func (b *bucket) removeSlot(slotIndex int) {
	i := slotIndex
	for ; i < slotsPerBucket-1; i++ {
		b.slots[i] = b.slots[i+1]
	}
	b.slots[i] = Slot{}
}
