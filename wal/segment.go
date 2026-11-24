package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/valyala/bytebufferpool"
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed           = errors.New("the segment file is closed")
	ErrInvalidCRC       = errors.New("invalid crc, the data may be corrupted")
	ErrInvalidChunkType = errors.New("invalid chunk type, the data may be corrupted")
)

const (
	// 7 字节检验和
	// 校验和 长度 类型
	//   4    2    1
	chunkHeaderSize = 7

	// 32 KB的block大小
	blockSize = 32 * KB

	// 文件权限
	fileModePerm = 0644

	// uin32 + uint32 + int64 + uin32
	// 段ID + 块号 + 块内偏移量 + 块大小
	maxLen = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// Segment 表示 WAL 中的单个段文件。
// 段文件是只追加的，数据以块的形式写入。
// 每个块大小为 32KB，数据以块内分片的形式写入。
type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	committedSize      atomic.Int64
	closed             atomic.Bool
	ioMu               sync.RWMutex
	header             []byte
}

// segmentIter 是用于遍历段文件中的所有数据的迭代器。
// 每个迭代器持有独立的 32KB 流式块缓存，顺序遍历时每个物理块仅触发一次 pread 系统调用。
type segmentIter struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
	blockBuf    []byte
	cachedBlock int64
	cachedSize  int64
}

// ChunkPosition 表示在段文件中的位置信息。 用于从段文件中读取范围数据。
type ChunkPosition struct {
	// SegmentID标记唯一段
	SegmentId SegmentID
	// BlockNumber 数据块在段文件中的块号
	BlockNumber uint32
	// ChunkOffset 数据块在段文件中的起始偏移量
	ChunkOffset int64
	// ChunkSize 数据块在段文件中占用的字节数
	ChunkSize uint32
}

var zeroPadding [chunkHeaderSize]byte

// block池，池化 *[]byte 指针避免 Put 时切片头逃逸装箱。
var blockPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, blockSize)
		return &b
	},
}

func getBuffer() *[]byte {
	return blockPool.Get().(*[]byte)
}

func putBuffer(buf *[]byte) {
	if buf != nil && len(*buf) == blockSize {
		blockPool.Put(buf)
	}
}

// openSegmentFile 打开一个新的段文件。
func openSegmentFile(dirPath, extName string, id uint32) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	// 设置当前块号和块大小
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err)
	}

	seg := &segment{
		id:                 id,
		fd:                 fd,
		header:             make([]byte, chunkHeaderSize),
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}
	seg.committedSize.Store(offset)
	return seg, nil
}

// NewIter 创建一个新的段文件读取器。
func (seg *segment) NewIter() *segmentIter {
	return &segmentIter{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
		cachedBlock: -1,
	}
}

// recoverTail 截断文件尾部不完整的 chunk。
// 崩溃可能留下“头部完整但数据不足”或只有半个 chunk 头的尾部；
// 仅对文件尾部的残缺/全零尾块进行截断，段中间的 CRC/类型/IO 损坏会直接报错而不静默丢弃后续数据。
func (seg *segment) recoverTail() error {
	if seg.closed.Load() {
		return ErrClosed
	}
	size := seg.Size()
	if size == 0 {
		return nil
	}

	iter := seg.NewIter()
	var lastEnd int64
	for {
		next, err := iter.nextSkipPayload()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			if errors.Is(err, ErrInvalidCRC) || errors.Is(err, ErrInvalidChunkType) {
				// 检查是否为尾部残余（例如崩溃留下的最后一条损毁记录或尾随全零填充）。
				// 如果失败记录之后仍有非零数据，说明是段中间损坏，必须报错而非静默截断。
				if seg.isTailCorruption(iter.blockNumber, iter.chunkOffset, size) {
					break
				}
				return fmt.Errorf("segment %d corruption at offset %d: %w",
					seg.id, int64(iter.blockNumber)*blockSize+iter.chunkOffset, err)
			}
			return err
		}
		end := int64(next.BlockNumber)*blockSize + next.ChunkOffset
		if end <= lastEnd {
			break
		}
		lastEnd = end
		iter.blockNumber = next.BlockNumber
		iter.chunkOffset = next.ChunkOffset
	}

	if lastEnd < size {
		// Windows 上以 O_APPEND 打开的句柄可能不允许 fd.Truncate，改用路径级 Truncate。
		if err := os.Truncate(seg.fd.Name(), lastEnd); err != nil {
			return err
		}
		if err := seg.fd.Sync(); err != nil {
			return err
		}
		seg.currentBlockNumber = uint32(lastEnd / blockSize)
		seg.currentBlockSize = uint32(lastEnd % blockSize)
		seg.committedSize.Store(lastEnd)
	}
	return nil
}

// isTailCorruption 判断从 (blockNumber, chunkOffset) 开始的损坏是否位于文件尾部
// （即损坏记录直达文件末尾，或剩余字节全为 0）。
func (seg *segment) isTailCorruption(blockNumber uint32, chunkOffset int64, segSize int64) bool {
	failPos := int64(blockNumber)*blockSize + chunkOffset
	if failPos >= segSize {
		return true
	}
	// 如果剩余字节全为 0（常见于预分配或崩溃尾零），属于尾部无效数据。
	if seg.allZeroRange(failPos, segSize) {
		return true
	}
	// 读取失败 chunk 的头，看其声明的结束位置是否已经延伸到文件末尾（无后续记录）。
	var hdr [chunkHeaderSize]byte
	if failPos+chunkHeaderSize > segSize {
		return true
	}
	if _, err := seg.fd.ReadAt(hdr[:], failPos); err != nil {
		return false
	}
	length := int64(binary.LittleEndian.Uint16(hdr[4:6]))
	chunkEnd := failPos + chunkHeaderSize + length
	if chunkEnd >= segSize {
		return true
	}
	// 若 chunkEnd 之后全是 0，同样可视为尾部损坏。
	return seg.allZeroRange(chunkEnd, segSize)
}

func (seg *segment) allZeroRange(start, end int64) bool {
	bufPtr := getBuffer()
	defer putBuffer(bufPtr)
	buf := *bufPtr
	for pos := start; pos < end; {
		n := end - pos
		if n > int64(len(buf)) {
			n = int64(len(buf))
		}
		readN, err := seg.fd.ReadAt(buf[:n], pos)
		if err != nil && !errors.Is(err, io.EOF) {
			return false
		}
		for i := 0; i < readN; i++ {
			if buf[i] != 0 {
				return false
			}
		}
		if readN == 0 {
			break
		}
		pos += int64(readN)
	}
	return true
}

// Sync 将段文件刷新到磁盘。
func (seg *segment) Sync() error {
	seg.ioMu.RLock()
	defer seg.ioMu.RUnlock()
	if seg.closed.Load() {
		return nil
	}
	return seg.fd.Sync()
}

// Remove 删除段文件。
func (seg *segment) Remove() error {
	seg.ioMu.Lock()
	defer seg.ioMu.Unlock()
	if !seg.closed.Swap(true) {
		if err := seg.fd.Close(); err != nil {
			return err
		}
	}
	return os.Remove(seg.fd.Name())
}

// Close 关闭段文件。
func (seg *segment) Close() error {
	seg.ioMu.Lock()
	defer seg.ioMu.Unlock()
	if seg.closed.Swap(true) {
		return nil
	}
	return seg.fd.Close()
}

// Size 返回段文件的已提交大小（并发安全）。
func (seg *segment) Size() int64 {
	return seg.committedSize.Load()
}

// writeToBuffer 计算数据的块位置，将数据写入字节缓冲池，更新段文件状态
// 数据将以块的形式写入，块有四种类型：
// ChunkTypeFull（完整块）, ChunkTypeFirst（首块）, ChunkTypeMiddle（中间块）, ChunkTypeLast（末块）。
//
// 每个块都有一个头部，头部包含长度、类型和校验和。 块的有效载荷是你想要写入的实际数据。
func (seg *segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)

	if seg.closed.Load() {
		return nil, ErrClosed
	}

	// 如果剩余的块大小不足以容纳块头部，则填充该块（零分配静态零数组）
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		// 必要时进行填充
		if seg.currentBlockSize < blockSize {
			padLen := blockSize - seg.currentBlockSize
			chunkBuffer.B = append(chunkBuffer.B, zeroPadding[:padLen]...)
			padding += padLen
			// 切换到新块
			seg.currentBlockNumber += 1
			seg.currentBlockSize = 0
		}
	}

	// 获取段目前块的位置
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}

	dataSize := uint32(len(data))
	// 如果整个数据可以填入块
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		seg.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		// 如果数据大小超过块大小，数据将分批写入块中。
		var (
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = seg.currentBlockSize
		)

		for leftSize > 0 {
			// 可用空间，如果可用空间大于剩余数据，则只需要写入剩余数据大小
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}
			// 已处理的数据长度
			var end = dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}

			// 将数据块追加到缓冲区
			var chunkType ChunkType
			switch leftSize {
			case dataSize: // 第一个数据块
				chunkType = ChunkTypeFirst
			case chunkSize: // 最后一个数据块
				chunkType = ChunkTypeLast
			default: // 中间数据块
				chunkType = ChunkTypeMiddle
			}
			seg.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)

			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}

	// 缓冲区长度必须等于数据块大小加填充长度
	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		return nil, fmt.Errorf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen)
	}

	// 更新段状态
	seg.currentBlockSize += position.ChunkSize
	if seg.currentBlockSize >= blockSize {
		seg.currentBlockNumber += seg.currentBlockSize / blockSize
		seg.currentBlockSize = seg.currentBlockSize % blockSize
	}

	return position, nil
}

// writeAll 将批量数据写入段文件。
func (seg *segment) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if seg.closed.Load() {
		return nil, ErrClosed
	}

	// 如果发生任何错误，恢复段文件状态
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize
	originOffset := int64(originBlockNumber)*blockSize + int64(originBlockSize)

	// 初始化数据块缓冲区
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	// 如果出错，就回滚内存偏移并截断可能部分写入的文件尾部
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
			seg.committedSize.Store(originOffset)
			_ = os.Truncate(seg.fd.Name(), originOffset)
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// 将所有数据写入数据块缓冲区（预扩容避免大批量触发多次 runtime.growslice）
	var est int
	for _, rec := range data {
		est += len(rec) + chunkHeaderSize*(len(rec)/(blockSize-chunkHeaderSize)+2)
	}
	if cap(chunkBuffer.B) < est {
		chunkBuffer.B = make([]byte, 0, est)
	}
	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = seg.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	// 将数据块缓冲区写入段文件
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	seg.committedSize.Store(int64(seg.currentBlockNumber)*blockSize + int64(seg.currentBlockSize))
	return
}

// writeToBufferNoPos 与 writeToBuffer 等价，但不分配 *ChunkPosition 堆对象，专供 WAL 顺序追加热路径使用。
func (seg *segment) writeToBufferNoPos(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (uint32, error) {
	if seg.closed.Load() {
		return 0, ErrClosed
	}
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		if seg.currentBlockSize < blockSize {
			padLen := blockSize - seg.currentBlockSize
			chunkBuffer.B = append(chunkBuffer.B, zeroPadding[:padLen]...)
			seg.currentBlockNumber += 1
			seg.currentBlockSize = 0
		}
	}

	dataSize := uint32(len(data))
	var chunkBytes uint32
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		seg.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		chunkBytes = dataSize + chunkHeaderSize
	} else {
		var (
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = seg.currentBlockSize
		)
		for leftSize > 0 {
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}
			end := dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}
			var chunkType ChunkType
			switch leftSize {
			case dataSize:
				chunkType = ChunkTypeFirst
			case chunkSize:
				chunkType = ChunkTypeLast
			default:
				chunkType = ChunkTypeMiddle
			}
			seg.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)
			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		chunkBytes = blockCount*chunkHeaderSize + dataSize
	}

	seg.currentBlockSize += chunkBytes
	if seg.currentBlockSize >= blockSize {
		seg.currentBlockNumber += seg.currentBlockSize / blockSize
		seg.currentBlockSize = seg.currentBlockSize % blockSize
	}
	return chunkBytes, nil
}

// writeAllNoPos 批量写入段文件，预扩容 chunkBuffer 且跳过 []*ChunkPosition 分配。
func (seg *segment) writeAllNoPos(data [][]byte, estBytes int64) (written uint32, err error) {
	if seg.closed.Load() {
		return 0, ErrClosed
	}

	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize
	originOffset := int64(originBlockNumber)*blockSize + int64(originBlockSize)

	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
			seg.committedSize.Store(originOffset)
			_ = os.Truncate(seg.fd.Name(), originOffset)
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	if estBytes > 0 && int64(cap(chunkBuffer.B)) < estBytes {
		chunkBuffer.B = make([]byte, 0, estBytes)
	}
	for i := 0; i < len(data); i++ {
		var n uint32
		n, err = seg.writeToBufferNoPos(data[i], chunkBuffer)
		if err != nil {
			return 0, err
		}
		written += n
	}
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return 0, err
	}
	seg.committedSize.Store(int64(seg.currentBlockNumber)*blockSize + int64(seg.currentBlockSize))
	return written, nil
}

// Write 将数据写入段文件。
func (seg *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if seg.closed.Load() {
		return nil, ErrClosed
	}

	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize
	originOffset := int64(originBlockNumber)*blockSize + int64(originBlockSize)

	// 初始化数据块缓冲区
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	// 如果出错，就回滚内存偏移并截断可能部分写入的文件尾部
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
			seg.committedSize.Store(originOffset)
			_ = os.Truncate(seg.fd.Name(), originOffset)
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// 将所有数据写入数据块缓冲区
	pos, err = seg.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}
	// 将数据块缓冲区写入段文件
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	seg.committedSize.Store(int64(seg.currentBlockNumber)*blockSize + int64(seg.currentBlockSize))

	return
}

func (seg *segment) appendChunkBuffer(buf *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	// 长度	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(len(data)))
	// 类型	1 Byte	index:6
	seg.header[6] = chunkType
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(seg.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)

	// 添加头部和数据到缓冲区
	buf.B = append(buf.B, seg.header...)
	buf.B = append(buf.B, data...)
}

// 将待处理的数据块缓冲区写入段文件
func (seg *segment) writeChunkBuffer(buf *bytebufferpool.ByteBuffer) error {
	if seg.currentBlockSize > blockSize {
		return errors.New("the current block size exceeds the maximum block size")
	}

	// 将数据写入底层文件
	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// Read 根据块号和数据块偏移量从段文件中读取数据。
func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := seg.readInternal(blockNumber, chunkOffset, nil, 0, true)
	return value, err
}

// readAtPos 供 WAL.Read 点读使用：
// 1. 优先命中共享不可变满块缓存（零系统调用）；
// 2. 若未开启缓存且 ChunkSize 明确落在单个块内，仅精准读取该 chunk 字节（消除 512x 读放大）；
// 3. 否则走分块读取并回填满块缓存。
func (seg *segment) readAtPos(pos *ChunkPosition, cache *blockCache) ([]byte, error) {
	if cache == nil && pos.ChunkSize > chunkHeaderSize && pos.ChunkOffset+int64(pos.ChunkSize) <= blockSize {
		val, ok, err := seg.readSingleChunkExact(pos.BlockNumber, pos.ChunkOffset, int64(pos.ChunkSize))
		if ok || err != nil {
			return val, err
		}
	}
	value, _, err := seg.readInternal(pos.BlockNumber, pos.ChunkOffset, cache, pos.ChunkSize, true)
	return value, err
}

func (seg *segment) readSingleChunkExact(blockNumber uint32, chunkOffset, chunkSize int64) ([]byte, bool, error) {
	seg.ioMu.RLock()
	defer seg.ioMu.RUnlock()

	if seg.closed.Load() {
		return nil, false, ErrClosed
	}
	segSize := seg.Size()
	fileOffset := int64(blockNumber)*blockSize + chunkOffset
	if fileOffset >= segSize {
		return nil, false, io.EOF
	}
	if fileOffset+chunkSize > segSize {
		return nil, false, nil // 回退常规读取以精确区分 EOF / ErrUnexpectedEOF
	}

	buf := make([]byte, chunkSize)
	if _, err := seg.fd.ReadAt(buf, fileOffset); err != nil {
		return nil, false, err
	}
	length := binary.LittleEndian.Uint16(buf[4:6])
	if int64(length)+chunkHeaderSize != chunkSize {
		return nil, false, nil // ChunkSize 可能是迭代器估算值，回退常规读取
	}
	if buf[6] != ChunkTypeFull {
		return nil, false, nil
	}
	checksum := crc32.ChecksumIEEE(buf[4:])
	savedSum := binary.LittleEndian.Uint32(buf[:4])
	if savedSum != checksum {
		return nil, false, ErrInvalidCRC
	}
	return buf[chunkHeaderSize:], true, nil
}

func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64, cache *blockCache, hintSize uint32, collectPayload bool) ([]byte, *ChunkPosition, error) {
	return seg.readInternalWithStreamBuf(blockNumber, chunkOffset, cache, hintSize, collectPayload, nil, nil, nil)
}

func (seg *segment) readInternalWithStreamBuf(
	blockNumber uint32,
	chunkOffset int64,
	cache *blockCache,
	hintSize uint32,
	collectPayload bool,
	streamBuf []byte,
	cachedBlock *int64,
	cachedSize *int64,
) ([]byte, *ChunkPosition, error) {
	seg.ioMu.RLock()
	defer seg.ioMu.RUnlock()

	if seg.closed.Load() {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		poolPtr   *[]byte
		block     []byte
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
		isFirst   = true
	)
	if collectPayload && hintSize > chunkHeaderSize {
		result = make([]byte, 0, hintSize-chunkHeaderSize)
	}

	defer func() {
		if poolPtr != nil {
			putBuffer(poolPtr)
		}
	}()

	for {
		size := int64(blockSize)
		offset := int64(blockNumber) * blockSize
		if size+offset > segSize {
			size = segSize - offset
		}

		if chunkOffset >= size {
			if !isFirst {
				return nil, nil, io.ErrUnexpectedEOF
			}
			return nil, nil, io.EOF
		}
		if chunkOffset+chunkHeaderSize > size {
			return nil, nil, io.ErrUnexpectedEOF
		}

		if streamBuf != nil && cachedBlock != nil && cachedSize != nil {
			block = streamBuf
			if *cachedBlock != int64(blockNumber) || *cachedSize < size {
				if _, err := seg.fd.ReadAt(block[0:size], offset); err != nil {
					return nil, nil, err
				}
				*cachedBlock = int64(blockNumber)
				*cachedSize = size
			}
		} else if size == blockSize && cache != nil {
			if cached, ok := cache.get(seg.id, blockNumber); ok {
				block = cached
			} else {
				if poolPtr == nil {
					poolPtr = getBuffer()
				}
				block = *poolPtr
				if _, err := seg.fd.ReadAt(block[0:size], offset); err != nil {
					return nil, nil, err
				}
				cache.put(seg.id, blockNumber, block)
			}
		} else {
			if poolPtr == nil {
				poolPtr = getBuffer()
			}
			block = *poolPtr
			if _, err := seg.fd.ReadAt(block[0:size], offset); err != nil {
				return nil, nil, err
			}
		}

		// 头部
		header := block[chunkOffset : chunkOffset+chunkHeaderSize]

		// 长度
		length := binary.LittleEndian.Uint16(header[4:6])

		// 切片复制数据
		start := chunkOffset + chunkHeaderSize
		checksumEnd := start + int64(length)
		if checksumEnd > size {
			return nil, nil, io.ErrUnexpectedEOF
		}

		// 校验和
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// 类型状态机校验：Full | First -> Middle* -> Last
		chunkType := header[6]
		if isFirst {
			if chunkType != ChunkTypeFull && chunkType != ChunkTypeFirst {
				return nil, nil, ErrInvalidChunkType
			}
		} else {
			if chunkType != ChunkTypeMiddle && chunkType != ChunkTypeLast {
				return nil, nil, ErrInvalidChunkType
			}
		}

		if collectPayload {
			result = append(result, block[start:checksumEnd]...)
		}

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			// 如果这是块中的最后一个数据块，且剩余的块空间
			// 都是填充，则下一个数据块应该在下一个块中。
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		isFirst = false
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

func (segReader *segmentIter) ensureBuf() {
	if segReader.blockBuf == nil {
		segReader.blockBuf = make([]byte, blockSize)
		segReader.cachedBlock = -1
		segReader.cachedSize = 0
	}
}

// nextSkipPayload 供 recoverTail 使用：仅校验 chunk 头部与 CRC，零负载内存分配，且按 32KB 块流式缓存。
func (segReader *segmentIter) nextSkipPayload() (*ChunkPosition, error) {
	if segReader.segment.closed.Load() {
		return nil, ErrClosed
	}
	segReader.ensureBuf()
	_, nextChunk, err := segReader.segment.readInternalWithStreamBuf(
		segReader.blockNumber,
		segReader.chunkOffset,
		nil,
		0,
		false,
		segReader.blockBuf,
		&segReader.cachedBlock,
		&segReader.cachedSize,
	)
	return nextChunk, err
}

// Next 返回下一个数据块。
func (segReader *segmentIter) Next() ([]byte, *ChunkPosition, error) {
	// 段文件已关闭
	if segReader.segment.closed.Load() {
		return nil, nil, ErrClosed
	}
	segReader.ensureBuf()

	// 此位置描述当前数据块信息
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}

	value, nextChunk, err := segReader.segment.readInternalWithStreamBuf(
		segReader.blockNumber,
		segReader.chunkOffset,
		nil,
		0,
		true,
		segReader.blockBuf,
		&segReader.cachedBlock,
		&segReader.cachedSize,
	)
	if err != nil {
		return nil, nil, err
	}

	// 计算数据块大小。
	// 数据块大小只是一个估计值，不是准确值
	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))

	// 更新位置
	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

// Encode 将数据块位置编码为字节切片。 返回实际占用元素的切片。
func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

// EncodeFixedSize 将数据块位置编码为字节切片。 返回大小为 "maxLen" 的切片。
func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

// 将数据块位置编码为字节切片。
func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)

	var index = 0
	// SegmentId
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	if shrink {
		return buf[:index]
	}
	return buf
}

// DecodeChunkPosition 从字节切片解码数据块位置。
func DecodeChunkPosition(buf []byte) *ChunkPosition {
	pos, _ := DecodeChunkPositionWithSize(buf)
	return pos
}

// DecodeChunkPositionWithSize 解码数据块位置并返回消耗的字节数，供索引值附加元数据（如 ExpireAt）使用。
func DecodeChunkPositionWithSize(buf []byte) (*ChunkPosition, int) {
	if len(buf) == 0 {
		return nil, 0
	}

	var index = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, 0
	}
	index += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, 0
	}
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, 0
	}
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, 0
	}
	index += n

	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}, index
}
