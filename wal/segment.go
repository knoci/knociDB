package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

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
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
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
	closed             bool
	header             []byte
	startupBlock       *startupBlock
	isStartupTraversal bool
}

// segmentIter 是用于遍历段文件中的所有数据的迭代器。
// 你可以调用 Next 获取下一个数据块，
// 当没有数据时会返回 io.EOF。
type segmentIter struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
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

// 启动遍历时只有一个读取器（单个 goroutine），
// 因此我们可以复用一个块来完成整个遍历过程,以避免额外内存分配。
type startupBlock struct {
	block       []byte
	blockNumber int64
}

// block池，用于作为block缓冲区
var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

func getBuffer() []byte {
	return blockPool.Get().([]byte)
}

func putBuffer(buf []byte) {
	blockPool.Put(buf)
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
		return nil, fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err)
	}

	return &segment{
		id:                 id,
		fd:                 fd,
		header:             make([]byte, chunkHeaderSize),
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
		startupBlock: &startupBlock{
			block:       make([]byte, blockSize),
			blockNumber: -1,
		},
		isStartupTraversal: false,
	}, nil
}

// NewIter 创建一个新的段文件读取器。
func (seg *segment) NewIter() *segmentIter {
	return &segmentIter{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// Sync 将段文件刷新到磁盘。
func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

// Remove 删除段文件。
func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		if err := seg.fd.Close(); err != nil {
			return err
		}
	}
	return os.Remove(seg.fd.Name())
}

// Close 关闭段文件。
func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true
	return seg.fd.Close()
}

// Size 返回段文件的大小。
func (seg *segment) Size() int64 {
	size := int64(seg.currentBlockNumber) * int64(blockSize)
	return size + int64(seg.currentBlockSize)
}

// writeToBuffer 计算数据的块位置，将数据写入字节缓冲池，更新段文件状态
// 数据将以块的形式写入，块有四种类型：
// ChunkTypeFull（完整块）, ChunkTypeFirst（首块）, ChunkTypeMiddle（中间块）, ChunkTypeLast（末块）。
//
// 每个块都有一个头部，头部包含长度、类型和校验和。 块的有效载荷是你想要写入的实际数据。
func (seg *segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)

	if seg.closed {
		return nil, ErrClosed
	}

	// 如果剩余的块大小不足以容纳块头部，则填充该块
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		// 必要时进行填充
		if seg.currentBlockSize < blockSize {
			p := make([]byte, blockSize-seg.currentBlockSize)
			chunkBuffer.B = append(chunkBuffer.B, p...)
			padding += blockSize - seg.currentBlockSize
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
	if seg.closed {
		return nil, ErrClosed
	}

	// 如果发生任何错误，恢复段文件状态
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	// 初始化数据块缓冲区
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	// 如果出错，就回滚
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// 将所有数据写入数据块缓冲区
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
	return
}

// Write 将数据写入段文件。
func (seg *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}

	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	// 初始化数据块缓冲区
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	// 如果出错，就回滚
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
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

	// 写入后缓存的块不能再次重用。
	seg.startupBlock.blockNumber = -1
	return nil
}

// Read 根据块号和数据块偏移量从段文件中读取数据。
func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := seg.readInternal(blockNumber, chunkOffset)
	return value, err
}

func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		block     []byte
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
	)

	if seg.isStartupTraversal {
		// 复用startupBlock
		block = seg.startupBlock.block
	} else {
		// 请求block内存池
		block = getBuffer()
		if len(block) != blockSize {
			block = make([]byte, blockSize)
		}
		// 释放
		defer putBuffer(block)
	}

	for {
		size := int64(blockSize)
		offset := int64(blockNumber) * blockSize
		if size+offset > segSize {
			size = segSize - offset
		}

		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		if seg.isStartupTraversal {
			// 有两种情况我们需要从文件中读取块：
			// 1. 获取的块不是缓存的块
			// 2. 新的写入追加到块中，且块仍小于 32KB，由于新的写入，我们必须再次读取它。
			if seg.startupBlock.blockNumber != int64(blockNumber) || size != blockSize {
				// 从指定偏移量处读取段文件中的块。
				_, err := seg.fd.ReadAt(block[0:size], offset)
				if err != nil {
					return nil, nil, err
				}
				// 记住该块
				seg.startupBlock.blockNumber = int64(blockNumber)
			}
		} else {
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
		result = append(result, block[start:start+int64(length)]...)

		// 校验和
		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// 类型
		chunkType := header[6]

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
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

// Next 返回下一个数据块。
func (segReader *segmentIter) Next() ([]byte, *ChunkPosition, error) {
	// 段文件已关闭
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}

	// 此位置描述当前数据块信息
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}

	value, nextChunk, err := segReader.segment.readInternal(
		segReader.blockNumber,
		segReader.chunkOffset,
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
	if len(buf) == 0 {
		return nil
	}

	var index = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
