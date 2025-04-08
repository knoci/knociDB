package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	initialSegmentFileID = 1
)

var (
	ErrValueTooLarge       = errors.New("the data size can't larger than segment size")
	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
)

// WAL 表示一个预写式日志结构，为传入的写入操作提供持久性 和容错能力。
// 它由 activeSegment（当前用于新写入的段文件）和 olderSegments （用于读取操作的段文件映射）组成。
//
// options 字段存储 WAL 的各种配置选项。
//
// mu sync.RWMutex 用于并发访问 WAL 数据结构， 确保安全访问和修改。
type WAL struct {
	activeSegment     *segment               // 活动段文件，用于新的写入操作
	olderSegments     map[SegmentID]*segment // 旧的段文件，仅用于读取
	options           Options
	mu                sync.RWMutex
	bytesWrite        uint32
	renameIds         []SegmentID
	pendingWrites     [][]byte
	pendingSize       int64
	pendingWritesLock sync.Mutex
	closeC            chan struct{}
	syncTicker        *time.Ticker
}

// Reader 表示 WAL 的读取器。
// 它由 segmentIters（按段 ID 排序的 segmentIter 切片）
// 和 currentIter（当前 segmentIter 在切片中的索引）组成。
//
// currentIter 字段用于遍历 segmentIters 切片。
type Reader struct {
	segmentIters []*segmentIter
	currentIter  int
}

// Open 使用给定的选项打开一个 WAL。
// 如果目录不存在，它会创建目录，并打开目录中的所有段文件。
// 如果目录中没有段文件，它会创建一个新的段文件。
func Open(options Options) (*WAL, error) {
	if !strings.HasPrefix(options.SegmentFileExt, ".") {
		return nil, fmt.Errorf("segment file extension must start with '.'")
	}
	wal := &WAL{
		options:       options,
		olderSegments: make(map[SegmentID]*segment),
		pendingWrites: make([][]byte, 0),
		closeC:        make(chan struct{}),
	}

	// 如果目录不存在则创建
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	// 遍历目录并打开所有段文件
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// 获取所有段文件 ID
	var segmentIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	// 空目录，仅初始化一个新的段文件
	if len(segmentIDs) == 0 {
		segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt, initialSegmentFileID)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		// 按顺序打开段文件，将最大的一个作为活动段文件
		sort.Ints(segmentIDs)

		for i, segId := range segmentIDs {
			segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt,
				uint32(segId))
			if err != nil {
				return nil, err
			}
			if i == len(segmentIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}

	// 仅当 SyncInterval 大于 0 时启动同步操作
	if wal.options.SyncInterval > 0 {
		wal.syncTicker = time.NewTicker(wal.options.SyncInterval)
		go func() {
			for {
				select {
				case <-wal.syncTicker.C:
					_ = wal.Sync()
				case <-wal.closeC:
					wal.syncTicker.Stop()
					return
				}
			}
		}()
	}

	return wal, nil
}

// SegmentFileName 返回段文件的文件名。
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// OpenNewActiveSegment 打开一个新的段文件
// 并将其设置为活动段文件。 当前活动段文件未满，但想要创建新的段文件时使用。
func (wal *WAL) OpenNewActiveSegment() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	// 同步活动段文件
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	// 创建新的段文件并将其设置为活动段文件
	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt,
		wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

// ActiveSegmentID 返回活动段文件的 ID。
func (wal *WAL) ActiveSegmentID() SegmentID {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return wal.activeSegment.id
}

// IsEmpty 返回 WAL 是否为空。
// 只有当只有一个空的活动段文件时，WAL 才是空的。
func (wal *WAL) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}

// SetIsStartupTraversal 仅在 WAL 启动遍历期间使用。
// 注意，如果将其设置为 true，则只有一个读取器可以从 WAL 读取数据
// （单线程）。
func (wal *WAL) SetIsStartupTraversal(v bool) {
	for _, seg := range wal.olderSegments {
		seg.isStartupTraversal = v
	}
	wal.activeSegment.isStartupTraversal = v
}

// NewReaderWithMax 返回一个新的 WAL 读取器，
func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// 获取所有段文件读取器。
	var segmentIters []*segmentIter
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewIter()
			segmentIters = append(segmentIters, reader)
		}
	}
	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewIter()
		segmentIters = append(segmentIters, reader)
	}

	// 按段 ID 对读取器进行排序。
	sort.Slice(segmentIters, func(i, j int) bool {
		return segmentIters[i].segment.id < segmentIters[j].segment.id
	})

	return &Reader{
		segmentIters: segmentIters,
		currentIter:  0,
	}
}

// NewReaderWithStart 返回一个新的 WAL 读取器，
// 该读取器只会从位置大于或等于给定位置的
// 段文件中读取数据。
func (wal *WAL) NewReaderWithStart(startPos *ChunkPosition) (*Reader, error) {
	if startPos == nil {
		return nil, errors.New("start position is nil")
	}
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	reader := wal.NewReader()
	for {
		// 跳过段 ID 小于给定位置段 ID 的读取器。
		if reader.CurrentSegmentId() < startPos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		// 跳过位置小于给定位置的数据块。
		currentPos := reader.CurrentChunkPosition()
		if currentPos.BlockNumber >= startPos.BlockNumber &&
			currentPos.ChunkOffset >= startPos.ChunkOffset {
			break
		}
		// 调用 Next 继续查找。
		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

// NewReader 返回一个新的 WAL 读取器。
// 它将遍历所有段文件并读取其中的所有数据。
func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

// Next 返回 WAL 中的下一个数据块及其位置。
// 如果没有数据，将返回 io.EOF。 该位置可用于从段文件中读取数据。
func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if r.currentIter >= len(r.segmentIters) {
		return nil, nil, io.EOF
	}

	data, position, err := r.segmentIters[r.currentIter].Next()
	if err == io.EOF {
		r.currentIter++
		return r.Next()
	}
	return data, position, err
}

// SkipCurrentSegment 在读取 WAL 时跳过当前段文件。
func (r *Reader) SkipCurrentSegment() {
	r.currentIter++
}

// CurrentSegmentId 返回读取 WAL 时当前段文件的 ID。
func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentIters[r.currentIter].segment.id
}

// CurrentChunkPosition 返回当前数据块的位置
func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentIters[r.currentIter]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		BlockNumber: reader.blockNumber,
		ChunkOffset: reader.chunkOffset,
	}
}

// ClearPendingWrites 清除待处理的写入并重置待处理大小
func (wal *WAL) ClearPendingWrites() {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	wal.pendingSize = 0
	wal.pendingWrites = wal.pendingWrites[:0]
}

// PendingWrites 将数据添加到 wal.pendingWrites 并等待批量写入。
// 如果 pendingWrites 中的数据超过一个段的大小，
// 将返回 'ErrPendingSizeTooLarge' 错误并清除 pendingWrites。
func (wal *WAL) PendingWrites(data []byte) {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	size := wal.maxDataWriteSize(int64(len(data)))
	wal.pendingSize += size
	wal.pendingWrites = append(wal.pendingWrites, data)
}

// rotateActiveSegment 创建一个新的段文件并替换活动段。
func (wal *WAL) rotateActiveSegment() error {
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	wal.bytesWrite = 0
	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt,
		wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

// WriteAll 将 wal.pendingWrites 写入 WAL 然后清除 pendingWrites，
// 它不会根据 wal.options 同步段文件，你应该手动调用 Sync()。
func (wal *WAL) WriteAll() ([]*ChunkPosition, error) {
	if len(wal.pendingWrites) == 0 {
		return make([]*ChunkPosition, 0), nil
	}

	wal.mu.Lock()
	defer func() {
		wal.ClearPendingWrites()
		wal.mu.Unlock()
	}()

	// 如果待处理大小仍然大于段大小，返回错误
	if wal.pendingSize > wal.options.SegmentSize {
		return nil, ErrPendingSizeTooLarge
	}

	// 如果活动段文件已满，同步它并创建一个新的。
	if wal.activeSegment.Size()+wal.pendingSize > wal.options.SegmentSize {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	// 将所有数据写入活动段文件。
	positions, err := wal.activeSegment.writeAll(wal.pendingWrites)
	if err != nil {
		return nil, err
	}

	return positions, nil
}

// Write 将数据写入 WAL。 实际上，它将数据写入活动段文件。
// 它返回数据在 WAL 中的位置，如果有错误则返回错误。
func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}
	// 如果活动段文件已满，同步它并创建一个新的。
	if wal.isFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	// 将数据写入活动段文件。
	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	// 更新 bytesWrite 字段。
	wal.bytesWrite += position.ChunkSize

	// 如果开启了Sync。
	var needSync = wal.options.Sync
	if !needSync && wal.options.BytesPerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytesPerSync
	}
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}

	return position, nil
}

// Read 根据给定的位置从 WAL 中读取数据。
func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// 根据位置信息找到段文件
	var segment *segment
	if pos.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentId]
	}

	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, wal.options.SegmentFileExt)
	}

	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

// Close 关闭 WAL。
func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	select {
	case <-wal.closeC:
	default:
		close(wal.closeC)
	}

	// 关闭所有段文件。
	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
		wal.renameIds = append(wal.renameIds, segment.id)
	}
	wal.olderSegments = nil

	wal.renameIds = append(wal.renameIds, wal.activeSegment.id)
	// 关闭活跃段。
	return wal.activeSegment.Close()
}

// Delete 删除 WAL 的所有段文件。
func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// 删除所有段文件。
	for _, segment := range wal.olderSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	// 删除活跃段。
	return wal.activeSegment.Remove()
}

// Sync 将活动段文件同步到稳定存储（如磁盘）。
func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

// RenameFileExt 重命名所有段文件的扩展名。
func (wal *WAL) RenameFileExt(ext string) error {
	if !strings.HasPrefix(ext, ".") {
		return fmt.Errorf("segment file extension must start with '.'")
	}
	wal.mu.Lock()
	defer wal.mu.Unlock()

	renameFile := func(id SegmentID) error {
		oldName := SegmentFileName(wal.options.DirPath, wal.options.SegmentFileExt, id)
		newName := SegmentFileName(wal.options.DirPath, ext, id)
		return os.Rename(oldName, newName)
	}

	for _, id := range wal.renameIds {
		if err := renameFile(id); err != nil {
			return err
		}
	}

	wal.options.SegmentFileExt = ext
	return nil
}

func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+wal.maxDataWriteSize(delta) > wal.options.SegmentSize
}

// maxDataWriteSize 计算可能得最大尺寸
// maximum size = max padding + (num_block + 1) * headerSize + dataSize
func (wal *WAL) maxDataWriteSize(size int64) int64 {
	return chunkHeaderSize + size + (size/blockSize+1)*chunkHeaderSize
}
