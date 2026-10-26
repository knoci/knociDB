package knocidb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/knoci/knocidb/wal"
	"golang.org/x/sync/errgroup"
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

	// 压缩期间新 ValueLog 还没替换进 walFiles，但索引可能已经指向新位置。
	// 读路径通过 extraWALs 找到这些“临时可读”的新段（按分区隔离）。
	mu        sync.RWMutex
	extraWALs [][]*wal.WAL
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

	// 对象存储
	objectStorage bool
}

// 为值日志打开 WAL 文件，将打开多个 WAL 文件用于并发读写
// 为每个 WAL 初始化废弃表，构建 dpTable
// recoverValueLogTempFiles 处理压缩崩溃留下的 .temp 文件：
// - 正式文件还在：旧数据仍是权威版本，删除 temp；
// - 正式文件不在：说明崩溃发生在“删旧文件之后、rename 之前”，把 temp 提升为正式文件。
func recoverValueLogTempFiles(dirPath string, partition int) error {
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}
	suffix := fmt.Sprintf(tempValueLogFileExt, partition)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		tempPath := filepath.Join(dirPath, name)
		finalPath := filepath.Join(dirPath, strings.TrimSuffix(name, ".temp"))
		if _, statErr := os.Stat(finalPath); statErr == nil {
			if err := os.Remove(tempPath); err != nil {
				return err
			}
			continue
		}
		if err := os.Rename(tempPath, finalPath); err != nil {
			return err
		}
	}
	return nil
}

func openValueLog(options valueLogOptions) (*valueLog, error) {
	var walFiles []*wal.WAL
	var dpTables []*discardtable
	for i := 0; i < int(options.partitionNum); i++ {
		if err := recoverValueLogTempFiles(options.dirPath, i); err != nil {
			return nil, err
		}
		vLogWal, err := wal.Open(wal.Options{
			DirPath:        options.dirPath,
			SegmentSize:    options.segmentSize,
			SegmentFileExt: fmt.Sprintf(valueLogFileExt, i),
			Sync:           false, // 手动同步
			BytesPerSync:   0,     // 与 Sync 相同
			ObjectStorage:  options.objectStorage,
		})
		if err != nil {
			return nil, err
		}
		walFiles = append(walFiles, vLogWal)
		// 初始化 dpTable
		dpTable := newDiscardTable(i)
		dpTables = append(dpTables, dpTable)
	}

	return &valueLog{
		walFiles:      walFiles,
		dpTables:      dpTables,
		discardNumber: options.discardtableNumber,
		totalNumber:   options.totalNumber,
		options:       options}, nil
}

// addReadableWAL 注册压缩期间的临时 WAL，使索引更新后立刻可读。
func (vlog *valueLog) addReadableWAL(partition int, walFile *wal.WAL) {
	vlog.mu.Lock()
	for len(vlog.extraWALs) <= partition {
		vlog.extraWALs = append(vlog.extraWALs, nil)
	}
	vlog.extraWALs[partition] = append(vlog.extraWALs[partition], walFile)
	vlog.mu.Unlock()
}

func (vlog *valueLog) removeReadableWAL(partition int, walFile *wal.WAL) {
	vlog.mu.Lock()
	if partition < len(vlog.extraWALs) {
		list := vlog.extraWALs[partition]
		for i, w := range list {
			if w == walFile {
				vlog.extraWALs[partition] = append(list[:i], list[i+1:]...)
				break
			}
		}
	}
	vlog.mu.Unlock()
}

// 从指定位置读取值日志记录。
// 先读正式 WAL，失败时再尝试压缩期间注册的临时 WAL。
func (vlog *valueLog) read(pos *KeyPosition) (*ValueLogRecord, error) {
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	vlog.mu.RLock()
	var main *wal.WAL
	if int(pos.partition) < len(vlog.walFiles) {
		main = vlog.walFiles[pos.partition]
	}
	var extras []*wal.WAL
	if int(pos.partition) < len(vlog.extraWALs) {
		extras = append(extras, vlog.extraWALs[pos.partition]...)
	}
	vlog.mu.RUnlock()

	var firstErr error
	if main != nil {
		buf, err := main.Read(pos.position)
		if err == nil {
			record := decodeValueLogRecord(buf)
			if record == nil {
				return nil, fmt.Errorf("invalid value log record at %+v", pos.position)
			}
			return record, nil
		}
		firstErr = err
	}
	for _, w := range extras {
		if w == nil {
			continue
		}
		buf, err := w.Read(pos.position)
		if err == nil {
			record := decodeValueLogRecord(buf)
			if record == nil {
				return nil, fmt.Errorf("invalid value log record at %+v", pos.position)
			}
			return record, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, fmt.Errorf("value log partition %d not found", pos.partition)
}

// 将vlog记录写入，它将被写入相应的分区
func (vlog *valueLog) writeBatch(records []*ValueLogRecord) ([]*KeyPosition, error) {
	// 按分区对记录进行分组
	partitionRecords := make([][]*ValueLogRecord, vlog.options.partitionNum)
	vlog.totalNumber += uint32(len(records))
	for _, record := range records {
		// 获取并收集分区号。
		p := vlog.getKeyPartition(record.key)
		partitionRecords[p] = append(partitionRecords[p], record)
	}

	pChan := make(chan []*KeyPosition, vlog.options.partitionNum)
	g, ctx := errgroup.WithContext(context.Background())
	// 遍历处理每个分区
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if len(partitionRecords[i]) == 0 {
			continue
		}

		part := i
		// 协程异步写入，统一channel收集
		g.Go(func() error {
			var err error
			defer func() {
				if err != nil {
					// 清除wal日志写缓冲区。
					vlog.walFiles[part].ClearPendingWrites()
				}
			}()

			var keyPositions []*KeyPosition
			for _, record := range partitionRecords[part] {
				select {
				case <-ctx.Done():
					err = ctx.Err()
					return err
				default:
					// 写入wal日志写缓冲区。
					vlog.walFiles[part].PendingWrites(encodeValueLogRecord(record))
				}
			}
			// 全部写入并且获得在wal的segment中位置信息。
			positions, err := vlog.walFiles[part].WriteAll()
			if err != nil {
				return err
			}
			// 把在wal的segment中位置信息填入key在vlog中位置信息
			for i, pos := range positions {
				keyPositions = append(keyPositions, &KeyPosition{
					key:       partitionRecords[part][i].key,
					partition: uint32(part),
					uid:       partitionRecords[part][i].uid,
					position:  pos,
				})
			}
			pChan <- keyPositions
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(pChan)

	var keyPositions []*KeyPosition
	for pos := range pChan {
		keyPositions = append(keyPositions, pos...)
	}

	return keyPositions, nil
}

// 将值日志同步到磁盘
func (vlog *valueLog) sync() error {
	for _, walFile := range vlog.walFiles {
		if err := walFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// 关闭值日志
func (vlog *valueLog) close() error {
	for _, walFile := range vlog.walFiles {
		if err := walFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) getKeyPartition(key []byte) int {
	return int(vlog.options.hashKeyFunction(key) % uint64(vlog.options.partitionNum))
}

func (vlog *valueLog) setDiscard(partition uint32, id uuid.UUID) {
	vlog.dpTables[partition].addEntry(id)
	vlog.discardNumber++
}

func (vlog *valueLog) isDiscard(partition int, id uuid.UUID) bool {
	return vlog.dpTables[partition].existEntry(id)
}

func (vlog *valueLog) cleanDiscardTable() {
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		vlog.dpTables[i].clean()
	}
	vlog.totalNumber -= vlog.discardNumber
	vlog.discardNumber = 0
}
