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
	discardNumber uint64
	totalNumber   uint64
	options       valueLogOptions

	statsMu sync.Mutex
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
	discardtableNumber uint64

	// 总数量
	totalNumber uint64
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

// activeSegmentNames 返回每个分区当前活跃段文件名，
// 冷段上传必须跳过这些仍可能继续追加的文件。
func (vlog *valueLog) activeSegmentNames() map[string]struct{} {
	out := make(map[string]struct{}, len(vlog.walFiles))
	for i, walFile := range vlog.walFiles {
		ext := fmt.Sprintf(valueLogFileExt, i)
		name := filepath.Base(wal.SegmentFileName(vlog.options.dirPath, ext, walFile.ActiveSegmentID()))
		out[name] = struct{}{}
	}
	return out
}

func openValueLog(options valueLogOptions) (*valueLog, error) {
	// 清理上次崩溃可能遗留的压缩暂存目录。
	if entries, err := os.ReadDir(options.dirPath); err == nil {
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), ".vlog-compact-") {
				_ = os.RemoveAll(filepath.Join(options.dirPath, entry.Name()))
			}
		}
	}

	var walFiles []*wal.WAL
	var dpTables []*discardtable
	closeOpened := func() {
		for _, w := range walFiles {
			_ = w.Close()
		}
	}
	for i := 0; i < int(options.partitionNum); i++ {
		if err := recoverValueLogTempFiles(options.dirPath, i); err != nil {
			closeOpened()
			return nil, err
		}
		vLogWal, err := wal.Open(wal.Options{
			DirPath:        options.dirPath,
			SegmentSize:    options.segmentSize,
			SegmentFileExt: fmt.Sprintf(valueLogFileExt, i),
			Sync:           false, // 手动同步
			BytesPerSync:   0,     // 与 Sync 相同
			BlockCacheSize: 16 * wal.MB,
		})
		if err != nil {
			closeOpened()
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

// 从指定位置读取值日志记录。
func (vlog *valueLog) read(pos *KeyPosition) (*ValueLogRecord, error) {
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	if int(pos.partition) >= len(vlog.walFiles) || vlog.walFiles[pos.partition] == nil {
		return nil, fmt.Errorf("value log partition %d not found", pos.partition)
	}
	buf, err := vlog.walFiles[pos.partition].Read(pos.position)
	if err != nil {
		return nil, err
	}
	record := decodeValueLogRecord(buf)
	if record == nil {
		return nil, fmt.Errorf("invalid value log record at %+v", pos.position)
	}
	return record, nil
}

// 将vlog记录写入，它将被写入相应的分区
func (vlog *valueLog) writeBatch(records []*ValueLogRecord) ([]*KeyPosition, error) {
	if len(records) == 0 {
		return nil, nil
	}
	// 按分区对记录进行分组
	partitionRecords := make([][]*ValueLogRecord, vlog.options.partitionNum)
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
			encoded := make([][]byte, 0, len(partitionRecords[part]))
			for _, record := range partitionRecords[part] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					encoded = append(encoded, encodeValueLogRecord(record))
				}
			}
			// 使用无状态 WriteBatch 写入，避免 PendingWrites/WriteAll 状态共享竞态；
			// 若单批超过段大小，降级为逐条写入以自动轮转段文件。
			positions, err := vlog.walFiles[part].WriteBatch(encoded)
			if err != nil {
				if err == wal.ErrPendingSizeTooLarge {
					positions = make([]*wal.ChunkPosition, 0, len(encoded))
					for _, data := range encoded {
						pos, werr := vlog.walFiles[part].Write(data)
						if werr != nil {
							return werr
						}
						positions = append(positions, pos)
					}
				} else {
					return err
				}
			}
			keyPositions := make([]*KeyPosition, 0, len(positions))
			for idx, pos := range positions {
				keyPositions = append(keyPositions, &KeyPosition{
					key:       partitionRecords[part][idx].key,
					partition: uint32(part),
					uid:       partitionRecords[part][idx].uid,
					position:  pos,
					expireAt:  partitionRecords[part][idx].expireAt,
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

	vlog.statsMu.Lock()
	vlog.totalNumber += uint64(len(records))
	vlog.statsMu.Unlock()

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
	vlog.statsMu.Lock()
	vlog.dpTables[partition].addEntry(id)
	vlog.discardNumber++
	vlog.statsMu.Unlock()
}

func (vlog *valueLog) isDiscard(partition int, id uuid.UUID) bool {
	return vlog.dpTables[partition].existEntry(id)
}

func (vlog *valueLog) cleanDiscardTable() {
	vlog.statsMu.Lock()
	defer vlog.statsMu.Unlock()
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		vlog.dpTables[i].clean()
	}
	if vlog.totalNumber >= vlog.discardNumber {
		vlog.totalNumber -= vlog.discardNumber
	} else {
		vlog.totalNumber = 0
	}
	vlog.discardNumber = 0
}

func (vlog *valueLog) stats() (uint64, uint64) {
	vlog.statsMu.Lock()
	defer vlog.statsMu.Unlock()
	return vlog.totalNumber, vlog.discardNumber
}
