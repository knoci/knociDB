package knocidb

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"knocidb/wal"
)

const (
	valueLogFileExt = ".VLOG.%d"
)

// valueLog 值日志是基于 Wisckey 论文中的概念命名的
// 详情可以看doc中的Wisckey论文
type valueLog struct {
	walFiles      []*wal.WAL
	dpTables      []*discardtable
	discardNumber uint32
	totalNumber   uint32
	options       valueLogOptions
}

type valueLogOptions struct {
	// dirPath 指定 WAL 段文件存储的目录路径
	dirPath string

	// segmentSize 指定每个段文件的最大字节大小
	segmentSize int64

	// 值日志被分成多个部分用于并发读写
	partitionNum uint32

	// 用于分片的函数
	KeyFunction func([]byte) uint64

	// 在读取指定内存容量的条目后将有效条目写入磁盘
	compactBatchCapacity int

	// 已废弃的数量
	discardtableNumber uint32

	// 总数量
	totalNumber uint32
}

// 为值日志打开 WAL 文件，它将打开多个 WAL 文件用于并发读写
// WAL 文件的数量由 partitionNum 指定
// 为每个 WAL 初始化废弃表，构建 dpTable
func openValueLog(options valueLogOptions) (*valueLog, error) {
	var walFiles []*wal.WAL
	var dpTables []*discardtable
	for i := 0; i < int(options.partitionNum); i++ {
		vLogWal, err := wal.Open(wal.Options{
			DirPath:        options.dirPath,
			SegmentSize:    options.segmentSize,
			SegmentFileExt: fmt.Sprintf(valueLogFileExt, i),
			Sync:           false, // 手动同步
			BytesPerSync:   0,     // 与 Sync 相同
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

// 从指定位置读取值日志记录
func (vlog *valueLog) read(pos *KeyPosition) (*ValueLogRecord, error) {
	buf, err := vlog.walFiles[pos.partition].Read(pos.position)
	if err != nil {
		return nil, err
	}
	log := decodeValueLogRecord(buf)
	return log, nil
}

// 将值日志记录写入值日志，它将被分成多个分区并同时写入相应的分区
func (vlog *valueLog) writeBatch(records []*ValueLogRecord) ([]*KeyPosition, error) {
	// 按分区对记录进行分组
	partitionRecords := make([][]*ValueLogRecord, vlog.options.partitionNum)
	vlog.totalNumber += uint32(len(records))
	for _, record := range records {
		// 获取并收集分区号。
		p := vlog.getKeyPartition(record.key)
		partitionRecords[p] = append(partitionRecords[p], record)
	}

	// 用于接收写入值日志后，记录位置的通道
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

	// 获得记录的位置
	var keyPositions []*KeyPosition
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		pos := <-pChan
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
	// TODO 要先设计分区算法
	return 0
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
