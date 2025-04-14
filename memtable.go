package knocidb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/bwmarrin/snowflake"
	arenaskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"knocidb/wal"
)

const (
	// WAL 文件名格式为 .SEG.%d
	// %d 是内存表的唯一 ID，用于生成 WAL 文件名
	// 例如，ID 为 1 的内存表的 WAL 文件名是 .SEG.1
	walFileExt     = ".SEG.%d"
	initialTableID = 1
)

type (
	// memtable 在数据被刷新到索引和值日志之前保存数据
	// 刷新后数据最终会被持久化到两个地方：
	//		- 索引（B+树）：存储键和位置信息
	//		- 值日志（vlog）：存储实际的值数据
	memtable struct {
		mu      sync.RWMutex
		wal     *wal.WAL           // 预写日志
		skl     *arenaskl.Skiplist // 跳表
		options memtableOptions
	}

	// memtableOptions 表示内存表的配置选项
	memtableOptions struct {
		dirPath         string // 预写日志 WAL 文件存储的位置
		tableID         uint32 // 内存表的唯一 ID，用于生成 WAL 文件名
		memSize         uint32 // 内存表的最大大小
		walBytesPerSync uint32 // 通过 BytesPerSync 参数将 WAL 文件刷新到磁盘
		walSync         bool   // WAL 在每次写入后立即刷新
	}
)

// 打开WAL获取所有memtable
func openMemtables(options Options) ([]*memtable, error) {
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// 获取所有内存表 ID
	var tableIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		var prefix int
		_, err = fmt.Sscanf(entry.Name(), "%d"+walFileExt, &prefix, &id)
		if err != nil {
			continue
		}
		tableIDs = append(tableIDs, id)
	}

	// 默认表
	if len(tableIDs) == 0 {
		tableIDs = append(tableIDs, initialTableID)
	}

	sort.Ints(tableIDs)
	tables := make([]*memtable, len(tableIDs))
	for i, table := range tableIDs {
		table, errOpenMemtable := openMemtable(memtableOptions{
			dirPath:         options.DirPath,
			tableID:         uint32(table),
			memSize:         options.MemtableSize,
			walSync:         options.Sync,
			walBytesPerSync: options.BytesPerSync,
		})
		if errOpenMemtable != nil {
			return nil, errOpenMemtable
		}
		tables[i] = table
	}
	return tables, nil
}

// memtable持有一个wal(预写日志)，所以当打开一个memtable时，
// 实际上是打开相应的wal文件。并从wal加载所有条目以重建跳表的内容。
func openMemtable(options memtableOptions) (*memtable, error) {
	// 初始化跳表
	skl := arenaskl.NewSkiplist(int64(float64(options.memSize) * 1.5))
	table := &memtable{
		options: options,
		skl:     skl,
	}

	// 打开预写日志文件
	walFile, err := wal.Open(wal.Options{
		DirPath:        options.dirPath,
		SegmentSize:    math.MaxInt, // 无限制，确保一个wal文件只包含一个段文件
		SegmentFileExt: fmt.Sprintf(walFileExt, options.tableID),
		Sync:           options.walSync,
		BytesPerSync:   options.walBytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	table.wal = walFile

	// 从wal加载所有条目,以重建跳表的内容
	indexRecords := make(map[uint64][]*LogRecord)
	reader := table.wal.NewReader()
	for {
		segment, _, errNext := reader.Next()
		if errNext != nil {
			if errors.Is(errNext, io.EOF) {
				break
			}
			return nil, errNext
		}
		record := decodeLogRecord(segment)
		if record.Type == LogRecordBatchFinished {
			batchID, errParseBytes := snowflake.ParseBytes(record.Key)
			if errParseBytes != nil {
				return nil, errParseBytes
			}
			// 批次处理
			for _, idxRecord := range indexRecords[uint64(batchID)] {
				table.skl.Put(y.KeyWithTs(idxRecord.Key, 0),
					y.ValueStruct{Value: idxRecord.Value, Meta: idxRecord.Type})
			}
			delete(indexRecords, uint64(batchID))
		} else {
			indexRecords[record.BatchID] = append(indexRecords[record.BatchID], record)
		}
	}

	// 成功打开并读取WAL文件，返回内存表
	return table, nil
}

// putBatch 将一批LogRecord写入内存表。
func (mt *memtable) putBatch(pendingWrites map[string]*LogRecord, batchID snowflake.ID, options WriteOptions) error {
	// 如果WAL未禁用，首先写入WAL以确保持久性和原子性
	if !options.DisableWal {
		// 将记录添加到wal.pendingWrites，这相当于wal的一个写缓冲区
		for _, record := range pendingWrites {
			record.BatchID = uint64(batchID)
			encRecord := encodeLogRecord(record)
			mt.wal.PendingWrites(encRecord)
		}

		// 添加一条记录表示批处理的结束
		endRecord := encodeLogRecord(&LogRecord{
			Key:  batchID.Bytes(),
			Type: LogRecordBatchFinished,
		})
		mt.wal.PendingWrites(endRecord)

		// 写入wal.pendingWrites
		if _, err := mt.wal.WriteAll(); err != nil {
			return err
		}
		// 如有必要，刷新WAL
		if options.Sync && !mt.options.walSync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}
	// 上锁，多线程
	mt.mu.Lock()
	// 写入内存中的跳表
	for key, record := range pendingWrites {
		mt.skl.Put(y.KeyWithTs([]byte(key), 0), y.ValueStruct{Value: record.Value, Meta: record.Type})
	}
	mt.mu.Unlock()

	return nil
}

// 从内存表获取值 如果指定的键被标记为已删除，则返回true
func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	valueStruct := mt.skl.Get(y.KeyWithTs(key, 0))
	deleted := valueStruct.Meta == LogRecordDeleted
	return deleted, valueStruct.Value
}

func (mt *memtable) isFull() bool {
	return mt.skl.MemSize() >= int64(mt.options.memSize)
}

func (mt *memtable) deleteWAl() error {
	if mt.wal != nil {
		return mt.wal.Delete()
	}
	return nil
}

func (mt *memtable) close() error {
	if mt.wal != nil {
		return mt.wal.Close()
	}
	return nil
}

func (mt *memtable) sync() error {
	if mt.wal != nil {
		return mt.wal.Sync()
	}
	return nil
}

// memtableIterator 内存表迭代器，基于UniIterator
type memtableIterator struct {
	options IteratorOptions
	iter    *arenaskl.UniIterator
}

func newMemtableIterator(options IteratorOptions, memtable *memtable) *memtableIterator {
	return &memtableIterator{
		options: options,
		iter:    memtable.skl.NewUniIterator(options.Reverse),
	}
}

// Rewind 查找迭代器中的第一个键。
func (mi *memtableIterator) Rewind() {
	mi.iter.Rewind()
	if len(mi.options.Prefix) == 0 {
		return
	}
	// 前缀扫描
	for mi.iter.Valid() && !bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix) {
		mi.iter.Next()
	}
}

// Seek 将迭代器移动到大于（当reverse为true时小于）或等于指定键的键。
func (mi *memtableIterator) Seek(key []byte) {
	mi.iter.Seek(y.KeyWithTs(key, 0))
	if len(mi.options.Prefix) == 0 {
		return
	}
	// 前缀扫描
	for mi.Valid() && !bytes.HasPrefix(mi.Key(), mi.options.Prefix) {
		mi.Next()
	}
}

// Next 将迭代器移动到下一个键。
func (mi *memtableIterator) Next() {
	mi.iter.Next()
	if len(mi.options.Prefix) == 0 {
		return
	}
	// 前缀扫描
	for mi.iter.Valid() && !bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix) {
		mi.iter.Next()
	}
}

// Key 获取当前键。
func (mi *memtableIterator) Key() []byte {
	return y.ParseKey(mi.iter.Key())
}

// Value 获取当前值。
func (mi *memtableIterator) Value() any {
	return mi.iter.Value()
}

// Valid 返回迭代器是否已耗尽。
func (mi *memtableIterator) Valid() bool {
	return mi.iter.Valid()
}

// Close 关闭迭代器。
func (mi *memtableIterator) Close() error {
	return mi.iter.Close()
}
