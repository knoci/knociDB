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
	"github.com/knoci/knocidb/wal"
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
		mu       sync.RWMutex
		wal      *wal.WAL           // 预写日志
		skl      *arenaskl.Skiplist // 跳表
		options  memtableOptions
		shardID  uint32
		batchIDs map[uint64]struct{}
	}

	// memtableOptions 表示内存表的配置选项
	memtableOptions struct {
		dirPath         string // 预写日志 WAL 文件存储的位置
		tableID         uint32 // 内存表的唯一 ID，用于生成 WAL 文件名
		shardID         uint32 // 所属本地并发分区
		memSize         uint32 // 内存表的最大大小
		walBytesPerSync uint32 // 通过 BytesPerSync 参数将 WAL 文件刷新到磁盘
		walSync         bool   // WAL 在每次写入后立即刷新
	}
)

// openMemtables 扫描并打开每个分区的 WAL。
// applyAll 为 true 时兼容旧 WAL（没有 commit log）；否则只回放已提交批次。
func openMemtables(options Options, committed map[uint64]struct{}, applyAll bool) ([]*memtable, map[uint64]struct{}, error) {
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, nil, err
	}

	byShard := make(map[uint32][]int)
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
		shardID := tableShardID(uint32(id))
		if int(shardID) >= options.PartitionNum {
			continue
		}
		byShard[shardID] = append(byShard[shardID], id)
	}

	partitionNum := options.PartitionNum
	if partitionNum <= 0 {
		partitionNum = 1
	}
	tables := make([]*memtable, 0, partitionNum)
	seen := make(map[uint64]struct{})
	for shardID := 0; shardID < partitionNum; shardID++ {
		ids := byShard[uint32(shardID)]
		if len(ids) == 0 {
			ids = []int{int(makeTableID(uint32(shardID), initialTableID))}
		}
		sort.Ints(ids)
		for _, id := range ids {
			table, err := openMemtableWithCommitted(memtableOptions{
				dirPath:         options.DirPath,
				tableID:         uint32(id),
				shardID:         uint32(shardID),
				memSize:         options.MemtableSize,
				walSync:         options.Sync,
				walBytesPerSync: options.BytesPerSync,
			}, committed, applyAll, seen)
			if err != nil {
				return nil, nil, err
			}
			tables = append(tables, table)
		}
	}
	return tables, seen, nil
}

// openMemtable 打开一个内存表并回放全部已完成的批次（测试/旧格式入口）。
func openMemtable(options memtableOptions) (*memtable, error) {
	return openMemtableWithCommitted(options, nil, true, nil)
}

func openMemtableWithCommitted(options memtableOptions, committed map[uint64]struct{}, applyAll bool, seen map[uint64]struct{}) (*memtable, error) {
	skl := arenaskl.NewSkiplist(int64(float64(options.memSize) * 1.5))
	table := &memtable{
		options:  options,
		skl:      skl,
		shardID:  options.shardID,
		batchIDs: make(map[uint64]struct{}),
	}

	walFile, err := wal.Open(wal.Options{
		DirPath:        options.dirPath,
		SegmentSize:    math.MaxInt,
		SegmentFileExt: fmt.Sprintf(walFileExt, options.tableID),
		Sync:           options.walSync,
		BytesPerSync:   options.walBytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	table.wal = walFile

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
		if record == nil {
			return nil, fmt.Errorf("invalid log record in wal")
		}
		if record.Type == LogRecordBatchFinished {
			batchID, errParseBytes := snowflake.ParseBytes(record.Key)
			if errParseBytes != nil {
				return nil, errParseBytes
			}
			table.batchIDs[uint64(batchID)] = struct{}{}
			_, committedBatch := committed[uint64(batchID)]
			if seen != nil {
				seen[uint64(batchID)] = struct{}{}
			}
			singleShard := len(record.Value) == 1 && record.Value[0] == 1
			if applyAll || committedBatch || singleShard {
				for _, idxRecord := range indexRecords[uint64(batchID)] {
					table.skl.Put(y.KeyWithTs(idxRecord.Key, 0),
						y.ValueStruct{Value: idxRecord.Value, Meta: idxRecord.Type})
				}
			}
			delete(indexRecords, uint64(batchID))
		} else {
			indexRecords[record.BatchID] = append(indexRecords[record.BatchID], record)
		}
	}
	return table, nil
}

func (mt *memtable) prepareRecord(record *LogRecord, batchID snowflake.ID, shardCount int, options WriteOptions) error {
	if record.Type == LogRecordNormal && record.Value != nil && len(record.Value) == 0 {
		record.Type = LogRecordNormalEmpty
	}
	if options.DisableWal {
		return nil
	}
	mt.batchIDs[uint64(batchID)] = struct{}{}
	record.BatchID = uint64(batchID)
	mt.wal.PendingWrites(encodeLogRecord(record))
	finished := &LogRecord{Key: batchID.Bytes(), Type: LogRecordBatchFinished}
	if shardCount > 0 && shardCount <= 255 {
		finished.Value = []byte{byte(shardCount)}
	}
	mt.wal.PendingWrites(encodeLogRecord(finished))
	if _, err := mt.wal.WriteAll(); err != nil {
		return err
	}
	if options.Sync || mt.options.walSync {
		return mt.wal.Sync()
	}
	return nil
}

func (mt *memtable) applyRecord(record *LogRecord) {
	mt.mu.Lock()
	mt.skl.Put(y.KeyWithTs([]byte(record.Key), 0), y.ValueStruct{Value: record.Value, Meta: record.Type})
	mt.mu.Unlock()
}

// prepareBatch 把批次写入 WAL，但不修改 skiplist。
func (mt *memtable) prepareBatch(pendingWrites map[string]*LogRecord, batchID snowflake.ID, options WriteOptions, shardCount int) error {
	for _, record := range pendingWrites {
		if record.Type == LogRecordNormal && record.Value != nil && len(record.Value) == 0 {
			record.Type = LogRecordNormalEmpty
		}
	}
	if options.DisableWal {
		return nil
	}
	mt.batchIDs[uint64(batchID)] = struct{}{}
	for _, record := range pendingWrites {
		record.BatchID = uint64(batchID)
		mt.wal.PendingWrites(encodeLogRecord(record))
	}
	finished := &LogRecord{
		Key:  batchID.Bytes(),
		Type: LogRecordBatchFinished,
	}
	if shardCount > 0 && shardCount <= 255 {
		finished.Value = []byte{byte(shardCount)}
	}
	mt.wal.PendingWrites(encodeLogRecord(finished))
	if _, err := mt.wal.WriteAll(); err != nil {
		return err
	}
	if options.Sync || mt.options.walSync {
		return mt.wal.Sync()
	}
	return nil
}

// applyBatch 把已经写入 WAL 的批次应用到 skiplist。
func (mt *memtable) applyBatch(pendingWrites map[string]*LogRecord) {
	mt.mu.Lock()
	for key, record := range pendingWrites {
		mt.skl.Put(y.KeyWithTs([]byte(key), 0), y.ValueStruct{Value: record.Value, Meta: record.Type})
	}
	mt.mu.Unlock()
}

// putBatch 是 prepareBatch + applyBatch 的兼容入口。
func (mt *memtable) putBatch(pendingWrites map[string]*LogRecord, batchID snowflake.ID, options WriteOptions) error {
	if err := mt.prepareBatch(pendingWrites, batchID, options, 0); err != nil {
		return err
	}
	mt.applyBatch(pendingWrites)
	return nil
}

// 从内存表获取值 如果指定的键被标记为已删除，则返回true
func (mt *memtable) get(key []byte) (bool, []byte) {
	deleted, _, value := mt.getWithState(key)
	return deleted, value
}

// getWithState 返回 (deleted, empty, value)。
// empty 用于区分“键存在但 value 为空”和“键不存在”。
func (mt *memtable) getWithState(key []byte) (bool, bool, []byte) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	valueStruct := mt.skl.Get(y.KeyWithTs(key, 0))
	switch valueStruct.Meta {
	case LogRecordDeleted:
		return true, false, valueStruct.Value
	case LogRecordNormalEmpty:
		return false, true, valueStruct.Value
	default:
		return false, false, valueStruct.Value
	}
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
