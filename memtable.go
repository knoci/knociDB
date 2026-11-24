package knocidb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/knoci/knocidb/internal/arenaskl"
	"github.com/knoci/knocidb/wal"
	"github.com/valyala/bytebufferpool"
)

const (
	// WAL 文件名格式：
	// 新格式：.SEG.<shardID>.<localSeq>（支持每分片 40 亿+ 次轮转，无 16-bit 回绕串分片风险）
	// 兼容旧格式：.SEG.<legacyTableID>
	walFileExt       = ".SEG.%d.%d"
	legacyWalFileExt = ".SEG.%d"
	initialTableID   = 1
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
		tableID         uint64 // 内存表的唯一 ID（高 32 位 shardID | 低 32 位 localSeq）
		shardID         uint32 // 所属本地并发分区
		memSize         uint32 // 内存表的最大大小
		walBytesPerSync uint32 // 通过 BytesPerSync 参数将 WAL 文件刷新到磁盘
		walSync         bool   // WAL 在每次写入后立即刷新
		legacyExt       bool   // 仅在兼容打开旧版 .SEG.<id> 文件时为 true
		legacyID        uint32 // 旧版文件后缀 ID
	}
)

type parsedMemWAL struct {
	shardID   uint32
	tableID   uint64
	legacyExt bool
	legacyID  uint32
}

func parseMemtableWALName(name string) (parsedMemWAL, bool) {
	idx := strings.Index(name, ".SEG.")
	if idx <= 0 {
		return parsedMemWAL{}, false
	}
	if _, err := strconv.ParseUint(name[:idx], 10, 64); err != nil {
		return parsedMemWAL{}, false
	}
	rest := name[idx+len(".SEG."):]
	if dot := strings.IndexByte(rest, '.'); dot >= 0 {
		shardVal, err1 := strconv.ParseUint(rest[:dot], 10, 32)
		seqVal, err2 := strconv.ParseUint(rest[dot+1:], 10, 32)
		if err1 != nil || err2 != nil {
			return parsedMemWAL{}, false
		}
		shardID := uint32(shardVal)
		return parsedMemWAL{
			shardID: shardID,
			tableID: makeTableID(shardID, uint32(seqVal)),
		}, true
	}
	rawID, err := strconv.ParseUint(rest, 10, 32)
	if err != nil {
		return parsedMemWAL{}, false
	}
	legacyID := uint32(rawID)
	shardID := legacyID >> legacyTableIDShardShift
	localID := legacyID & 0xffff
	return parsedMemWAL{
		shardID:   shardID,
		tableID:   makeTableID(shardID, localID),
		legacyExt: true,
		legacyID:  legacyID,
	}, true
}

// openMemtables 扫描并打开每个分区的 WAL。
// applyAll 为 true 时兼容旧 WAL（没有 commit log）；否则只回放已提交批次。
func openMemtables(options Options, committed map[uint64]struct{}, applyAll bool) ([]*memtable, map[uint64]struct{}, error) {
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, nil, err
	}

	byShard := make(map[uint32][]parsedMemWAL)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		parsed, ok := parseMemtableWALName(entry.Name())
		if !ok {
			continue
		}
		if int(parsed.shardID) >= options.PartitionNum {
			continue
		}
		byShard[parsed.shardID] = append(byShard[parsed.shardID], parsed)
	}

	partitionNum := options.PartitionNum
	if partitionNum <= 0 {
		partitionNum = 1
	}
	tables := make([]*memtable, 0, partitionNum)
	closeTables := func() {
		for _, t := range tables {
			_ = t.close()
		}
	}
	seen := make(map[uint64]struct{})
	for shardID := 0; shardID < partitionNum; shardID++ {
		items := byShard[uint32(shardID)]
		if len(items) == 0 {
			items = []parsedMemWAL{{
				shardID: uint32(shardID),
				tableID: makeTableID(uint32(shardID), initialTableID),
			}}
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].tableID < items[j].tableID
		})
		for _, item := range items {
			table, err := openMemtableWithCommitted(memtableOptions{
				dirPath:         options.DirPath,
				tableID:         item.tableID,
				shardID:         uint32(shardID),
				memSize:         options.MemtableSize,
				walSync:         options.Sync,
				walBytesPerSync: options.BytesPerSync,
				legacyExt:       item.legacyExt,
				legacyID:        item.legacyID,
			}, committed, applyAll, seen)
			if err != nil {
				closeTables()
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

func (opts memtableOptions) walSegmentExt() string {
	if opts.legacyExt {
		return fmt.Sprintf(legacyWalFileExt, opts.legacyID)
	}
	return fmt.Sprintf(walFileExt, opts.shardID, tableLocalID(opts.tableID))
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
		SegmentFileExt: options.walSegmentExt(),
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
			batchID, errParseBytes := strconv.ParseUint(string(record.Key), 10, 64)
			if errParseBytes != nil {
				return nil, errParseBytes
			}
			singleShard := len(record.Value) == 1 && record.Value[0] == 1
			if !singleShard {
				table.batchIDs[batchID] = struct{}{}
			}
			_, committedBatch := committed[batchID]
			if seen != nil && !singleShard {
				seen[batchID] = struct{}{}
			}
			if applyAll || committedBatch || singleShard {
				for _, idxRecord := range indexRecords[batchID] {
					table.skl.Put(idxRecord.Key, 0, arenaskl.ValueStruct{
						Value:    idxRecord.Value,
						Meta:     idxRecord.Type,
						ExpireAt: idxRecord.ExpireAt,
					})
				}
			}
			delete(indexRecords, batchID)
		} else {
			indexRecords[record.BatchID] = append(indexRecords[record.BatchID], record)
		}
	}
	return table, nil
}

func (mt *memtable) prepareRecord(record *LogRecord, batchID uint64, shardCount int, options WriteOptions) error {
	if record.Type == LogRecordNormal && record.Value != nil && len(record.Value) == 0 {
		record.Type = LogRecordNormalEmpty
	}
	if options.DisableWal {
		return nil
	}
	if shardCount != 1 {
		mt.batchIDs[batchID] = struct{}{}
	}
	record.BatchID = batchID
	var idBuf [24]byte
	var shardVal [1]byte
	finished := LogRecord{Key: strconv.AppendUint(idBuf[:0], batchID, 10), Type: LogRecordBatchFinished}
	if shardCount > 0 && shardCount <= 255 {
		shardVal[0] = byte(shardCount)
		finished.Value = shardVal[:]
	}

	encBuf := bytebufferpool.Get()
	encBuf.Reset()
	need := maxLogRecordHeaderSize*2 + len(record.Key) + len(record.Value) + len(finished.Key) + 1
	if cap(encBuf.B) < need {
		encBuf.B = make([]byte, 0, need)
	}
	encBuf.B = appendEncodedLogRecord(encBuf.B, record)
	split := len(encBuf.B)
	encBuf.B = appendEncodedLogRecord(encBuf.B, &finished)
	var recs [2][]byte
	recs[0] = encBuf.B[:split]
	recs[1] = encBuf.B[split:]
	err := mt.wal.AppendBatch(recs[:])
	bytebufferpool.Put(encBuf)
	if err != nil {
		return err
	}
	if options.Sync || mt.options.walSync {
		return mt.wal.Sync()
	}
	return nil
}

func (mt *memtable) applyRecord(record *LogRecord, seq uint64) {
	mt.applyRecordWithMode(record, seq, false)
}

func (mt *memtable) applyRecordWithMode(record *LogRecord, seq uint64, canReplace bool) {
	mt.skl.PutOrReplace(record.Key, seq, arenaskl.ValueStruct{
		Value:    record.Value,
		Meta:     record.Type,
		ExpireAt: record.ExpireAt,
	}, canReplace)
}

// prepareBatch 把批次写入 WAL，但不修改 skiplist。
func (mt *memtable) prepareBatch(pendingWrites map[string]*LogRecord, batchID uint64, options WriteOptions, shardCount int) error {
	totalNeed := maxLogRecordHeaderSize + 32
	for _, record := range pendingWrites {
		if record.Type == LogRecordNormal && record.Value != nil && len(record.Value) == 0 {
			record.Type = LogRecordNormalEmpty
		}
		totalNeed += maxLogRecordHeaderSize + len(record.Key) + len(record.Value)
	}
	if options.DisableWal {
		return nil
	}
	if shardCount != 1 {
		mt.batchIDs[batchID] = struct{}{}
	}
	encBuf := bytebufferpool.Get()
	encBuf.Reset()
	if cap(encBuf.B) < totalNeed {
		encBuf.B = make([]byte, 0, totalNeed)
	}
	offsets := make([]int, 0, len(pendingWrites)+2)
	offsets = append(offsets, 0)
	for _, record := range pendingWrites {
		record.BatchID = batchID
		encBuf.B = appendEncodedLogRecord(encBuf.B, record)
		offsets = append(offsets, len(encBuf.B))
	}
	var idBuf [24]byte
	var shardVal [1]byte
	finished := LogRecord{
		Key:  strconv.AppendUint(idBuf[:0], batchID, 10),
		Type: LogRecordBatchFinished,
	}
	if shardCount > 0 && shardCount <= 255 {
		shardVal[0] = byte(shardCount)
		finished.Value = shardVal[:]
	}
	encBuf.B = appendEncodedLogRecord(encBuf.B, &finished)
	offsets = append(offsets, len(encBuf.B))

	encoded := make([][]byte, len(offsets)-1)
	for i := 0; i < len(encoded); i++ {
		encoded[i] = encBuf.B[offsets[i]:offsets[i+1]]
	}
	err := mt.wal.AppendBatch(encoded)
	bytebufferpool.Put(encBuf)
	if err != nil {
		return err
	}
	if options.Sync || mt.options.walSync {
		return mt.wal.Sync()
	}
	return nil
}

// applyBatch 把已经写入 WAL 的批次应用到 skiplist。
func (mt *memtable) applyBatch(pendingWrites map[string]*LogRecord, seq uint64) {
	mt.applyBatchWithMode(pendingWrites, seq, false)
}

func (mt *memtable) applyBatchWithMode(pendingWrites map[string]*LogRecord, seq uint64, canReplace bool) {
	for key, record := range pendingWrites {
		k := record.Key
		if len(k) == 0 && len(key) > 0 {
			k = []byte(key)
		}
		mt.skl.PutOrReplace(k, seq, arenaskl.ValueStruct{
			Value:    record.Value,
			Meta:     record.Type,
			ExpireAt: record.ExpireAt,
		}, canReplace)
	}
}

// putBatch 是 prepareBatch + applyBatch 的兼容入口。
func (mt *memtable) putBatch(pendingWrites map[string]*LogRecord, batchID uint64, options WriteOptions) error {
	if err := mt.prepareBatch(pendingWrites, batchID, options, 0); err != nil {
		return err
	}
	mt.applyBatch(pendingWrites, 0)
	return nil
}

// getEntry 无锁读取指定键的最新 ValueStruct（包含 Meta 与 ExpireAt）。
func (mt *memtable) getEntry(key []byte) (arenaskl.ValueStruct, bool) {
	return mt.skl.Get(key, 0)
}

// 从内存表获取值 如果指定的键被标记为已删除，则返回true
func (mt *memtable) get(key []byte) (bool, []byte) {
	deleted, _, value := mt.getWithState(key)
	return deleted, value
}

// getWithState 返回 (deleted, empty, value)。
// empty 用于区分“键存在但 value 为空”和“键不存在”。
func (mt *memtable) getWithState(key []byte) (bool, bool, []byte) {
	valueStruct, found := mt.skl.Get(key, 0)
	if !found {
		return false, false, nil
	}
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

// memtableIterator 内存表 MVCC 迭代器，支持无锁快照读与 O(log N) 前缀定位。
type memtableIterator struct {
	options IteratorOptions
	iter    *arenaskl.Iterator
	valid   bool
}

func newMemtableIterator(options IteratorOptions, memtable *memtable, readSeq uint64) *memtableIterator {
	return &memtableIterator{
		options: options,
		iter:    memtable.skl.NewIterator(options.Reverse, readSeq),
	}
}

// Rewind 查找迭代器中的第一个键（前缀扫描采用 O(log N) Seek 定位）。
func (mi *memtableIterator) Rewind() {
	if len(mi.options.Prefix) == 0 {
		mi.iter.Rewind()
		mi.valid = mi.iter.Valid()
		return
	}
	if mi.options.Reverse {
		upper := prefixUpperBound(mi.options.Prefix)
		if upper == nil {
			mi.iter.Rewind()
		} else {
			mi.iter.Seek(upper)
			if mi.iter.Valid() && bytes.Equal(mi.iter.Key(), upper) {
				mi.iter.Next()
			}
		}
		mi.valid = mi.iter.Valid() && bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix)
		return
	}
	mi.iter.Seek(mi.options.Prefix)
	mi.valid = mi.iter.Valid() && bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix)
}

// Seek 将迭代器移动到大于（当reverse为true时小于）或等于指定键的键。
func (mi *memtableIterator) Seek(key []byte) {
	if len(mi.options.Prefix) == 0 {
		mi.iter.Seek(key)
		mi.valid = mi.iter.Valid()
		return
	}
	if mi.options.Reverse {
		mi.iter.Seek(key)
		if !mi.iter.Valid() || bytes.Compare(mi.iter.Key(), mi.options.Prefix) < 0 {
			mi.valid = false
			return
		}
		if !bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix) {
			mi.Rewind()
			if mi.valid && bytes.Compare(mi.iter.Key(), key) > 0 {
				mi.valid = false
			}
			return
		}
		mi.valid = true
		return
	}
	target := key
	if bytes.Compare(target, mi.options.Prefix) < 0 {
		target = mi.options.Prefix
	}
	mi.iter.Seek(target)
	mi.valid = mi.iter.Valid() && bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix)
}

// Next 将迭代器移动到下一个键（离开前缀范围立即停止）。
func (mi *memtableIterator) Next() {
	if !mi.valid {
		return
	}
	mi.iter.Next()
	if !mi.iter.Valid() {
		mi.valid = false
		return
	}
	if len(mi.options.Prefix) > 0 && !bytes.HasPrefix(mi.iter.Key(), mi.options.Prefix) {
		mi.valid = false
		return
	}
	mi.valid = true
}

// Key 获取当前键。
func (mi *memtableIterator) Key() []byte {
	return mi.iter.Key()
}

// Value 获取当前值。
func (mi *memtableIterator) Value() any {
	return mi.iter.Value()
}

// Valid 返回迭代器是否已耗尽。
func (mi *memtableIterator) Valid() bool {
	return mi.valid && mi.iter.Valid()
}

// Close 关闭迭代器。
func (mi *memtableIterator) Close() error {
	mi.valid = false
	return mi.iter.Close()
}
