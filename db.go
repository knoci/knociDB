package knocidb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/knoci/knocidb/diskhash"
	"github.com/knoci/knocidb/wal"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/gofrs/flock"
)

const (
	fileLockName    = "FLOCK"
	discardMetaName = "DSCMETA"
)

type DB struct {
	activeMem        *memtable           // 用于写入的活跃内存表
	immuMems         []*memtable         // 不可变内存表，等待刷新到磁盘
	index            Index               // 多分区索引，用于存储键和块位置
	vlog             *valueLog           // 值日志
	bloomManager     *BloomFilterManager // 布隆过滤器管理器
	fileLock         *flock.Flock        // 文件锁，防止多个进程使用相同的数据库目录
	flushChan        chan *memtable      // 用于通知刷新协程将内存表刷新到磁盘
	flushLock        sync.Mutex          // 刷新锁，防止在压缩未发生时进行刷新
	diskIO           *DiskIO             // 监控磁盘的 IO 状态并在适当时允许自动压缩
	compactChan      chan discardState   // 用于通知需要压缩的分片
	mu               sync.RWMutex
	closed           bool
	closeOnce        sync.Once
	closeErr         error
	bgWG             sync.WaitGroup // 自动压缩/IO 监控协程
	closeflushChan   chan struct{}  // 用于优雅地关闭刷新监听协程
	closeCompactChan chan struct{}  // 用于优雅地关闭自动压缩监听协程
	options          Options
	batchPool        sync.Pool // 批处理池，用于减少内存分配开销

	objectTier     *objectTier   // 对象存储冷层/备份层
	objectTierStop chan struct{} // 停止冷段后台上传
	objectTierWG   sync.WaitGroup

	// 记录打开过的 memtable，Close 时兜底关闭，避免 flush 竞态遗留 WAL 句柄。
	memtables map[uint32]*memtable
}

// Open 使用指定的选项打开数据库。
// 如果数据库目录不存在，将自动创建。
//
// 多个进程不能同时使用相同的数据库目录，
// 否则将返回 ErrDatabaseIsUsing 错误。
//
// 它将首先打开 WAL 以重建内存表，然后打开索引和值日志。
// 如果成功则返回 DB 对象，否则返回错误。
func Open(options Options) (*DB, error) {
	// 检查所有选项是否有效
	if err := validateOptions(&options); err != nil {
		return nil, err
	}

	// 如果数据目录不存在则创建
	if _, err := os.Stat(options.DirPath); err != nil {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 对象存储冷层：在打开 DB 前先创建，配置错误直接失败。
	tier, err := newObjectTier(options.ObjectTier)
	if err != nil {
		return nil, fmt.Errorf("init object tier failed: %w", err)
	}
	if tier != nil {
		defer func() {
			// 只有成功把 tier 交给 DB 时才不关闭；失败路径避免泄漏。
			if tier != nil {
				_ = tier.Close()
			}
		}()
	}

	// 创建文件锁，防止多个进程使用相同的数据库目录
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}
	opened := false
	defer func() {
		if !opened {
			_ = fileLock.Unlock()
		}
	}()

	// 持锁后再恢复，避免两个进程同时导入同一快照。
	if err := restoreFromObjectTier(options); err != nil {
		return nil, err
	}

	// 如果不存在则创建废弃元数据文件，读取废弃数量
	discardMetaPath := filepath.Join(options.DirPath, discardMetaName)
	discardNumber, totalEntryNumber, err := loadDiscardEntryMeta(discardMetaPath)
	if err != nil {
		return nil, err
	}

	// 打开所有内存表
	memtables, err := openMemtables(options)
	if err != nil {
		return nil, err
	}

	// 打开索引
	index, err := openIndex(indexOptions{
		indexType:       options.IndexType,
		dirPath:         options.DirPath,
		partitionNum:    options.PartitionNum,
		keyHashFunction: options.KeyHashFunction,
	})
	if err != nil {
		return nil, err
	}

	// 打开值日志
	vlog, err := openValueLog(valueLogOptions{
		dirPath:              options.DirPath,
		segmentSize:          options.ValueLogFileSize,
		partitionNum:         uint32(options.PartitionNum),
		hashKeyFunction:      options.KeyHashFunction,
		compactBatchCapacity: options.CompactBatchCapacity,
		discardtableNumber:   discardNumber,
		totalNumber:          totalEntryNumber,
	})
	if err != nil {
		return nil, err
	}

	// 初始化磁盘IO
	diskIO := new(DiskIO)
	diskIO.targetPath = options.DirPath
	diskIO.samplingInterval = options.DiskIOSamplingInterval
	diskIO.windowSize = options.DiskIOSamplingWindow
	diskIO.busyRate = options.DiskIOBusyRate
	diskIO.Init()

	// 初始化布隆过滤器管理器
	var bloomManager *BloomFilterManager
	if options.EnableBloomFilter {
		bloomOptions := BloomFilterOptions{
			ExpectedElements:    options.BloomFilterExpectedElements,
			FalsePositiveRate:   options.BloomFilterFalsePositiveRate,
			DirPath:             options.DirPath,
			PartitionNum:        options.PartitionNum,
			keyHashFunction:     options.KeyHashFunction,
			SaveIntervalSeconds: options.BloomFilterSaveIntervalSeconds,
			SaveChangeThreshold: options.BloomFilterSaveChangeThreshold,
		}
		bloomManager = NewBloomFilterManager(bloomOptions)
	}

	memtableMap := make(map[uint32]*memtable, len(memtables))
	for _, table := range memtables {
		memtableMap[table.options.tableID] = table
	}

	db := &DB{
		activeMem:        memtables[len(memtables)-1],
		immuMems:         memtables[:len(memtables)-1],
		index:            index,
		vlog:             vlog,
		bloomManager:     bloomManager,
		fileLock:         fileLock,
		flushChan:        make(chan *memtable, options.MemtableNums-1),
		compactChan:      make(chan discardState, 1),
		closeflushChan:   make(chan struct{}),
		closeCompactChan: make(chan struct{}),
		diskIO:           diskIO,
		options:          options,
		batchPool:        sync.Pool{New: makeBatch},
		memtables:        memtableMap,
	}

	if tier != nil {
		db.objectTier = tier
		db.objectTierStop = make(chan struct{})
		tier = nil // 所有权交给 DB
		db.startObjectTierUploader()
	}

	// 重建布隆过滤器：不能相信可能过期的 .BLOOM 文件，否则会产生假阴性
	if err := db.rebuildBloomFilters(); err != nil {
		// 失败时保持未就绪，TestKey 会返回 true，保证正确性
		log.Printf("rebuild bloom filters failed: %v", err)
	}

	// 如果在打开数据库时有一些不可变内存表，将它们刷新到磁盘
	if len(db.immuMems) > 0 {
		for _, table := range db.immuMems {
			db.flushMemtable(table)
		}
	}

	// 异步启动刷新内存表的协程，
	// 当活跃内存表已满时，带有新写入的内存表将被刷新到磁盘。
	go db.listenMemtableFlush()

	if options.AutoCompactSupport {
		// 异步启动自动压缩协程，
		// 监听废弃表状态，并自动压缩。
		db.bgWG.Add(1)
		go func() {
			defer db.bgWG.Done()
			db.listenAutoCompact()
		}()

		// 启动磁盘IO监控，
		// 在繁忙时阻塞低阈值压缩操作。
		if options.EnableDiskIO {
			db.bgWG.Add(1)
			go func() {
				defer db.bgWG.Done()
				db.listenDiskIOState()
			}()
		}
	}

	opened = true
	return db, nil
}

// rebuildBloomFilters 从索引/ValueLog 重建布隆过滤器。
func (db *DB) rebuildBloomFilters() error {
	if db.bloomManager == nil {
		return nil
	}
	db.bloomManager.ClearAll()

	switch idx := db.index.(type) {
	case *BPTree:
		if err := idx.forEachKey(func(key []byte) error {
			db.bloomManager.AddKey(key)
			return nil
		}); err != nil {
			return err
		}
	case *HashTable:
		// 哈希索引没有有序游标；扫描 ValueLog 中所有 key 更保守（可能多，但不会漏）。
		for _, walFile := range db.vlog.walFiles {
			reader := walFile.NewReader()
			for {
				chunk, _, err := reader.Next()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					return err
				}
				record := decodeValueLogRecord(chunk)
				if record != nil {
					db.bloomManager.AddKey(record.key)
				}
			}
		}
	default:
		return fmt.Errorf("unsupported index type for bloom rebuild")
	}

	db.bloomManager.MarkReady()
	return nil
}

func (db *DB) startObjectTierUploader() {
	if db.objectTier == nil {
		return
	}
	db.objectTierWG.Add(1)
	go func() {
		defer db.objectTierWG.Done()
		db.uploadColdSegments(true)
		ticker := time.NewTicker(db.objectTier.opts.UploadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-db.objectTierStop:
				return
			case <-ticker.C:
				db.uploadColdSegments(true)
			}
		}
	}()
}

func (db *DB) uploadColdSegments(skipActive bool) {
	if db.objectTier == nil || db.vlog == nil {
		return
	}
	var active map[string]struct{}
	if skipActive {
		db.mu.RLock()
		active = db.vlog.activeSegmentNames()
		db.mu.RUnlock()
	}
	n, err := db.objectTier.UploadColdSegments(db.options.DirPath, active)
	if err != nil {
		log.Printf("upload cold segments failed: %v", err)
		return
	}
	if n > 0 {
		log.Printf("uploaded %d cold segment(s) to object storage", n)
	}
}

// Close 关闭数据库，关闭所有数据文件并释放文件锁。
// 将closed标志设置为true。
// 关闭后不能再使用DB实例。
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.closeErr = db.closeInternal()
	})
	return db.closeErr
}

func (db *DB) closeInternal() error {
	// 无论中途哪个资源关闭失败，都必须释放文件锁，否则同目录无法再次打开。
	defer func() {
		if db.fileLock != nil {
			_ = db.fileLock.Unlock()
		}
		db.closed = true
	}()

	// 先停止对象存储后台上传；最后的上传要等内存表刷完再做。
	if db.objectTierStop != nil {
		close(db.objectTierStop)
		db.objectTierWG.Wait()
	}

	// 先停止自动压缩/IO 监控，避免关闭文件时它们还在使用
	close(db.closeCompactChan)
	db.bgWG.Wait()

	select {
	case _, ok := <-db.flushChan:
		if !ok {
			// channel 已经关闭
		} else {
			// channel 未关闭，可以安全关闭
			close(db.flushChan)
		}
	default:
		// channel 未关闭且为空，可以安全关闭
		close(db.flushChan)
	}
	<-db.closeflushChan

	// 内存表已刷盘，上传新产生的冷段后再关闭 vlog。
	if db.objectTier != nil {
		_ = db.vlog.sync()
		db.uploadColdSegments(false)
		_ = db.objectTier.Close()
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭所有打开过的内存表。注册表包含已从 immuMems 移除、但可能仍有句柄的 WAL。
	for _, table := range db.memtables {
		if err := table.close(); err != nil {
			return err
		}
	}
	for _, table := range db.immuMems {
		if err := table.close(); err != nil {
			return err
		}
	}
	if err := db.activeMem.close(); err != nil {
		return err
	}
	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	db.flushLock.Lock()
	// 持久化废弃数量和总条目数量
	discardMetaPath := filepath.Join(db.options.DirPath, discardMetaName)
	err := storeDiscardEntryMeta(discardMetaPath, db.vlog.discardNumber, db.vlog.totalNumber)
	if err != nil {
		return err
	}
	defer db.flushLock.Unlock()

	// 保存布隆过滤器
	if db.bloomManager != nil {
		db.bloomManager.StopAutoSave()
		if err := db.bloomManager.SaveAll(); err != nil {
			return err
		}
	}

	// 关闭值日志
	if err = db.vlog.close(); err != nil {
		return err
	}
	return nil
}

// Sync 将所有数据文件同步到底层存储。
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 同步所有内存表的WAL
	for _, table := range db.immuMems {
		if err := table.sync(); err != nil {
			return err
		}
	}
	if err := db.activeMem.sync(); err != nil {
		return err
	}
	// 同步索引
	if err := db.index.Sync(); err != nil {
		return err
	}
	// 同步值日志
	if err := db.vlog.sync(); err != nil {
		return err
	}

	return nil
}

// Put 使用默认写入选项进行写入。
func (db *DB) Put(key []byte, value []byte) error {
	return db.PutWithOptions(key, value, DefaultWriteOptions)
}

// PutWithOptions 将键值对写入数据库。
// 实际上，它会打开一个新的批处理并提交它。
// 可以认为这个批处理只有一个Put操作。
func (db *DB) PutWithOptions(key []byte, value []byte, options WriteOptions) error {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.options.WriteOptions = options
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// 这是单个put操作，我们可以将Sync设置为false。
	// 因为数据将被写入WAL，并且WAL文件将根据DB选项同步到磁盘。
	batch.init(false, false, false, db).withPendingWrites()
	if err := batch.Put(key, value); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit()
}

// Get 从数据库获取指定键的值。
// 实际上，它会打开一个新的批处理并提交它。
// 可以认为这个批处理只有一个Get操作。
func (db *DB) Get(key []byte) ([]byte, error) {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.init(true, false, true, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

// Delete 使用默认写入选项进行删除。
func (db *DB) Delete(key []byte) error {
	return db.DeleteWithOptions(key, DefaultWriteOptions)
}

// DeleteWithOptions 从数据库中删除指定的键。
// 实际上，它会打开一个新的批处理并提交它。
// 可以认为这个批处理只有一个Delete操作。
func (db *DB) DeleteWithOptions(key []byte, options WriteOptions) error {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.options.WriteOptions = options
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// 这是单个delete操作，我们可以将Sync设置为false。
	// 因为数据将被写入WAL，
	// 并且WAL文件将根据DB选项同步到磁盘。
	batch.init(false, false, false, db).withPendingWrites()
	if err := batch.Delete(key); err != nil {
		batch.unlock()
		return err
	}
	return batch.Commit()
}

// Exist 检查指定的键是否存在于数据库中。
// 实际上，它会打开一个新的批处理并提交它。
// 你可以认为这个批处理只有一个Exist操作。
func (db *DB) Exist(key []byte) (bool, error) {
	batch, ok := db.batchPool.Get().(*Batch)
	if !ok {
		panic("batchPoll.Get failed")
	}
	batch.init(true, false, true, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Exist(key)
}

// validateOptions 验证给定的选项。
func validateOptions(options *Options) error {
	if options.DirPath == "" {
		return ErrDBDirectoryISEmpty
	}
	if options.MemtableSize <= 0 {
		options.MemtableSize = DefaultOptions.MemtableSize
	}
	if options.MemtableNums <= 0 {
		options.MemtableNums = DefaultOptions.MemtableNums
	}
	if options.PartitionNum <= 0 {
		options.PartitionNum = DefaultOptions.PartitionNum
	}
	if options.ValueLogFileSize <= 0 {
		options.ValueLogFileSize = DefaultOptions.ValueLogFileSize
	}
	// 确保ValueLogFileSize >= MemtableSize
	if options.ValueLogFileSize < int64(options.MemtableSize) {
		options.ValueLogFileSize = int64(options.MemtableSize)
	}
	return nil
}

// getMemTables 获取所有内存表，包括活跃内存表和不可变内存表。
// 必须在持有db.mu锁的情况下调用。
func (db *DB) getMemTables() []*memtable {
	var tables []*memtable
	tables = append(tables, db.activeMem)

	last := len(db.immuMems) - 1
	for i := range db.immuMems {
		tables = append(tables, db.immuMems[last-i])
	}

	return tables
}

// waitMemtableSpace 等待内存表中的空间。
// 如果活跃内存表已满，它将由后台协程刷新到磁盘。
// 但如果刷新速度慢于写入速度，内存表中可能没有空间。
// 因此写入操作将等待内存表中的空间，超时时间由 WaitMemSpaceTimeout 指定。
func (db *DB) waitMemtableSpace() error {
	if !db.activeMem.isFull() {
		return nil
	}

	timer := time.NewTimer(db.options.WaitMemSpaceTimeout)
	defer timer.Stop()
	select {
	case db.flushChan <- db.activeMem:
		db.immuMems = append(db.immuMems, db.activeMem)
		options := db.activeMem.options
		options.tableID++
		// 打开一个新的内存表用于写入
		table, err := openMemtable(options)
		if err != nil {
			return err
		}
		db.activeMem = table
		db.memtables[table.options.tableID] = table
	case <-timer.C:
		return ErrWaitMemtableSpaceTimeOut
	}

	return nil
}

// flushMemtable 将指定的内存表刷新到磁盘。
// 将执行以下步骤：
// 1. 遍历内存表中的所有记录，将它们分为已删除的键和日志记录。
// 2. 将日志记录写入值日志，获取键的位置。
// 3. 添加旧的uuid，将所有键和位置写入索引。
// 4. 添加已删除的uuid，并从索引中删除已删除的键。
// 5. 删除WAL。
func (db *DB) flushMemtable(table *memtable) {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	sklIter := table.skl.NewIterator()
	var deletedKeys [][]byte
	var logRecords []*ValueLogRecord

	// 遍历内存表中的所有记录，将它们分为已删除的键和日志记录
	// 对于每个日志记录，我们生成uuid。
	for sklIter.SeekToFirst(); sklIter.Valid(); sklIter.Next() {
		key, valueStruct := y.ParseKey(sklIter.Key()), sklIter.Value()
		if valueStruct.Meta == LogRecordDeleted {
			deletedKeys = append(deletedKeys, key)
		} else {
			logRecord := ValueLogRecord{key: key, value: valueStruct.Value, uid: uuid.New()}
			logRecords = append(logRecords, &logRecord)
		}
	}
	_ = sklIter.Close()
	// log.Println("len del:",len(deletedKeys),len(logRecords))

	// 写入值日志，获取键的位置
	keyPos, err := db.vlog.writeBatch(logRecords)
	if err != nil {
		log.Println("vlog writeBatch failed:", err)
		return
	}

	// 同步值日志
	if err = db.vlog.sync(); err != nil {
		log.Println("vlog sync failed:", err)
		return
	}

	// Hash 索引的 PutBatch 不返回旧位置，需要先回查（MatchKeyFunc 会顺带读出旧 UUID）。
	var hashOldPositions []*KeyPosition
	if db.options.IndexType == Hash {
		for _, kp := range keyPos {
			old, err := db.currentKeyPosition(kp.key)
			if err != nil {
				log.Println("lookup old key position failed:", err)
				continue
			}
			if old != nil {
				hashOldPositions = append(hashOldPositions, old)
			}
		}
	}

	// 先加布隆，再写索引：避免“索引已可见、布隆还没加”期间读到假阴性。
	// 如果索引写失败，布隆最多多几个 key（假阳性），不会漏 key。
	if db.bloomManager != nil {
		for _, kp := range keyPos {
			db.bloomManager.AddKey(kp.key)
		}
	}

	// 将所有键和位置写入索引。
	var putMatchKeys []diskhash.MatchKeyFunc
	if db.options.IndexType == Hash && len(keyPos) > 0 {
		putMatchKeys = make([]diskhash.MatchKeyFunc, len(keyPos))
		for i := range putMatchKeys {
			putMatchKeys[i] = MatchKeyFunc(db, keyPos[i].key, nil, nil)
		}
	}

	oldKeyPostions, err := db.index.PutBatch(keyPos, putMatchKeys...)
	if err != nil {
		log.Println("index PutBatch failed:", err)
		return
	}
	oldKeyPostions = append(oldKeyPostions, hashOldPositions...)

	// 将旧键 uuid 加入废弃表（覆盖写的旧版本）。
	for _, oldKeyPostion := range oldKeyPostions {
		db.vlog.setDiscard(oldKeyPostion.partition, oldKeyPostion.uid)
	}

	// 删除键：先查旧位置（Hash 索引不会返回），再删索引，最后把旧 uuid 加入废弃表。
	var hashDeletePositions []*KeyPosition
	if db.options.IndexType == Hash {
		for _, key := range deletedKeys {
			old, err := db.currentKeyPosition(key)
			if err != nil {
				log.Println("lookup deleted key position failed:", err)
				continue
			}
			if old != nil {
				hashDeletePositions = append(hashDeletePositions, old)
			}
		}
	}

	var deleteMatchKeys []diskhash.MatchKeyFunc
	if db.options.IndexType == Hash && len(deletedKeys) > 0 {
		deleteMatchKeys = make([]diskhash.MatchKeyFunc, len(deletedKeys))
		for i := range deleteMatchKeys {
			deleteMatchKeys[i] = MatchKeyFunc(db, deletedKeys[i], nil, nil)
		}
	}

	if oldKeyPostions, err = db.index.DeleteBatch(deletedKeys, deleteMatchKeys...); err != nil {
		log.Println("index DeleteBatch failed:", err)
		return
	}
	oldKeyPostions = append(oldKeyPostions, hashDeletePositions...)

	// 删除的 key 也必须进入废弃表，否则增量压缩会把它当有效记录重新插回索引。
	for _, oldKeyPostion := range oldKeyPostions {
		db.vlog.setDiscard(oldKeyPostion.partition, oldKeyPostion.uid)
	}

	// 同步索引
	if err = db.index.Sync(); err != nil {
		log.Println("index sync failed:", err)
		return
	}

	// 删除WAL
	if err = table.deleteWAl(); err != nil {
		log.Println("delete wal failed:", err)
		return
	}

	// 删除内存中保存的旧内存表
	db.mu.Lock()
	defer db.mu.Unlock()
	oldTableID := table.options.tableID
	if table == db.activeMem {
		options := db.activeMem.options
		options.tableID++
		// 打开一个新的内存表用于写入
		table, err = openMemtable(options)
		if err != nil {
			panic("flush activate memtable wrong")
		}
		db.activeMem = table
		db.memtables[table.options.tableID] = table
	} else {
		if len(db.immuMems) == 1 {
			db.immuMems = db.immuMems[:0]
		} else {
			db.immuMems = db.immuMems[1:]
		}
	}
	delete(db.memtables, oldTableID)
	db.sendThresholdState()
}

// currentKeyPosition 返回 key 当前在索引中的位置。
// Hash 索引的 Get 通过 MatchKeyFunc 回表，顺带把旧 UUID 填进 KeyPosition。
func (db *DB) currentKeyPosition(key []byte) (*KeyPosition, error) {
	if db.options.IndexType == Hash {
		var keyPos *KeyPosition
		_, err := db.index.Get(key, MatchKeyFunc(db, key, &keyPos, nil))
		return keyPos, err
	}
	return db.index.Get(key)
}

func (db *DB) sendThresholdState() {
	if db.options.AutoCompactSupport {
		// 检查废弃表大小
		lowerThreshold := uint32((float32)(db.vlog.totalNumber) * db.options.AdvisedCompactionRate)
		upperThreshold := uint32((float32)(db.vlog.totalNumber) * db.options.ForceCompactionRate)
		thresholdState := discardState{
			thresholdState: ThresholdState(UnarriveThreshold),
		}
		if db.vlog.discardNumber >= upperThreshold {
			thresholdState = discardState{
				thresholdState: ThresholdState(ArriveForceThreshold),
			}
		} else if db.vlog.discardNumber > lowerThreshold {
			thresholdState = discardState{
				thresholdState: ThresholdState(ArriveAdvisedThreshold),
			}
		}
		select {
		case db.compactChan <- thresholdState:
		default: // 正在压缩，什么都不做。
		}
	}
}

func (db *DB) listenMemtableFlush() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		// 定时器
		case table, ok := <-db.flushChan:
			if ok {
				db.flushMemtable(table)
			} else {
				db.closeflushChan <- struct{}{}
				return
			}
		case <-sig:
			return
		}
	}
}

// listenAutoComapct 是一种自动化且更细粒度的方法，不会阻塞Bptree。
// 它动态检测每个分片的冗余情况，并根据当前IO状态决定是否进行压缩。
//
//nolint:gocognit
func (db *DB) listenAutoCompact() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	firstCompact := true
	thresholdstate := ThresholdState(UnarriveThreshold)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	retryDelay := 1 * time.Second
	maxRetryDelay := 60 * time.Second
	var nextRetryAt time.Time
	for {
		select {
		case state, ok := <-db.compactChan:
			if !ok {
				return
			}
			thresholdstate = state.thresholdState
		case <-db.closeCompactChan:
			return
		case <-sig:
			return
		case <-ticker.C:
			if !nextRetryAt.IsZero() && time.Now().Before(nextRetryAt) {
				continue
			}
			if thresholdstate == ThresholdState(ArriveForceThreshold) {
				var err error
				if firstCompact {
					firstCompact = false
					err = db.Compact()
				} else {
					err = db.CompactWithDiscardtable()
				}
				if err != nil {
					log.Println("compact error:", err)
					nextRetryAt = time.Now().Add(retryDelay)
					if retryDelay < maxRetryDelay {
						retryDelay *= 2
					}
					continue
				}
				thresholdstate = ThresholdState(UnarriveThreshold)
				nextRetryAt = time.Time{}
				retryDelay = 1 * time.Second
			} else if thresholdstate == ThresholdState(ArriveAdvisedThreshold) {
				free := true
				var err error
				if db.options.EnableDiskIO {
					free, err = db.diskIO.IsFree()
					if err != nil {
						log.Println("disk io state error:", err)
						nextRetryAt = time.Now().Add(retryDelay)
						if retryDelay < maxRetryDelay {
							retryDelay *= 2
						}
						continue
					}
				}
				if free {
					if firstCompact {
						firstCompact = false
						err = db.Compact()
					} else {
						err = db.CompactWithDiscardtable()
					}
					if err != nil {
						log.Println("compact error:", err)
						nextRetryAt = time.Now().Add(retryDelay)
						if retryDelay < maxRetryDelay {
							retryDelay *= 2
						}
						continue
					}
					thresholdstate = ThresholdState(UnarriveThreshold)
					nextRetryAt = time.Time{}
					retryDelay = 1 * time.Second
				} else {
					log.Println("IO 当前繁忙")
				}
			}
		}
	}
}

func (db *DB) listenDiskIOState() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case <-sig:
			return
		case <-db.closeCompactChan:
			return
		default:
			if err := db.diskIO.Monitor(); err != nil {
				log.Printf("disk io monitor stopped: %v", err)
				return
			}
		}
	}
}

// sameChunkPosition 只比较定位所需字段。
// 迭代器给出的 ChunkSize 是估算值（可能包含 padding），不能用于判断记录是否仍是当前版本。
func sameChunkPosition(a, b *wal.ChunkPosition) bool {
	if a == nil || b == nil {
		return false
	}
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}

// Compact 会遍历vlog中的所有值，并将有效值写入新的vlog文件。
// 然后用新文件替换旧的vlog文件，并删除旧文件。
//
//nolint:gocognit
func (db *DB) Compact() error {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	log.Println("[Compact data]")
	openVlogFile := func(part int, ext string, initialID uint32) (*wal.WAL, error) {
		walFile, err := wal.Open(wal.Options{
			DirPath:          db.vlog.options.dirPath,
			SegmentSize:      db.vlog.options.segmentSize,
			SegmentFileExt:   fmt.Sprintf(ext, part),
			InitialSegmentID: wal.SegmentID(initialID),
			Sync:             false, // 我们会手动同步
			BytesPerSync:     0,     // 与Sync相同
		})
		if err != nil {
			if walFile != nil {
				_ = walFile.Delete()
			}
			return nil, err
		}
		return walFile, nil
	}

	g, _ := errgroup.WithContext(context.Background())
	var capacity int64
	var capacityList = make([]int64, db.options.PartitionNum)
	for i := 0; i < int(db.vlog.options.partitionNum); i++ {
		part := i
		g.Go(func() error {
			// 新段使用比旧段更大的 ID，避免压缩过程中新旧段文件名冲突。
			newID := db.vlog.walFiles[part].ActiveSegmentID() + 1
			newVlogFile, err := openVlogFile(part, tempValueLogFileExt, uint32(newID))
			if err != nil {
				return err
			}
			// 索引可能在新文件 rename 之前就指向新段，先让读路径能找到它。
			db.vlog.addReadableWAL(part, newVlogFile)
			defer db.vlog.removeReadableWAL(part, newVlogFile)
			validRecords := make([]*ValueLogRecord, 0)
			reader := db.vlog.walFiles[part].NewReader() // 遍历wal中的所有记录，找到有效记录
			for {
				chunk, pos, err := reader.Next()
				atomic.AddInt64(&capacity, int64(len(chunk)))
				capacityList[part] += int64(len(chunk))
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					_ = newVlogFile.Delete()
					return err
				}

				record := decodeValueLogRecord(chunk)
				if record == nil {
					continue
				}
				var hashTableKeyPos *KeyPosition
				var matchKey func(diskhash.Slot) (bool, error)
				if db.options.IndexType == Hash {
					matchKey = MatchKeyFunc(db, record.key, &hashTableKeyPos, nil)
				}
				keyPos, err := db.index.Get(record.key, matchKey)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}

				if db.options.IndexType == Hash {
					keyPos = hashTableKeyPos
				}

				if keyPos == nil {
					continue
				}
				if keyPos.partition == uint32(part) && sameChunkPosition(keyPos.position, pos) {
					validRecords = append(validRecords, record)
				}

				if capacity >= int64(db.vlog.options.compactBatchCapacity) {
					err = db.rewriteValidRecords(newVlogFile, validRecords, part)
					if err != nil {
						_ = newVlogFile.Delete()
						return err
					}
					validRecords = validRecords[:0]
					atomic.AddInt64(&capacity, -capacityList[part])
					capacityList[part] = 0
				}
			}

			if len(validRecords) > 0 {
				err := db.rewriteValidRecords(newVlogFile, validRecords, part)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}
			}

			// 用新文件替换 wal。读者在 Get 期间持有 db.mu.RLock，这里用写锁切换，
			// 避免读者拿到旧指针后文件已被删除。
			db.mu.Lock()
			// 先关闭新文件句柄并 rename；旧段仍保持打开，rename 不会覆盖旧段名。
			_ = newVlogFile.Close()
			if err := newVlogFile.RenameFileExt(fmt.Sprintf(valueLogFileExt, part)); err != nil {
				db.mu.Unlock()
				return err
			}
			reopened, err := openVlogFile(part, valueLogFileExt, 1)
			if err != nil {
				db.mu.Unlock()
				return err
			}
			// reopened 同时打开旧段和新段；此时再关闭旧 WAL，并删除旧段。
			if oldWal := db.vlog.walFiles[part]; oldWal != nil {
				_ = oldWal.Close()
			}
			_ = reopened.DeleteSegmentsBelow(wal.SegmentID(newID))
			db.vlog.walFiles[part] = reopened
			db.vlog.dpTables[part].clean()
			db.mu.Unlock()

			return nil
		})
	}
	err := g.Wait()
	db.mu.Lock()
	db.vlog.cleanDiscardTable()
	db.mu.Unlock()
	return err
}

// CompactWithDiscardtable 会遍历vlog中的所有值，通过discardtable查找旧值，
// 并将有效值写入新的vlog文件。
// 然后用新文件替换旧的vlog文件，并删除旧文件。
//
//nolint:gocognit
func (db *DB) CompactWithDiscardtable() error {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	log.Println("[CompactWithDiscardtable data]")
	openVlogFile := func(part int, ext string, initialID uint32) (*wal.WAL, error) {
		walFile, err := wal.Open(wal.Options{
			DirPath:          db.vlog.options.dirPath,
			SegmentSize:      db.vlog.options.segmentSize,
			SegmentFileExt:   fmt.Sprintf(ext, part),
			InitialSegmentID: wal.SegmentID(initialID),
			Sync:             false, // 我们会手动同步
			BytesPerSync:     0,     // 与Sync相同
		})
		if err != nil {
			if walFile != nil {
				_ = walFile.Delete()
			}
			return nil, err
		}
		return walFile, nil
	}

	g, _ := errgroup.WithContext(context.Background())
	var capacity int64
	var capacityList = make([]int64, db.options.PartitionNum)
	for i := 0; i < int(db.vlog.options.partitionNum); i++ {
		part := i
		g.Go(func() error {
			// 新段使用比旧段更大的 ID，避免压缩过程中新旧段文件名冲突。
			newID := db.vlog.walFiles[part].ActiveSegmentID() + 1
			newVlogFile, err := openVlogFile(part, tempValueLogFileExt, uint32(newID))
			if err != nil {
				return err
			}
			// 索引可能在新文件 rename 之前就指向新段，先让读路径能找到它。
			db.vlog.addReadableWAL(part, newVlogFile)
			defer db.vlog.removeReadableWAL(part, newVlogFile)
			validRecords := make([]*ValueLogRecord, 0)
			reader := db.vlog.walFiles[part].NewReader()
			// 遍历wal中的所有记录，找到有效记录
			for {
				chunk, pos, err := reader.Next()
				atomic.AddInt64(&capacity, int64(len(chunk)))
				capacityList[part] += int64(len(chunk))
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					_ = newVlogFile.Delete()
					return err
				}

				record := decodeValueLogRecord(chunk)
				if record == nil {
					continue
				}

				// 废弃表命中时可以直接跳过；未命中时仍要回查索引，
				// 因为废弃表可能在重启/崩溃后不完整。索引不存在或位置不匹配的记录一律丢弃，
				// 这样增量压缩也不会把已删除的 key 重新插回索引。
				if !db.vlog.isDiscard(part, record.uid) {
					var keyPos *KeyPosition
					if db.options.IndexType == Hash {
						var hashTableKeyPos *KeyPosition
						matchKey := MatchKeyFunc(db, record.key, &hashTableKeyPos, nil)
						if _, err = db.index.Get(record.key, matchKey); err != nil {
							_ = newVlogFile.Delete()
							return err
						}
						keyPos = hashTableKeyPos
					} else {
						keyPos, err = db.index.Get(record.key)
						if err != nil {
							_ = newVlogFile.Delete()
							return err
						}
					}

					if keyPos != nil && keyPos.partition == uint32(part) &&
						sameChunkPosition(keyPos.position, pos) {
						validRecords = append(validRecords, record)
					}
				}

				if capacity >= int64(db.vlog.options.compactBatchCapacity) {
					err = db.rewriteValidRecords(newVlogFile, validRecords, part)
					if err != nil {
						_ = newVlogFile.Delete()
						return err
					}
					validRecords = validRecords[:0]
					atomic.AddInt64(&capacity, -capacityList[part])
					capacityList[part] = 0
				}
			}
			if len(validRecords) > 0 {
				err := db.rewriteValidRecords(newVlogFile, validRecords, part)
				if err != nil {
					_ = newVlogFile.Delete()
					return err
				}
			}

			// 同 Compact：切换文件指针时持有写锁，避免并发读者读到已删除的旧文件。
			db.mu.Lock()
			// 先关闭新文件句柄并 rename；旧段仍保持打开，rename 不会覆盖旧段名。
			_ = newVlogFile.Close()
			if err := newVlogFile.RenameFileExt(fmt.Sprintf(valueLogFileExt, part)); err != nil {
				db.mu.Unlock()
				return err
			}
			reopened, err := openVlogFile(part, valueLogFileExt, 1)
			if err != nil {
				db.mu.Unlock()
				return err
			}
			// reopened 同时打开旧段和新段；此时再关闭旧 WAL，并删除旧段。
			if oldWal := db.vlog.walFiles[part]; oldWal != nil {
				_ = oldWal.Close()
			}
			_ = reopened.DeleteSegmentsBelow(wal.SegmentID(newID))
			db.vlog.walFiles[part] = reopened
			db.vlog.dpTables[part].clean()
			db.mu.Unlock()
			return nil
		})
	}

	err := g.Wait()
	db.mu.Lock()
	db.vlog.cleanDiscardTable()
	db.mu.Unlock()
	return err
}

func (db *DB) rewriteValidRecords(walFile *wal.WAL, validRecords []*ValueLogRecord, part int) error {
	for _, record := range validRecords {
		walFile.PendingWrites(encodeValueLogRecord(record))
	}

	walChunkPositions, err := walFile.WriteAll()
	if err != nil {
		return err
	}

	positions := make([]*KeyPosition, len(walChunkPositions))
	for i, walChunkPosition := range walChunkPositions {
		positions[i] = &KeyPosition{
			key:       validRecords[i].key,
			partition: uint32(part),
			position:  walChunkPosition,
		}
	}
	matchKeys := make([]diskhash.MatchKeyFunc, len(positions))
	if db.options.IndexType == Hash {
		for i := range matchKeys {
			matchKeys[i] = MatchKeyFunc(db, positions[i].key, nil, nil)
		}
	}
	_, err = db.index.PutBatch(positions, matchKeys...)
	return err
}

func (db *DB) GetOptions() Options {
	return db.options
}

// 加载废弃条目元数据，如果首次打开则创建元数据文件。
func loadDiscardEntryMeta(discardMetaPath string) (uint32, uint32, error) {
	var err error
	var discardNumber uint32
	var totalEntryNumber uint32
	if _, err = os.Stat(discardMetaPath); os.IsNotExist(err) {
		// 不存在则创建一个
		var file *os.File
		file, err = os.Create(discardMetaPath)
		if err != nil {
			return discardNumber, totalEntryNumber, err
		}
		discardNumber = 0
		totalEntryNumber = 0
		file.Close()
	} else if err != nil {
		return discardNumber, totalEntryNumber, err
	} else {
		// 没有错误，我们加载元数据
		var file *os.File
		file, err = os.Open(discardMetaPath)
		if err != nil {
			return discardNumber, totalEntryNumber, err
		}
		defer file.Close()

		// 将文件指针设置为0
		_, err = file.Seek(0, 0)
		if err != nil {
			return discardNumber, totalEntryNumber, err
		}

		// 读取废弃数量
		err = binary.Read(file, binary.LittleEndian, &discardNumber)
		if err != nil {
			return discardNumber, totalEntryNumber, err
		}

		// 读取总条目数量
		err = binary.Read(file, binary.LittleEndian, &totalEntryNumber)
		if err != nil {
			return discardNumber, totalEntryNumber, err
		}
	}
	return discardNumber, totalEntryNumber, nil
}

// 持久化废弃数量和总条目数量.
func storeDiscardEntryMeta(discardMetaPath string, discardNumber uint32, totalNumber uint32) error {
	file, err := os.OpenFile(discardMetaPath, os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	// 将文件指针设置为0并覆盖
	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}

	// 写入废弃数量
	err = binary.Write(file, binary.LittleEndian, &discardNumber)
	if err != nil {
		return err
	}

	// 写入总条目数量
	err = binary.Write(file, binary.LittleEndian, &totalNumber)
	if err != nil {
		return err
	}
	return nil
}
