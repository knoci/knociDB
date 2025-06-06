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
	"reflect"
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
	closeflushChan   chan struct{} // 用于优雅地关闭刷新监听协程
	closeCompactChan chan struct{} // 用于优雅地关闭自动压缩监听协程
	options          Options
	batchPool        sync.Pool // 批处理池，用于减少内存分配开销
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

	// 创建文件锁，防止多个进程使用相同的数据库目录
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
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
		objectStorage:        options.ObjectStorage,
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
			ExpectedElements:  options.BloomFilterExpectedElements,
			FalsePositiveRate: options.BloomFilterFalsePositiveRate,
			DirPath:           options.DirPath,
			PartitionNum:      options.PartitionNum,
			keyHashFunction:   options.KeyHashFunction,
		}
		bloomManager = NewBloomFilterManager(bloomOptions)
	}

	db := &DB{
		activeMem:        memtables[len(memtables)-1],
		immuMems:         memtables[:len(memtables)-1],
		index:            index,
		vlog:             vlog,
		bloomManager:     bloomManager,
		fileLock:         fileLock,
		flushChan:        make(chan *memtable, options.MemtableNums-1),
		closeflushChan:   make(chan struct{}),
		closeCompactChan: make(chan struct{}),
		diskIO:           diskIO,
		options:          options,
		batchPool:        sync.Pool{New: makeBatch},
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
		go db.listenAutoCompact()

		// 启动磁盘IO监控，
		// 在繁忙时阻塞低阈值压缩操作。
		if options.EnableDiskIO {
			go db.listenDiskIOState()
		}
	}

	return db, nil
}

// Close 关闭数据库，关闭所有数据文件并释放文件锁。
// 将closed标志设置为true。
// 关闭后不能再使用DB实例。
func (db *DB) Close() error {
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

	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭所有内存表
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
		if err := db.bloomManager.SaveAll(); err != nil {
			return err
		}
	}

	// 关闭值日志
	if err = db.vlog.close(); err != nil {
		return err
	}
	// 释放文件锁
	if err = db.fileLock.Unlock(); err != nil {
		return err
	}

	db.closed = true
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

	// 将所有键和位置写入索引。
	var putMatchKeys []diskhash.MatchKeyFunc
	if db.options.IndexType == Hash && len(keyPos) > 0 {
		putMatchKeys = make([]diskhash.MatchKeyFunc, len(keyPos))
		for i := range putMatchKeys {
			putMatchKeys[i] = MatchKeyFunc(db, keyPos[i].key, nil, nil)
		}
	}

	// 将所有键和位置写入索引。
	oldKeyPostions, err := db.index.PutBatch(keyPos, putMatchKeys...)
	if err != nil {
		log.Println("index PutBatch failed:", err)
		return
	}

	// 将键添加到布隆过滤器
	if db.bloomManager != nil {
		for _, kp := range keyPos {
			db.bloomManager.AddKey(kp.key)
		}
	}

	// 将旧键uuid添加到废弃表中
	for _, oldKeyPostion := range oldKeyPostions {
		db.vlog.setDiscard(oldKeyPostion.partition, oldKeyPostion.uid)
	}

	// 将已删除键uuid添加到废弃表中，并从索引中删除已删除的键。
	var deleteMatchKeys []diskhash.MatchKeyFunc
	if db.options.IndexType == Hash && len(deletedKeys) > 0 {
		deleteMatchKeys = make([]diskhash.MatchKeyFunc, len(deletedKeys))
		for i := range deleteMatchKeys {
			deleteMatchKeys[i] = MatchKeyFunc(db, deletedKeys[i], nil, nil)
		}
	}

	// 从索引中删除已删除的键
	if oldKeyPostions, err = db.index.DeleteBatch(deletedKeys, deleteMatchKeys...); err != nil {
		log.Println("index DeleteBatch failed:", err)
		return
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
	if table == db.activeMem {
		options := db.activeMem.options
		options.tableID++
		// 打开一个新的内存表用于写入
		table, err = openMemtable(options)
		if err != nil {
			panic("flush activate memtable wrong")
		}
		db.activeMem = table
	} else {
		if len(db.immuMems) == 1 {
			db.immuMems = db.immuMems[:0]
		} else {
			db.immuMems = db.immuMems[1:]
		}
	}
	db.sendThresholdState()
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
	for {
		select {
		case state, ok := <-db.compactChan:
			if ok {
				thresholdstate = state.thresholdState
			} else {
				db.closeCompactChan <- struct{}{}
				return
			}
		case <-sig:
			return
		case <-ticker.C:
			// 需要多层嵌套条件以处理不同阈值和错误判断。
			if thresholdstate == ThresholdState(ArriveForceThreshold) {
				var err error
				if firstCompact {
					firstCompact = false
					err = db.Compact()
				} else {
					err = db.CompactWithDiscardtable()
				}
				if err != nil {
					panic(err)
				}
				thresholdstate = ThresholdState(UnarriveThreshold)
			} else if thresholdstate == ThresholdState(ArriveAdvisedThreshold) {
				// 根据当前IO状态判断是否进行压缩
				free := true
				var err error = nil
				if db.options.EnableDiskIO {
					free, err = db.diskIO.IsFree()
					if err != nil {
						panic(err)
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
						panic(err)
					}
					thresholdstate = ThresholdState(UnarriveThreshold)
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
		default:
			err := db.diskIO.Monitor()
			if err != nil {
				panic(err)
			}
		}
	}
}

// Compact 会遍历vlog中的所有值，并将有效值写入新的vlog文件。
// 然后用新文件替换旧的vlog文件，并删除旧文件。
//
//nolint:gocognit
func (db *DB) Compact() error {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	log.Println("[Compact data]")
	openVlogFile := func(part int, ext string) *wal.WAL {
		walFile, err := wal.Open(wal.Options{
			DirPath:        db.vlog.options.dirPath,
			SegmentSize:    db.vlog.options.segmentSize,
			SegmentFileExt: fmt.Sprintf(ext, part),
			Sync:           false, // 我们会手动同步
			BytesPerSync:   0,     // 与Sync相同
		})
		if err != nil {
			_ = walFile.Delete()
			panic(err)
		}
		return walFile
	}

	g, _ := errgroup.WithContext(context.Background())
	var capacity int64
	var capacityList = make([]int64, db.options.PartitionNum)
	for i := 0; i < int(db.vlog.options.partitionNum); i++ {
		part := i
		g.Go(func() error {
			newVlogFile := openVlogFile(part, tempValueLogFileExt)
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
				if keyPos.partition == uint32(part) && reflect.DeepEqual(keyPos.position, pos) {
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

			// 用新文件替换wal。
			_ = db.vlog.walFiles[part].Delete()
			_ = newVlogFile.Close()
			if err := newVlogFile.RenameFileExt(fmt.Sprintf(valueLogFileExt, part)); err != nil {
				return err
			}
			db.vlog.walFiles[part] = openVlogFile(part, valueLogFileExt)

			// 压缩后清理dpTable
			db.vlog.dpTables[part].clean()

			return nil
		})
	}
	db.vlog.cleanDiscardTable()
	return g.Wait()
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
	openVlogFile := func(part int, ext string) *wal.WAL {
		walFile, err := wal.Open(wal.Options{
			DirPath:        db.vlog.options.dirPath,
			SegmentSize:    db.vlog.options.segmentSize,
			SegmentFileExt: fmt.Sprintf(ext, part),
			Sync:           false, // 我们会手动同步
			BytesPerSync:   0,     // 与Sync相同
		})
		if err != nil {
			_ = walFile.Delete()
			panic(err)
		}
		return walFile
	}

	g, _ := errgroup.WithContext(context.Background())
	var capacity int64
	var capacityList = make([]int64, db.options.PartitionNum)
	for i := 0; i < int(db.vlog.options.partitionNum); i++ {
		part := i
		g.Go(func() error {
			newVlogFile := openVlogFile(part, tempValueLogFileExt)
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
				if !db.vlog.isDiscard(part, record.uid) {
					// 在dptable中未找到旧uuid，加入有效记录。
					validRecords = append(validRecords, record)
				}
				if db.options.IndexType == Hash {
					var hashTableKeyPos *KeyPosition
					// var matchKey func(diskhash.Slot) (bool, error)
					matchKey := MatchKeyFunc(db, record.key, &hashTableKeyPos, nil)
					var keyPos *KeyPosition
					keyPos, err = db.index.Get(record.key, matchKey)
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
					if keyPos.partition == uint32(part) && reflect.DeepEqual(keyPos.position, pos) {
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

			// 用新文件替换wal。
			_ = db.vlog.walFiles[part].Delete()
			_ = newVlogFile.Close()
			if err := newVlogFile.RenameFileExt(fmt.Sprintf(valueLogFileExt, part)); err != nil {
				return err
			}
			db.vlog.walFiles[part] = openVlogFile(part, valueLogFileExt)
			return nil
		})
	}

	err := g.Wait()
	db.vlog.cleanDiscardTable()
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
	file.Close()
	return nil
}
