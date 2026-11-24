package knocidb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/knoci/knocidb/util"
	"github.com/knoci/knocidb/wal"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/gofrs/flock"
)

const (
	fileLockName    = "FLOCK"
	discardMetaName = "DSCMETA"
)

type DB struct {
	activeMem        *memtable           // shard 0 的活跃内存表（兼容字段）
	immuMems         []*memtable         // shard 0 的不可变内存表（兼容字段）
	shards           []*shard            // 本地并发分区
	index            Index               // 多分区索引，用于存储键和块位置
	vlog             *valueLog           // 值日志
	bloomManager     *BloomFilterManager // 布隆过滤器管理器
	fileLock         *flock.Flock        // 文件锁，防止多个进程使用相同的数据库目录
	flushChan        chan *memtable      // 兼容保留字段（每分片由独立有序 flusher 驱动）
	flushStopCh      chan struct{}       // 优雅关闭每分片有序刷新协程
	flushWorkerWG    sync.WaitGroup      // 等待所有分片刷新协程排空退出
	flushLock        sync.RWMutex        // 刷新闸门：flush 读锁，compact/snapshot/close 写锁
	flushErrMu       sync.RWMutex        // 保护 flush 失败状态
	flushErr         error               // 首次 flush 失败后进入 fail-fast 状态
	flushWG          sync.WaitGroup      // 后台 flush 兼容等待组
	vlogGuard        sync.RWMutex        // 防止读者仍持有旧索引位置时回收 ValueLog 段
	diskIO           *DiskIO             // 监控磁盘的 IO 状态并在适当时允许自动压缩
	compactChan      chan discardState   // 用于通知需要压缩的分片
	mu               sync.RWMutex
	closed           atomic.Bool
	closeOnce        sync.Once
	closeErr         error
	bgWG             sync.WaitGroup // 自动压缩/IO 监控协程
	closeflushChan   chan struct{}  // 兼容保留字段
	closeCompactChan chan struct{}  // 用于优雅地关闭自动压缩监听协程
	options          Options
	batchPool        sync.Pool // 批处理池，用于减少内存分配开销

	objectTier     *objectTier   // 对象存储冷层/备份层
	objectTierStop chan struct{} // 停止冷段后台上传
	objectTierWG   sync.WaitGroup

	// 记录打开过的 memtable，Close 时兜底关闭，避免 flush 竞态遗留 WAL 句柄。
	memtables   map[uint64]*memtable
	memtablesMu sync.Mutex
	manifestMu  sync.Mutex
	commitLog   *commitLog

	batchRefMu sync.Mutex
	batchRefs  map[uint64]int

	seq         atomic.Uint64 // MVCC 逻辑写序列号（单调递增），支撑零冻结一致性迭代器
	activeIters atomic.Int64  // 当前打开的 MVCC 快照迭代器数量；为 0 时同键覆写可直接原位原子替换
	compactMu   sync.Mutex    // 仅串行化并发 Compaction / Close / Snapshot 文件拷贝，不阻塞常规刷盘

	ttl      *ttlStore   // 兼容字段（保留给历史测试与快照兼容检查，新路径默认 nil）
	ttlIndex *ttlTracker // 轻量内存 TTL 跟踪器（元数据已内嵌至 WAL/Memtable/BPTree）
	ttlStop  chan struct{}
	ttlWG    sync.WaitGroup

	snapshotMu      sync.Mutex
	snapshotWG      sync.WaitGroup
	snapshotsClosed bool
	snapshotRunning bool

	manifestGeneration uint64
	manifestCreatedAt  time.Time
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
	if err := recoverSnapshotRestore(options.DirPath); err != nil {
		return nil, fmt.Errorf("recover interrupted snapshot import: %w", err)
	}
	if err := restoreFromObjectTier(options); err != nil {
		return nil, err
	}

	// 如果不存在则创建废弃元数据文件，读取废弃数量
	discardMetaPath := filepath.Join(options.DirPath, discardMetaName)
	discardNumber, totalEntryNumber, err := loadDiscardEntryMeta(discardMetaPath)
	if err != nil {
		return nil, err
	}

	manifestInfo, manifestErr := readDBManifestInfo(options.DirPath)
	if manifestErr != nil {
		log.Printf("read manifest failed: %v", manifestErr)
	}

	// 打开提交日志与所有内存表
	commitLog, committed, legacy, err := openCommitLog(options.DirPath, options.Sync)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !opened {
			_ = commitLog.close()
		}
	}()
	// 如果 MANIFEST 明确记录该库已启用 COMMIT 日志，缺失 COMMIT 不允许静默降级为 legacy 回放未提交事务。
	if legacy && manifestInfo != nil && manifestInfo.HasCommitLog {
		legacy = false
	}

	memtables, seen, err := openMemtables(options, committed, legacy)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !opened {
			for _, t := range memtables {
				_ = t.close()
			}
		}
	}()
	if legacy {
		// 旧版本 DB 没有 COMMIT 文件：把现有 WAL 批次补记为已提交，
		// 避免升级后第一次重启丢失旧数据。
		for id := range seen {
			if err := commitLog.append(id, true); err != nil {
				return nil, err
			}
		}
	}

	// 打开索引
	index, err := openIndex(indexOptions{
		dirPath:         options.DirPath,
		partitionNum:    options.PartitionNum,
		keyHashFunction: options.KeyHashFunction,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if !opened {
			_ = index.Close()
		}
	}()

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
	defer func() {
		if !opened {
			_ = vlog.close()
		}
	}()

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
		defer func() {
			if !opened {
				bloomManager.StopAutoSave()
			}
		}()
	}

	memtableMap := make(map[uint64]*memtable, len(memtables))
	shards := make([]*shard, options.PartitionNum)
	for i := range shards {
		shards[i] = newShard(uint32(i))
	}
	for _, table := range memtables {
		memtableMap[table.options.tableID] = table
		if int(table.shardID) < len(shards) {
			shards[table.shardID].immu = append(shards[table.shardID].immu, table)
		}
	}
	for _, s := range shards {
		if len(s.immu) == 0 {
			return nil, fmt.Errorf("shard %d has no memtable", s.id)
		}
		s.active = s.immu[len(s.immu)-1]
		s.immu = s.immu[:len(s.immu)-1]
	}

	var manifestGeneration uint64
	var manifestCreatedAt time.Time
	if manifestInfo != nil {
		manifestGeneration = manifestInfo.Generation
		manifestCreatedAt = manifestInfo.CreatedAt
	}
	flushBuf := options.MemtableNums - 1
	if flushBuf < 1 {
		flushBuf = 1
	}
	db := &DB{
		shards:             shards,
		activeMem:          shards[0].active,
		immuMems:           shards[0].immu,
		index:              index,
		vlog:               vlog,
		bloomManager:       bloomManager,
		fileLock:           fileLock,
		flushChan:          make(chan *memtable, flushBuf),
		flushStopCh:        make(chan struct{}),
		compactChan:        make(chan discardState, 1),
		closeflushChan:     make(chan struct{}),
		closeCompactChan:   make(chan struct{}),
		diskIO:             diskIO,
		options:            options,
		batchPool:          sync.Pool{New: makeBatch},
		memtables:          memtableMap,
		commitLog:          commitLog,
		batchRefs:          make(map[uint64]int),
		ttlIndex:           newTTLTracker(),
		manifestGeneration: manifestGeneration,
		manifestCreatedAt:  manifestCreatedAt,
	}
	db.seq.Store(1)
	// 先从持久化 BPTree 索引（最旧层）加载带 TTL 的键，再按 memtable 顺序覆盖更新。
	if bt, ok := index.(*BPTree); ok {
		if err := bt.forEachTTL(func(key []byte, expireAt int64) {
			db.ttlIndex.set(key, expireAt)
		}); err != nil {
			log.Printf("rebuild ttl tracker from index failed: %v", err)
		}
	}
	for _, table := range memtables {
		for id := range table.batchIDs {
			// 只有已有提交标记的批次（含旧库迁移）才能被裁剪保留。
			if _, ok := committed[id]; legacy || ok {
				db.batchRefs[id]++
			}
		}
		// 从恢复的内存表重建内存 TTL 跟踪器（覆盖旧索引状态）
		it := table.skl.NewIterator(false, 0)
		for it.Rewind(); it.Valid(); it.Next() {
			vs := it.Value()
			if vs.Meta == LogRecordDeleted || vs.ExpireAt <= 0 {
				db.ttlIndex.remove(it.Key())
			} else {
				db.ttlIndex.set(it.Key(), vs.ExpireAt)
			}
		}
		_ = it.Close()
	}

	if tier != nil {
		db.objectTier = tier
		db.objectTierStop = make(chan struct{})
		tier = nil // 所有权交给 DB
		db.startObjectTierUploader()
	}
	if options.TTLScanInterval > 0 {
		db.ttlStop = make(chan struct{})
		db.ttlWG.Add(1)
		go db.ttlSweeper()
	}

	// 重建布隆过滤器：不能相信可能过期的 .BLOOM 文件，否则会产生假阴性
	if err := db.rebuildBloomFilters(); err != nil {
		// 失败时保持未就绪，TestKey 会返回 true，保证正确性
		log.Printf("rebuild bloom filters failed: %v", err)
	}

	// 如果在打开数据库时有一些不可变内存表，复制切片后按序刷新到磁盘，
	// 避免 flushMemtableE 原位修改 s.immu 导致跳表或重复刷新。
	for _, s := range db.shards {
		pending := append([]*memtable(nil), s.immu...)
		for _, table := range pending {
			db.flushMemtable(table)
		}
	}

	// 为每个分片启动单独的有序刷新协程，严格按 FIFO 顺序将 immutable memtable 刷盘。
	for _, s := range db.shards {
		shardRef := s
		db.flushWorkerWG.Add(1)
		go func() {
			defer db.flushWorkerWG.Done()
			db.runShardFlusher(shardRef)
		}()
	}

	if err := db.writeDBManifest(); err != nil {
		log.Printf("write manifest failed: %v", err)
	}

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

func (db *DB) ttlSweeper() {
	defer db.ttlWG.Done()
	ticker := time.NewTicker(db.options.TTLScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.ttlStop:
			return
		case now := <-ticker.C:
			nowMs := now.UnixMilli()
			keys := db.ttlIndex.expired(nowMs)
			for _, key := range keys {
				if db.closed.Load() {
					return
				}
				db.sweepExpiredKey(key, nowMs)
			}
		}
	}
}

// sweepExpiredKey 在单分片写锁内二次校验键确实仍处于已过期状态后再写墓碑，彻底消除 TTL Sweeper TOCTOU 误删。
func (db *DB) sweepExpiredKey(key []byte, nowMs int64) {
	s := db.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if db.closed.Load() {
		return
	}
	_, _, expireAt, found, err := db.getLiveRecordLocked(s, key, nowMs, false)
	if err != nil {
		return
	}
	if found {
		// 键已被并发续期或覆盖为无 TTL，更新/清理跟踪器即可，绝不误删！
		if expireAt <= 0 {
			db.ttlIndex.remove(key)
		} else {
			db.ttlIndex.set(key, expireAt)
		}
		return
	}
	if expireAt <= 0 {
		// 键已不存在或已被删除
		db.ttlIndex.remove(key)
		return
	}
	// 确认仍然过期（found == false && expireAt > 0 && expireAt <= nowMs），写入墓碑。
	if err := db.waitMemtableSpace(s); err != nil {
		return
	}
	record := &LogRecord{Key: key, Type: LogRecordDeleted}
	batchID := nextBatchID()
	if err := s.active.prepareRecord(record, batchID, 1, DefaultWriteOptions); err != nil {
		return
	}
	s.active.applyRecordWithMode(record, db.seq.Add(1), db.activeIters.Load() == 0)
	db.ttlIndex.remove(key)
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
	_, err := db.closeAndRun(nil)
	return err
}

func (db *DB) closeAndRun(afterClose func() error) (ran bool, err error) {
	db.closeOnce.Do(func() {
		ran = true
		defer func() {
			if db.fileLock != nil {
				_ = db.fileLock.Unlock()
			}
		}()
		db.closeErr = db.closeInternal()
		if db.closeErr == nil && afterClose != nil {
			err = afterClose()
		}
	})
	if db.closeErr != nil {
		return ran, db.closeErr
	}
	return ran, err
}

func (db *DB) closeInternal() error {
	// 先拒绝新快照并等待进行中的快照，避免关闭仍在读取的文件。
	db.snapshotMu.Lock()
	db.snapshotsClosed = true
	db.snapshotMu.Unlock()
	db.snapshotWG.Wait()

	// 先停止 TTL 后台清理，避免关闭 bbolt 后仍有扫描。
	if db.ttlStop != nil {
		close(db.ttlStop)
		db.ttlWG.Wait()
	}
	if db.ttl != nil {
		defer db.ttl.close()
	}

	// 先停止对象存储后台上传；最后的上传要等内存表刷完再做。
	if db.objectTierStop != nil {
		close(db.objectTierStop)
		db.objectTierWG.Wait()
	}

	// 先停止自动压缩/IO 监控，避免关闭文件时它们还在使用
	close(db.closeCompactChan)
	db.bgWG.Wait()

	// 先置 closed 并过一遍所有分片写锁，确保不会再有新写入或迭代器轮转 memtable。
	shards := db.lockAllShards()
	db.closed.Store(true)
	db.unlockAllShards(shards)

	// 停止并排空所有分片有序刷新协程，确保所有已入队的 immutable memtable 刷盘完成。
	close(db.flushStopCh)
	db.flushWorkerWG.Wait()
	db.flushWG.Wait()

	db.compactMu.Lock()
	defer db.compactMu.Unlock()
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	// 内存表已刷盘，上传新产生的冷段后再关闭 vlog。
	if db.objectTier != nil {
		_ = db.vlog.sync()
		db.uploadColdSegments(false)
		_ = db.objectTier.Close()
	}

	shards = db.lockAllShards()
	defer db.unlockAllShards(shards)
	db.vlogGuard.Lock()
	defer db.vlogGuard.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭所有打开过的内存表；注册表包含所有 shard 的 active/immu 表。
	db.memtablesMu.Lock()
	for _, table := range db.memtables {
		if err := table.close(); err != nil {
			db.memtablesMu.Unlock()
			return err
		}
	}
	db.memtablesMu.Unlock()
	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	// 持久化废弃数量和总条目数量
	discardMetaPath := filepath.Join(db.options.DirPath, discardMetaName)
	totalNumber, discardNumber := db.vlog.stats()
	err := storeDiscardEntryMeta(discardMetaPath, discardNumber, totalNumber)
	if err != nil {
		return err
	}
	// 保存布隆过滤器
	if db.bloomManager != nil {
		db.bloomManager.StopAutoSave()
		if err := db.bloomManager.SaveAll(); err != nil {
			return err
		}
	}

	// 固化文件清单后再关闭 VLOG。
	if err = db.writeDBManifest(); err != nil {
		return err
	}

	// 关闭提交日志和值日志
	if err = db.commitLog.close(); err != nil {
		return err
	}
	if err = db.vlog.close(); err != nil {
		return err
	}
	return nil
}

// Sync 将所有数据文件同步到底层存储。
func (db *DB) Sync() error {
	// 同步所有 shard 内存表的 WAL
	for _, s := range db.shards {
		s.mu.RLock()
		for _, table := range s.memTables() {
			if err := table.sync(); err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		s.mu.RUnlock()
	}
	// 同步索引
	if err := db.index.Sync(); err != nil {
		return err
	}
	// 同步值日志
	if err := db.vlog.sync(); err != nil {
		return err
	}
	// 同步提交日志，确保快照/恢复看到完整 committed 集合。
	if db.commitLog != nil {
		if err := db.commitLog.Sync(); err != nil {
			return err
		}
	}
	// 同步 TTL 旁路索引
	if db.ttl != nil {
		if err := db.ttl.sync(); err != nil {
			return err
		}
	}

	return nil
}

// Put 使用默认写入选项进行写入。
func (db *DB) Put(key []byte, value []byte) error {
	return db.PutWithOptions(key, value, DefaultWriteOptions)
}

// PutWithOptions 将键值对写入数据库。单条写入直接走 WAL + memtable，不创建 Batch。
func (db *DB) PutWithOptions(key []byte, value []byte, options WriteOptions) error {
	return db.putWithOptionsAndTTL(key, value, options, 0)
}

func (db *DB) putWithOptionsAndTTL(key []byte, value []byte, options WriteOptions, expireAt int64) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if db.closed.Load() {
		return ErrDBClosed
	}
	s := db.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if db.closed.Load() {
		return ErrDBClosed
	}
	if err := db.waitMemtableSpace(s); err != nil {
		return err
	}
	recType := LogRecordNormal
	if value != nil && len(value) == 0 {
		recType = LogRecordNormalEmpty
	}
	record := &LogRecord{Key: key, Value: value, Type: recType, ExpireAt: expireAt}
	batchID := nextBatchID()
	if err := s.active.prepareRecord(record, batchID, 1, options); err != nil {
		return err
	}
	s.active.applyRecordWithMode(record, db.seq.Add(1), db.activeIters.Load() == 0)
	if expireAt > 0 {
		db.ttlIndex.set(key, expireAt)
	} else {
		db.ttlIndex.remove(key)
	}
	return nil
}

// getLiveRecordLocked 在调用方持有 s.mu（读锁或写锁）的前提下查询键的最新状态。
// 当 now == 0 时仅在命中 ExpireAt > 0 记录后才惰性读取系统时钟，消除非 TTL 热路径上的时钟开销；
// 当键已过期时返回 found=false, expireAt>0（且 expireAt <= now），便于 ttlSweeper 区分“已过期”与“不存在/已删除”。
func (db *DB) getLiveRecordLocked(s *shard, key []byte, now int64, needValue bool) ([]byte, bool, int64, bool, error) {
	for i := len(s.immu); i >= 0; i-- {
		var table *memtable
		if i == len(s.immu) {
			table = s.active
		} else {
			table = s.immu[i]
		}
		vs, ok := table.getEntry(key)
		if !ok {
			continue
		}
		if vs.Meta == LogRecordDeleted {
			return nil, false, 0, false, nil
		}
		if vs.ExpireAt > 0 {
			if now == 0 {
				now = time.Now().UnixMilli()
			}
			if vs.ExpireAt <= now {
				return nil, false, vs.ExpireAt, false, nil
			}
		}
		if vs.Meta == LogRecordNormalEmpty {
			return []byte{}, true, vs.ExpireAt, true, nil
		}
		if len(vs.Value) == 0 {
			// Put(key, nil) 与历史 memtable.getWithState 保持一致：LogRecordNormal 且长度为 0 视为不可读（继续向下或返回 NotFound）
			continue
		}
		if !needValue {
			return nil, false, vs.ExpireAt, true, nil
		}
		return vs.Value, false, vs.ExpireAt, true, nil
	}
	if db.bloomManager != nil && !db.bloomManager.TestKey(key) {
		return nil, false, 0, false, nil
	}
	db.vlogGuard.RLock()
	defer db.vlogGuard.RUnlock()
	position, err := db.index.Get(key)
	if err != nil {
		return nil, false, 0, false, err
	}
	if position == nil {
		return nil, false, 0, false, nil
	}
	if position.expireAt > 0 {
		if now == 0 {
			now = time.Now().UnixMilli()
		}
		if position.expireAt <= now {
			return nil, false, position.expireAt, false, nil
		}
	}
	if !needValue {
		return nil, false, position.expireAt, true, nil
	}
	record, err := db.vlog.read(position)
	if err != nil {
		return nil, false, 0, false, err
	}
	if len(record.value) == 0 {
		return []byte{}, true, position.expireAt, true, nil
	}
	return record.value, false, position.expireAt, true, nil
}

// Get 从数据库获取指定键的值。
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	s := db.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	val, empty, _, found, err := db.getLiveRecordLocked(s, key, 0, true)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrKeyNotFound
	}
	if empty {
		return []byte{}, nil
	}
	return val, nil
}

// Delete 使用默认写入选项进行删除。
func (db *DB) Delete(key []byte) error {
	return db.DeleteWithOptions(key, DefaultWriteOptions)
}

// DeleteWithOptions 从数据库中删除指定的键。单条删除直接走 WAL + memtable。
func (db *DB) DeleteWithOptions(key []byte, options WriteOptions) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if db.closed.Load() {
		return ErrDBClosed
	}
	s := db.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if db.closed.Load() {
		return ErrDBClosed
	}
	if err := db.waitMemtableSpace(s); err != nil {
		return err
	}
	record := &LogRecord{Key: key, Type: LogRecordDeleted}
	batchID := nextBatchID()
	if err := s.active.prepareRecord(record, batchID, 1, options); err != nil {
		return err
	}
	s.active.applyRecordWithMode(record, db.seq.Add(1), db.activeIters.Load() == 0)
	db.ttlIndex.remove(key)
	return nil
}

// Exist 检查指定的键是否存在于数据库中（无需读取 ValueLog）。
func (db *DB) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if db.closed.Load() {
		return false, ErrDBClosed
	}
	s := db.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if db.closed.Load() {
		return false, ErrDBClosed
	}
	_, _, _, found, err := db.getLiveRecordLocked(s, key, 0, false)
	return found, err
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
	if options.MemtableNums < 2 {
		options.MemtableNums = 2
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
	if options.KeyHashFunction == nil {
		options.KeyHashFunction = DefaultOptions.KeyHashFunction
	}
	if options.WaitMemSpaceTimeout <= 0 {
		options.WaitMemSpaceTimeout = DefaultOptions.WaitMemSpaceTimeout
	}
	if options.CompactBatchCapacity <= 0 {
		options.CompactBatchCapacity = DefaultOptions.CompactBatchCapacity
	}
	return nil
}

// waitMemtableSpace 等待单分片内存表空间（调用方持有 s.mu.Lock）。
func (db *DB) waitMemtableSpace(s *shard) error {
	return db.waitMemtableSpaceLocked([]*shard{s}, s)
}

// waitMemtableSpaceLocked 等待指定分片内存表空间；调用方必须按升序持有 lockedShards 全部写锁。
// 若分片不可变队列已满，先按逆序释放 lockedShards 等待刷新腾出空间再按升序重新加锁，杜绝与 flushMemtableE 死锁。
func (db *DB) waitMemtableSpaceLocked(lockedShards []*shard, s *shard) error {
	if err := db.flushError(); err != nil {
		return err
	}
	if !s.active.isFull() {
		return nil
	}

	maxImmu := db.options.MemtableNums - 1
	if maxImmu < 1 {
		maxImmu = 1
	}

	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for len(s.immu) >= maxImmu {
		s.notifyFlush()
		if timer == nil {
			timer = time.NewTimer(db.options.WaitMemSpaceTimeout)
		}
		for i := len(lockedShards) - 1; i >= 0; i-- {
			lockedShards[i].mu.Unlock()
		}
		var timedOut bool
		select {
		case <-s.spaceFreed:
		case <-db.flushStopCh:
		case <-timer.C:
			timedOut = true
		}
		for _, ls := range lockedShards {
			ls.mu.Lock()
		}
		if db.closed.Load() {
			return ErrDBClosed
		}
		if err := db.flushError(); err != nil {
			return err
		}
		if !s.active.isFull() {
			return nil
		}
		if timedOut && len(s.immu) >= maxImmu {
			return ErrWaitMemtableSpaceTimeOut
		}
	}

	nextOpts := s.active.options
	nextOpts.legacyExt = false
	nextOpts.legacyID = 0
	nextOpts.tableID = nextTableID(s.id, nextOpts.tableID)
	table, err := openMemtable(nextOpts)
	if err != nil {
		return err
	}
	s.immu = append(s.immu, s.active)
	s.active = table
	if s.id == 0 {
		db.activeMem = table
		db.immuMems = s.immu
	}
	db.memtablesMu.Lock()
	db.memtables[table.options.tableID] = table
	db.memtablesMu.Unlock()
	s.notifyFlush()
	return nil
}

func (db *DB) setFlushError(err error) {
	if err == nil {
		return
	}
	db.flushErrMu.Lock()
	if db.flushErr == nil {
		db.flushErr = err
	}
	db.flushErrMu.Unlock()
}

func (db *DB) flushError() error {
	db.flushErrMu.RLock()
	defer db.flushErrMu.RUnlock()
	if db.flushErr == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrFlushFailed, db.flushErr)
}

// flushMemtable 将指定的内存表刷新到磁盘。
// 将执行以下步骤：
// 1. 遍历内存表中的所有记录，将它们分为已删除的键和日志记录。
// 2. 将日志记录写入值日志，获取键的位置。
// 3. 添加旧的uuid，将所有键和位置写入索引。
// 4. 添加已删除的uuid，并从索引中删除已删除的键。
// 5. 删除WAL。
func (db *DB) flushMemtable(table *memtable) {
	if err := db.flushMemtableE(table); err != nil {
		db.setFlushError(err)
		log.Printf("flush memtable failed: %v", err)
	}
}

func (db *DB) flushMemtableE(table *memtable) error {
	db.flushLock.RLock()
	defer db.flushLock.RUnlock()
	s := db.shards[table.shardID]
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	sklIter := table.skl.NewIterator(false, 0)
	var deletedKeys [][]byte
	var logRecords []*ValueLogRecord
	nowMs := time.Now().UnixMilli()

	// 遍历内存表中的所有最新记录，将已删除与已过期的键归入 deletedKeys，存活键写入 VLog 并透传 ExpireAt。
	for sklIter.Rewind(); sklIter.Valid(); sklIter.Next() {
		key := append([]byte(nil), sklIter.Key()...)
		valueStruct := sklIter.Value()
		if valueStruct.Meta == LogRecordDeleted || (valueStruct.ExpireAt > 0 && valueStruct.ExpireAt <= nowMs) {
			deletedKeys = append(deletedKeys, key)
			if valueStruct.ExpireAt > 0 && valueStruct.ExpireAt <= nowMs {
				db.ttlIndex.remove(key)
			}
		} else {
			logRecord := ValueLogRecord{
				key:      key,
				value:    valueStruct.Value,
				uid:      uuid.New(),
				expireAt: valueStruct.ExpireAt,
			}
			logRecords = append(logRecords, &logRecord)
		}
	}
	_ = sklIter.Close()

	// 写入值日志，获取键的位置
	keyPos, err := db.vlog.writeBatch(logRecords)
	if err != nil {
		return fmt.Errorf("vlog writeBatch: %w", err)
	}

	// 同步值日志
	if err = db.vlog.sync(); err != nil {
		return fmt.Errorf("vlog sync: %w", err)
	}

	// 先加布隆，再写索引：避免“索引已可见、布隆还没加”期间读到假阴性。
	// 如果索引写失败，布隆最多多几个 key（假阳性），不会漏 key。
	if db.bloomManager != nil {
		for _, kp := range keyPos {
			db.bloomManager.AddKey(kp.key)
		}
	}

	// 优先使用单事务升序 ApplyBatch 同时提交 Put 与 Delete，减少 bbolt 事务与页分裂开销。
	if bt, ok := db.index.(*BPTree); ok {
		oldKeyPositions, err := bt.ApplyBatch(keyPos, deletedKeys)
		if err != nil {
			return fmt.Errorf("index ApplyBatch: %w", err)
		}
		for _, oldKeyPosition := range oldKeyPositions {
			db.vlog.setDiscard(oldKeyPosition.partition, oldKeyPosition.uid)
		}
	} else {
		oldKeyPositions, err := db.index.PutBatch(keyPos)
		if err != nil {
			return fmt.Errorf("index PutBatch: %w", err)
		}
		for _, oldKeyPosition := range oldKeyPositions {
			db.vlog.setDiscard(oldKeyPosition.partition, oldKeyPosition.uid)
		}
		if oldKeyPositions, err = db.index.DeleteBatch(deletedKeys); err != nil {
			return fmt.Errorf("index DeleteBatch: %w", err)
		}
		for _, oldKeyPosition := range oldKeyPositions {
			db.vlog.setDiscard(oldKeyPosition.partition, oldKeyPosition.uid)
		}
	}

	// 同步索引
	if err = db.index.Sync(); err != nil {
		return fmt.Errorf("index sync: %w", err)
	}

	// 删除WAL
	if err = table.deleteWAl(); err != nil {
		return fmt.Errorf("delete wal: %w", err)
	}
	db.releaseBatchRefs(table.batchIDs)

	// 删除内存中保存的旧内存表（严格使用闭包 defer Unlock 杜绝错误路径死锁）
	s = db.shards[table.shardID]
	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		oldTableID := table.options.tableID
		if table == s.active {
			nextOpts := s.active.options
			nextOpts.legacyExt = false
			nextOpts.legacyID = 0
			nextOpts.tableID = nextTableID(s.id, nextOpts.tableID)
			newTable, openErr := openMemtable(nextOpts)
			if openErr != nil {
				return fmt.Errorf("open active memtable: %w", openErr)
			}
			s.active = newTable
			if s.id == 0 {
				db.activeMem = newTable
			}
			db.memtablesMu.Lock()
			db.memtables[newTable.options.tableID] = newTable
			db.memtablesMu.Unlock()
		} else {
			for i, t := range s.immu {
				if t == table {
					s.immu = append(s.immu[:i], s.immu[i+1:]...)
					break
				}
			}
			if s.id == 0 {
				db.immuMems = s.immu
			}
		}
		db.memtablesMu.Lock()
		delete(db.memtables, oldTableID)
		db.memtablesMu.Unlock()
		return nil
	}(); err != nil {
		return err
	}
	s.notifySpaceFreed()
	db.sendThresholdState()
	if err := db.writeDBManifest(); err != nil {
		return fmt.Errorf("write manifest after flush: %w", err)
	}
	return nil
}

func (db *DB) sendThresholdState() {
	if db.options.AutoCompactSupport {
		// 检查废弃表大小；totalNumber == 0 时不触发压缩。
		totalNumber, discardNumber := db.vlog.stats()
		if totalNumber == 0 {
			return
		}
		lowerThreshold := uint64(float64(totalNumber) * float64(db.options.AdvisedCompactionRate))
		upperThreshold := uint64(float64(totalNumber) * float64(db.options.ForceCompactionRate))
		thresholdState := discardState{
			thresholdState: ThresholdState(UnarriveThreshold),
		}
		if discardNumber >= upperThreshold && upperThreshold > 0 {
			thresholdState = discardState{
				thresholdState: ThresholdState(ArriveForceThreshold),
			}
		} else if discardNumber > lowerThreshold {
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

// runShardFlusher 为单个分片按严格 FIFO 顺序刷新 immutable memtable，
// 消除多协程乱序刷盘覆盖新值或复活已删除键的风险，且不劫持进程信号。
func (db *DB) runShardFlusher(s *shard) {
	popNext := func() *memtable {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if len(s.immu) == 0 {
			return nil
		}
		return s.immu[0]
	}

	for {
		if next := popNext(); next != nil {
			db.flushMemtable(next)
			db.maybePruneCommitLog()
			continue
		}
		select {
		case <-s.flushNotify:
		case <-db.flushStopCh:
			for {
				next := popNext()
				if next == nil {
					return
				}
				db.flushMemtable(next)
				db.maybePruneCommitLog()
			}
		}
	}
}

// listenAutoCompact 动态检测每个分片的冗余情况，并根据当前 IO 状态决定是否进行压缩。
// 退出完全由 db.closeCompactChan 驱动，不在库内劫持进程 OS 信号。
//
//nolint:gocognit
func (db *DB) listenAutoCompact() {
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
		case <-ticker.C:
			if !nextRetryAt.IsZero() && time.Now().Before(nextRetryAt) {
				continue
			}
			if thresholdstate == ThresholdState(ArriveForceThreshold) {
				var err error
				if firstCompact {
					err = db.Compact()
					if err == nil {
						firstCompact = false
					}
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
						err = db.Compact()
						if err == nil {
							firstCompact = false
						}
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
					log.Println("IO busy, deferring advised compaction")
				}
			}
		}
	}
}

func (db *DB) listenDiskIOState() {
	for {
		select {
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

// Compact 复制索引仍引用的值，并在索引同步后回收旧段。
func (db *DB) Compact() error {
	return db.compact(false)
}

// CompactWithDiscardtable 用废弃表跳过已知的旧值。
func (db *DB) CompactWithDiscardtable() error {
	return db.compact(true)
}

type compactPendingItem struct {
	oldPos   *wal.ChunkPosition
	key      []byte
	uid      uuid.UUID
	expireAt int64
}

func (db *DB) compact(useDiscardTable bool) error {
	// 仅串行化压缩本身，不持有全局 flushLock，写入与后台 Memtable Flush 可全程并发执行。
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	if db.closed.Load() {
		return ErrDBClosed
	}

	cutoffSegments := make([]wal.SegmentID, len(db.vlog.walFiles))
	const maxCompactBatchBytes = 8 * MB
	batchLimit := max(1, db.vlog.options.compactBatchCapacity/len(cutoffSegments))
	if batchLimit > maxCompactBatchBytes {
		batchLimit = maxCompactBatchBytes
	}

	var compactGroup errgroup.Group
	for part, source := range db.vlog.walFiles {
		partIdx := part
		src := source
		compactGroup.Go(func() error {
			cutoffID, err := db.compactPartitionOnline(partIdx, src, batchLimit, useDiscardTable)
			cutoffSegments[partIdx] = cutoffID
			if err != nil {
				return fmt.Errorf("compact partition %d: %w", partIdx, err)
			}
			return nil
		})
	}
	if err := compactGroup.Wait(); err != nil {
		return err
	}
	if err := db.index.Sync(); err != nil {
		return fmt.Errorf("sync compacted index: %w", err)
	}

	// 旧索引视图可能仍被点读或迭代器持有，在 vlogGuard 写锁下安全回收已迁移的封存段。
	var reclaimErr error
	db.vlogGuard.Lock()
	for part, source := range db.vlog.walFiles {
		if cutoffSegments[part] == 0 {
			continue
		}
		if err := source.DeleteSegmentsBelow(cutoffSegments[part]); err != nil {
			reclaimErr = err
			break
		}
	}
	db.vlogGuard.Unlock()
	if reclaimErr != nil {
		return fmt.Errorf("reclaim compacted value log: %w", reclaimErr)
	}

	db.vlog.cleanDiscardTable()
	if err := db.writeDBManifest(); err != nil {
		log.Printf("write manifest after compact failed: %v", err)
	}
	return nil
}

// compactPartitionOnline 在线增量压缩单个 VLog 分区：
// 1. 轮转活跃段使所有待压缩段成为不可变封存段（< cutoffID），与并发 Flush 写入的新段天然隔离；
// 2. 分批（上限 8MB）扫描封存段中的存活且未过期记录，追加写回该分区当前活跃段并落盘；
// 3. 通过 BPTree.CompareAndSwapBatch 仅当索引仍指向 oldPos 时更新为 newPos，绝不覆盖并发新写入。
func (db *DB) compactPartitionOnline(part int, source *wal.WAL, batchLimit int, useDiscardTable bool) (wal.SegmentID, error) {
	if source.IsEmpty() {
		return 0, nil
	}
	if err := source.OpenNewActiveSegment(); err != nil {
		return 0, err
	}
	cutoffID := source.ActiveSegmentID()
	if cutoffID <= 1 {
		return 0, nil
	}

	reader := source.NewReaderWithMax(cutoffID - 1)
	var chunks [][]byte
	var pending []compactPendingItem
	var batchBytes int
	var unsyncedBytes int
	const syncThresholdBytes = 1 * MB
	nowMs := time.Now().UnixMilli()

	flushBatch := func(final bool) error {
		if len(chunks) == 0 {
			if final && unsyncedBytes > 0 {
				unsyncedBytes = 0
				return source.Sync()
			}
			return nil
		}
		positions, err := writeCompactedBatch(source, chunks)
		if err != nil {
			return err
		}
		unsyncedBytes += batchBytes
		if final || unsyncedBytes >= syncThresholdBytes {
			if err := source.Sync(); err != nil {
				return err
			}
			unsyncedBytes = 0
		}
		if bt, ok := db.index.(*BPTree); ok {
			updates := make([]indexCASUpdate, len(pending))
			for i, item := range pending {
				updates[i] = indexCASUpdate{
					oldPos: item.oldPos,
					newPos: &KeyPosition{
						key:       item.key,
						partition: uint32(part),
						uid:       item.uid,
						position:  positions[i],
						expireAt:  item.expireAt,
					},
				}
			}
			if err := bt.CompareAndSwapBatch(part, updates); err != nil {
				return err
			}
		} else {
			keyPositions := make([]*KeyPosition, len(pending))
			for i, item := range pending {
				keyPositions[i] = &KeyPosition{
					key:       item.key,
					partition: uint32(part),
					uid:       item.uid,
					position:  positions[i],
					expireAt:  item.expireAt,
				}
			}
			if _, err := db.index.PutBatch(keyPositions); err != nil {
				return err
			}
		}
		clear(chunks)
		chunks = chunks[:0]
		clear(pending)
		pending = pending[:0]
		batchBytes = 0
		return nil
	}

	for {
		chunk, pos, err := reader.Next()
		if errors.Is(err, io.EOF) {
			if err := flushBatch(true); err != nil {
				return 0, err
			}
			return cutoffID, nil
		}
		if err != nil {
			return 0, err
		}
		record := decodeValueLogRecord(chunk)
		if record == nil {
			return 0, fmt.Errorf("invalid value log record at %+v", pos)
		}
		if useDiscardTable && db.vlog.isDiscard(part, record.uid) {
			continue
		}
		current, err := db.index.Get(record.key)
		if err != nil {
			return 0, err
		}
		if current == nil || current.partition != uint32(part) || !sameChunkPosition(current.position, pos) {
			continue
		}
		if current.expireAt > 0 && current.expireAt <= nowMs {
			continue
		}
		chunks = append(chunks, chunk)
		pending = append(pending, compactPendingItem{
			oldPos:   pos,
			key:      record.key,
			uid:      record.uid,
			expireAt: current.expireAt,
		})
		batchBytes += len(chunk)
		if batchBytes >= batchLimit {
			if err := flushBatch(false); err != nil {
				return 0, err
			}
		}
	}
}

func writeCompactedBatch(target *wal.WAL, chunks [][]byte) ([]*wal.ChunkPosition, error) {
	positions, err := target.WriteBatch(chunks)
	if !errors.Is(err, wal.ErrPendingSizeTooLarge) {
		return positions, err
	}
	positions = make([]*wal.ChunkPosition, 0, len(chunks))
	for _, chunk := range chunks {
		pos, werr := target.Write(chunk)
		if werr != nil {
			return nil, werr
		}
		positions = append(positions, pos)
	}
	return positions, nil
}

func (db *DB) GetOptions() Options {
	return db.options
}

// 加载废弃条目元数据：
// - 不存在时立即原子写入 16 字节合法初始头（消除首次正常关闭前崩溃导致 0 字节文件砖库）；
// - 兼容旧版 8 字节 (2×uint32) 与新版 16 字节 (2×uint64)；
// - 若检测到 0 字节或损坏短文件，记录日志并自动归零重建而非拒绝启动。
func loadDiscardEntryMeta(discardMetaPath string) (uint64, uint64, error) {
	data, err := os.ReadFile(discardMetaPath)
	if os.IsNotExist(err) {
		if err := storeDiscardEntryMeta(discardMetaPath, 0, 0); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	switch len(data) {
	case 16:
		discardNumber := binary.LittleEndian.Uint64(data[0:8])
		totalEntryNumber := binary.LittleEndian.Uint64(data[8:16])
		return discardNumber, totalEntryNumber, nil
	case 8:
		discardNumber := uint64(binary.LittleEndian.Uint32(data[0:4]))
		totalEntryNumber := uint64(binary.LittleEndian.Uint32(data[4:8]))
		return discardNumber, totalEntryNumber, nil
	default:
		log.Printf("recovering corrupt/empty discard metadata (%d bytes) at %s", len(data), discardMetaPath)
		if err := storeDiscardEntryMeta(discardMetaPath, 0, 0); err != nil {
			return 0, 0, err
		}
		return 0, 0, nil
	}
}

// 持久化废弃数量和总条目数量（原子临时文件 + Sync + RenameDurable + SyncDir）。
func storeDiscardEntryMeta(discardMetaPath string, discardNumber uint64, totalNumber uint64) error {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], discardNumber)
	binary.LittleEndian.PutUint64(buf[8:16], totalNumber)

	tmpPath := discardMetaPath + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(buf[:]); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := util.RenameDurable(tmpPath, discardMetaPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return util.SyncDir(filepath.Dir(discardMetaPath))
}
