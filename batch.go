package knocidb

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Batch 是数据库的批量操作。
// 如果 readonly 为 true，只能通过 Get 方法从批处理中获取数据。
// 如果尝试使用 Put 或 Delete 方法，将会返回错误。
//
// 如果 readonly 为 false，可以使用 Put 和 Delete 方法向批处理写入数据。
// 当调用 Commit 方法时，数据将被写入数据库。
//
// Batch 不是事务，它不保证隔离性。
// 但它可以保证原子性、一致性和持久性（如果 Sync 选项为 true）。
//
// 必须调用 Commit 方法提交写入；Batch 不持有全局数据库锁。
type Batch struct {
	db            *DB
	pendingWrites map[string]*LogRecord
	pendingTTL    map[string]int64 // expireAt(ms); 0 表示移除 TTL
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool
}

var globalBatchIDSeq = func() *atomic.Uint64 {
	var seq atomic.Uint64
	seq.Store(uint64(time.Now().UnixNano()))
	return &seq
}()

// nextBatchID 无锁生成严格单调递增的批次 ID，替代全局互斥锁的 Snowflake。
func nextBatchID() uint64 {
	return globalBatchIDSeq.Add(1)
}

// NewBatch 创建一个新的 Batch 实例。
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:        db,
		options:   options,
		committed: false,
	}
	if !options.ReadOnly {
		batch.pendingWrites = make(map[string]*LogRecord)
		batch.pendingTTL = make(map[string]int64)
	}
	return batch
}

func makeBatch() interface{} {
	return &Batch{
		options: DefaultBatchOptions,
	}
}

func (b *Batch) init(rdonly, sync bool, disableWal bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.options.DisableWal = disableWal
	b.db = db
	if !rdonly {
		if b.pendingWrites == nil {
			b.pendingWrites = make(map[string]*LogRecord)
		}
		if b.pendingTTL == nil {
			b.pendingTTL = make(map[string]int64)
		}
	}
	return b
}

func (b *Batch) withPendingWrites() {
	b.pendingWrites = make(map[string]*LogRecord)
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = nil
	b.pendingTTL = nil
	b.committed = false
}

// Close 保留给调用方显式释放 Batch；当前 Batch 不持有全局锁。
func (b *Batch) Close() {}

func (b *Batch) lockShards(shards []*shard) {
	for _, s := range shards {
		s.mu.Lock()
	}
}

func (b *Batch) unlockShards(shards []*shard) {
	for i := len(shards) - 1; i >= 0; i-- {
		shards[i].mu.Unlock()
	}
}

// Put 向批处理添加一个键值对以进行写入（自动清除该键原有的 TTL）。
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	recordType := LogRecordNormal
	if value != nil && len(value) == 0 {
		// 用独立类型区分“键存在但值为空”和“键不存在”；nil 保持旧语义（不可读）
		recordType = LogRecordNormalEmpty
	}

	b.mu.Lock()
	b.pendingWrites[string(key)] = &LogRecord{
		Key:      key,
		Value:    value,
		Type:     recordType,
		ExpireAt: 0,
	}
	delete(b.pendingTTL, string(key))
	b.mu.Unlock()

	return nil
}

// PutWithTTL 写入 key/value，并设置相对 TTL。
// ttl <= 0 返回错误；要移除 TTL 请使用 Persist。
func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	return b.PutWithExpireAt(key, value, time.Now().Add(ttl).UnixMilli())
}

// PutWithExpireAt 写入 key/value 并设置绝对过期时间戳（UnixMilli），供 Raft 状态机确定性复制 TTL。
func (b *Batch) PutWithExpireAt(key []byte, value []byte, expireAt int64) error {
	if err := b.Put(key, value); err != nil {
		return err
	}
	if expireAt <= 0 {
		return nil
	}
	b.mu.Lock()
	b.pendingWrites[string(key)].ExpireAt = expireAt
	b.mu.Unlock()
	return nil
}

// SetTTL 只修改已有 key 的过期时间，不写数据。
func (b *Batch) SetTTL(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	expireAt := time.Now().Add(ttl).UnixMilli()
	b.mu.Lock()
	if rec, ok := b.pendingWrites[string(key)]; ok && rec.Type != LogRecordDeleted {
		rec.ExpireAt = expireAt
	} else {
		b.pendingTTL[string(key)] = expireAt
	}
	b.mu.Unlock()
	return nil
}

// Persist 移除 key 的 TTL。
func (b *Batch) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	b.mu.Lock()
	if rec, ok := b.pendingWrites[string(key)]; ok && rec.Type != LogRecordDeleted {
		rec.ExpireAt = 0
	} else {
		b.pendingTTL[string(key)] = 0
	}
	b.mu.Unlock()
	return nil
}

// Get 从批处理中获取与给定键关联的值。
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed.Load() {
		return nil, ErrDBClosed
	}

	now := time.Now().UnixMilli()
	// 从 pendingWrites 获取
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			if record.ExpireAt > 0 && record.ExpireAt <= now {
				return nil, ErrKeyNotFound
			}
			switch record.Type {
			case LogRecordDeleted:
				return nil, ErrKeyNotFound
			case LogRecordNormalEmpty:
				return []byte{}, nil
			default:
				if record.Value == nil {
					return nil, ErrKeyNotFound
				}
				return record.Value, nil
			}
		}
		if expireAt, ok := b.pendingTTL[string(key)]; ok {
			b.mu.RUnlock()
			if expireAt > 0 && expireAt <= now {
				return nil, ErrKeyNotFound
			}
		} else {
			b.mu.RUnlock()
		}
	}

	s := b.db.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if b.db.closed.Load() {
		return nil, ErrDBClosed
	}

	val, empty, _, found, err := b.db.getLiveRecordLocked(s, key, now, true)
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

// Delete 在批处理中标记一个键为删除状态。
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	b.pendingWrites[string(key)] = &LogRecord{
		Key:      key,
		Type:     LogRecordDeleted,
		ExpireAt: 0,
	}
	delete(b.pendingTTL, string(key))
	b.mu.Unlock()

	return nil
}

// Exist 检查键是否存在于数据库中。
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed.Load() {
		return false, ErrDBClosed
	}

	now := time.Now().UnixMilli()
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			if record.Type == LogRecordDeleted {
				return false, nil
			}
			if record.ExpireAt > 0 && record.ExpireAt <= now {
				return false, nil
			}
			if record.Type == LogRecordNormal && record.Value == nil {
				return false, nil
			}
			return true, nil
		}
		if expireAt, ok := b.pendingTTL[string(key)]; ok {
			b.mu.RUnlock()
			if expireAt > 0 && expireAt <= now {
				return false, nil
			}
		} else {
			b.mu.RUnlock()
		}
	}

	s := b.db.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if b.db.closed.Load() {
		return false, ErrDBClosed
	}

	_, _, _, found, err := b.db.getLiveRecordLocked(s, key, now, false)
	return found, err
}

// Commit 提交批处理，如果批处理是只读的或为空，它将直接返回。
//
// 所有数据与内嵌 TTL 统一按 shard 分组写入 WAL 与 memtable，
// 彻底消除独立 TTL.DB 的非原子双写与同步 fsync 瓶颈。
func (b *Batch) Commit() error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}

	if b.options.ReadOnly || (len(b.pendingWrites) == 0 && len(b.pendingTTL) == 0) {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 检查是否已提交
	if b.committed {
		return ErrBatchCommitted
	}

	// 单条且无独立 SetTTL/Persist 的写入只需一个分片锁和一次 WAL 准备。
	if len(b.pendingWrites) == 1 && len(b.pendingTTL) == 0 {
		var key string
		var record *LogRecord
		for key, record = range b.pendingWrites {
		}
		s := b.db.shardFor([]byte(key))
		b.lockShards([]*shard{s})
		defer b.unlockShards([]*shard{s})
		if b.db.closed.Load() {
			return ErrDBClosed
		}
		batchID := nextBatchID()
		if err := b.db.waitMemtableSpace(s); err != nil {
			return err
		}
		if err := s.active.prepareRecord(record, batchID, 1, b.options.WriteOptions); err != nil {
			return err
		}
		s.active.applyRecordWithMode(record, b.db.seq.Add(1), b.db.activeIters.Load() == 0)
		b.db.ttlIndex.set(record.Key, record.ExpireAt)
		b.committed = true
		return nil
	}

	// 同一分片的多 key 批次快速路径（无独立 SetTTL/Persist）。
	if len(b.pendingTTL) == 0 {
		var single *shard
		for key := range b.pendingWrites {
			s := b.db.shardFor([]byte(key))
			if single == nil {
				single = s
				continue
			}
			if s.id != single.id {
				single = nil
				break
			}
		}
		if single != nil {
			b.lockShards([]*shard{single})
			defer b.unlockShards([]*shard{single})
			if b.db.closed.Load() {
				return ErrDBClosed
			}
			batchID := nextBatchID()
			if err := b.db.waitMemtableSpace(single); err != nil {
				return err
			}
			if err := single.active.prepareBatch(b.pendingWrites, batchID, b.options.WriteOptions, 1); err != nil {
				return err
			}
			single.active.applyBatchWithMode(b.pendingWrites, b.db.seq.Add(1), b.db.activeIters.Load() == 0)
			for _, rec := range b.pendingWrites {
				b.db.ttlIndex.set(rec.Key, rec.ExpireAt)
			}
			b.committed = true
			return nil
		}
	}

	// 通用多 shard（或含 SetTTL/Persist）路径：按升序锁定所有涉及分片。
	shardSet := make(map[uint32]*shard)
	for key := range b.pendingWrites {
		s := b.db.shardFor([]byte(key))
		shardSet[s.id] = s
	}
	for key := range b.pendingTTL {
		s := b.db.shardFor([]byte(key))
		shardSet[s.id] = s
	}
	shards := make([]*shard, 0, len(shardSet))
	for _, s := range shardSet {
		shards = append(shards, s)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i].id < shards[j].id })
	b.lockShards(shards)
	defer b.unlockShards(shards)
	if b.db.closed.Load() {
		return ErrDBClosed
	}

	byShard := make(map[uint32]map[string]*LogRecord, len(shards))
	for key, record := range b.pendingWrites {
		s := b.db.shardFor([]byte(key))
		if byShard[s.id] == nil {
			byShard[s.id] = make(map[string]*LogRecord)
		}
		byShard[s.id][key] = record
	}

	// 解析仅修改 TTL（SetTTL/Persist）的键：在分片写锁下读出当前存活值，统一并入原子写入。
	now := time.Now().UnixMilli()
	for key, expireAt := range b.pendingTTL {
		if _, hasWrite := b.pendingWrites[key]; hasWrite {
			continue
		}
		kBytes := []byte(key)
		s := b.db.shardFor(kBytes)
		val, empty, _, found, err := b.db.getLiveRecordLocked(s, kBytes, now, true)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		recType := LogRecordNormal
		if empty {
			recType = LogRecordNormalEmpty
			val = []byte{}
		}
		if byShard[s.id] == nil {
			byShard[s.id] = make(map[string]*LogRecord)
		}
		byShard[s.id][key] = &LogRecord{
			Key:      kBytes,
			Value:    val,
			Type:     recType,
			ExpireAt: expireAt,
		}
	}

	// 只保留真正有记录的分片。
	activeShards := make([]*shard, 0, len(byShard))
	for _, s := range shards {
		if len(byShard[s.id]) > 0 {
			activeShards = append(activeShards, s)
		}
	}
	if len(activeShards) == 0 {
		b.committed = true
		return nil
	}

	batchID := nextBatchID()
	for _, s := range activeShards {
		if err := b.db.waitMemtableSpaceLocked(shards, s); err != nil {
			return err
		}
	}
	for _, s := range activeShards {
		if err := s.active.prepareBatch(byShard[s.id], batchID, b.options.WriteOptions, len(activeShards)); err != nil {
			return err
		}
	}
	if !b.options.DisableWal && len(activeShards) > 1 {
		if err := b.db.commitBatch(batchID, len(activeShards), b.options.Sync); err != nil {
			return err
		}
	}
	seq := b.db.seq.Add(1)
	canReplace := b.db.activeIters.Load() == 0
	for _, s := range activeShards {
		s.active.applyBatchWithMode(byShard[s.id], seq, canReplace)
		for _, rec := range byShard[s.id] {
			b.db.ttlIndex.set(rec.Key, rec.ExpireAt)
		}
	}

	b.committed = true
	return nil
}
