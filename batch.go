package knocidb

import (
	"fmt"
	"sort"
	"time"

	"sync"

	"github.com/bwmarrin/snowflake"
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
	pendingTTL    map[string]int64 // expireAt(ms); 0 表示删除 TTL
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool
	batchID       *snowflake.Node
}

var globalBatchIDNode = func() *snowflake.Node {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return node
}()

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
		batch.batchID = globalBatchIDNode
	}
	return batch
}

func makeBatch() interface{} {
	return &Batch{
		options: DefaultBatchOptions,
		batchID: globalBatchIDNode,
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

// Put 向批处理添加一个键值对以进行写入。
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
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
	// 写入 pendingWrites
	b.pendingWrites[string(key)] = &LogRecord{
		Key:   key,
		Value: value,
		Type:  recordType,
	}
	b.mu.Unlock()

	return nil
}

// PutWithTTL 写入 key/value，并设置相对 TTL。
// ttl <= 0 返回错误；要移除 TTL 请使用 Persist。
func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	if err := b.Put(key, value); err != nil {
		return err
	}
	b.mu.Lock()
	b.pendingTTL[string(key)] = time.Now().Add(ttl).UnixMilli()
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
	b.mu.Lock()
	b.pendingTTL[string(key)] = time.Now().Add(ttl).UnixMilli()
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
	b.pendingTTL[string(key)] = 0
	b.mu.Unlock()
	return nil
}

func (b *Batch) ttlExpired(key []byte) (bool, error) {
	now := time.Now().UnixMilli()
	b.mu.RLock()
	if expireAt, ok := b.pendingTTL[string(key)]; ok {
		b.mu.RUnlock()
		return expireAt > 0 && expireAt <= now, nil
	}
	b.mu.RUnlock()
	if b.db.ttl == nil {
		return false, nil
	}
	return b.db.ttl.isExpired(key, now)
}

// Get 从批处理中获取与给定键关联的值。
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	// 从 pendingWrites 获取
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
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
		b.mu.RUnlock()
	}

	expired, err := b.ttlExpired(key)
	if err != nil {
		return nil, err
	}
	if expired {
		return nil, ErrKeyNotFound
	}

	s := b.db.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if b.db.closed {
		return nil, ErrDBClosed
	}

	// 从内存表获取
	tables := s.memTables()
	for _, table := range tables {
		deleted, empty, value := table.getWithState(key)
		if deleted {
			return nil, ErrKeyNotFound
		}
		if empty {
			return []byte{}, nil
		}
		if len(value) != 0 {
			return value, nil
		}
	}

	// 使用布隆过滤器快速检查键是否可能存在
	if b.db.bloomManager != nil {
		if !b.db.bloomManager.TestKey(key) {
			// 布隆过滤器表明键绝对不存在
			return nil, ErrKeyNotFound
		}
	}

	b.db.vlogGuard.RLock()
	defer b.db.vlogGuard.RUnlock()

	// 从索引获取
	position, err := b.db.index.Get(key)
	if err != nil {
		return nil, err
	}

	if position == nil {
		return nil, ErrKeyNotFound
	}
	record, err := b.db.vlog.read(position)
	if err != nil {
		return nil, err
	}
	return record.value, nil
}

// Delete 在批处理中标记一个键为删除状态。
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	b.pendingWrites[string(key)] = &LogRecord{
		Key:  key,
		Type: LogRecordDeleted,
	}
	if _, ok := b.pendingTTL[string(key)]; ok {
		b.pendingTTL[string(key)] = 0
	} else if b.db.ttl != nil && b.db.ttl.has(key) {
		b.pendingTTL[string(key)] = 0
	}
	b.mu.Unlock()

	return nil
}

// Exist 检查键是否存在于数据库中。
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	// 检查键是否存在于 pendingWrites 中
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			if record.Type == LogRecordDeleted {
				return false, nil
			}
			if record.Type == LogRecordNormal && record.Value == nil {
				return false, nil
			}
			return true, nil
		}
		b.mu.RUnlock()
	}

	expired, err := b.ttlExpired(key)
	if err != nil {
		return false, err
	}
	if expired {
		return false, nil
	}

	s := b.db.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if b.db.closed {
		return false, ErrDBClosed
	}

	// 从内存表获取
	tables := s.memTables()
	for _, table := range tables {
		deleted, empty, value := table.getWithState(key)
		if deleted {
			return false, nil
		}
		if empty || len(value) != 0 {
			return true, nil
		}
	}

	// 使用布隆过滤器快速检查键是否可能存在
	if b.db.bloomManager != nil {
		if !b.db.bloomManager.TestKey(key) {
			// 布隆过滤器表明键绝对不存在
			return false, nil
		}
	}

	// 检查键是否存在于索引中
	pos, err := b.db.index.Get(key)
	if err != nil {
		return false, err
	}
	return pos != nil, nil
}

// Commit 提交批处理，如果批处理是只读的或为空，它将直接返回。
//
// 它按 shard 分组写入 WAL，追加跨 shard 的 COMMIT 标记，
// 全部 WAL 成功后再应用到各 shard 的 memtable；索引在 flush 时写入。
func (b *Batch) Commit() error {
	if b.db.closed {
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

	// 单条且无 TTL 的写入只需一个分片锁和一次 WAL 准备，
	// 不需要构建分组/排序 map。
	if len(b.pendingWrites) == 1 && len(b.pendingTTL) == 0 {
		var key string
		var record *LogRecord
		for key, record = range b.pendingWrites {
		}
		s := b.db.shardFor([]byte(key))
		b.lockShards([]*shard{s})
		defer b.unlockShards([]*shard{s})
		if b.db.closed {
			return ErrDBClosed
		}
		batchID := b.batchID.Generate()
		if err := b.db.waitMemtableSpace(s); err != nil {
			return err
		}
		if err := s.active.prepareRecord(record, batchID, 1, b.options.WriteOptions); err != nil {
			return err
		}
		s.active.applyRecord(record)
		b.committed = true
		return nil
	}

	// 写入数据之前先落“设置 TTL”；崩溃时最多留下一个指向不存在 key 的 TTL。
	sets := make(map[string]int64)
	deletes := make(map[string]int64)
	for key, expireAt := range b.pendingTTL {
		if expireAt > 0 {
			sets[key] = expireAt
		} else {
			deletes[key] = 0
		}
	}
	if b.db.ttl != nil {
		if err := b.db.ttl.apply(sets); err != nil {
			return err
		}
	}

	if len(b.pendingWrites) > 0 {
		byShard := make(map[uint32]map[string]*LogRecord)
		for key, record := range b.pendingWrites {
			s := b.db.shardFor([]byte(key))
			if byShard[s.id] == nil {
				byShard[s.id] = make(map[string]*LogRecord)
			}
			byShard[s.id][key] = record
		}
		shards := make([]*shard, 0, len(byShard))
		for id := range byShard {
			shards = append(shards, b.db.shards[id])
		}
		sort.Slice(shards, func(i, j int) bool { return shards[i].id < shards[j].id })
		b.lockShards(shards)
		defer b.unlockShards(shards)
		if b.db.closed {
			return ErrDBClosed
		}

		batchID := b.batchID.Generate()
		for _, s := range shards {
			if err := b.db.waitMemtableSpace(s); err != nil {
				return err
			}
			if err := s.active.prepareBatch(byShard[s.id], batchID, b.options.WriteOptions, len(shards)); err != nil {
				return err
			}
		}
		if !b.options.DisableWal && len(shards) > 1 {
			b.db.addBatchRefs(uint64(batchID), len(shards))
			if err := b.db.commitLog.append(uint64(batchID), b.options.Sync); err != nil {
				return err
			}
		}
		for _, s := range shards {
			s.active.applyBatch(byShard[s.id])
		}
	}

	// 数据提交后再删除 TTL；崩溃时最多留下一个过期/孤儿 TTL。
	if b.db.ttl != nil {
		if err := b.db.ttl.apply(deletes); err != nil {
			return err
		}
	}

	b.committed = true
	return nil
}
