package knocidb

import (
	"fmt"
	"time"

	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/knoci/knocidb/diskhash"
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
// 必须调用 Commit 方法来提交批处理，否则数据库将被锁定。
type Batch struct {
	db            *DB
	pendingWrites map[string]*LogRecord
	pendingTTL    map[string]int64 // expireAt(ms); 0 表示删除 TTL
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool
	batchID       *snowflake.Node
	locked        bool
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
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchID = node
	}
	batch.lock()
	return batch
}

func makeBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchID: node,
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
	b.lock()
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

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
	b.locked = true
}

func (b *Batch) unlock() {
	if !b.locked {
		return
	}
	b.locked = false
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Close 释放 Batch 持有的数据库锁
func (b *Batch) Close() { b.unlock() }

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

	// 从内存表获取
	tables := b.db.getMemTables()
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

	// 从索引获取
	var value []byte
	var matchKey func(diskhash.Slot) (bool, error)
	if b.db.options.IndexType == Hash {
		matchKey = MatchKeyFunc(b.db, key, nil, &value)
	}

	position, err := b.db.index.Get(key, matchKey)
	if err != nil {
		return nil, err
	}

	if b.db.options.IndexType == Hash {
		if value == nil {
			return nil, ErrKeyNotFound
		}
		return value, nil
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

	// 从内存表获取
	tables := b.db.getMemTables()
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
	var value []byte
	var matchKeyFunc func(diskhash.Slot) (bool, error)
	if b.db.options.IndexType == Hash {
		matchKeyFunc = MatchKeyFunc(b.db, key, nil, &value)
	}
	pos, err := b.db.index.Get(key, matchKeyFunc)
	if err != nil {
		return false, err
	}
	if b.db.options.IndexType == Hash {
		return value != nil, nil
	}
	return pos != nil, nil
}

// Commit 提交批处理，如果批处理是只读的或为空，它将直接返回。
//
// 它将遍历 pendingWrites 并将数据写入数据库，
// 然后写入一条记录以表示批处理的结束，以保证原子性。
// 最后，它将写入索引。
func (b *Batch) Commit() error {
	defer b.unlock()
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
		// 等待内存表空间
		if err := b.db.waitMemtableSpace(); err != nil {
			return err
		}
		batchID := b.batchID.Generate()
		// 调用内存表的批量写入
		if err := b.db.activeMem.putBatch(b.pendingWrites, batchID, b.options.WriteOptions); err != nil {
			return err
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
