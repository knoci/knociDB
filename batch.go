package knocidb

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"knocidb/diskhash"
	"sync"
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
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool
	batchID       *snowflake.Node
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
	b.lock()
	return b
}

func (b *Batch) withPendingWrites() {
	b.pendingWrites = make(map[string]*LogRecord)
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = nil
	b.committed = false
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
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

	b.mu.Lock()
	// 写入 pendingWrites
	b.pendingWrites[string(key)] = &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
	b.mu.Unlock()

	return nil
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
			if record.Type == LogRecordDeleted {
				b.mu.RUnlock()
				return nil, ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Value, nil
		}
		b.mu.RUnlock()
	}

	// 从内存表获取
	tables := b.db.getMemTables()
	for _, table := range tables {
		deleted, value := table.get(key)
		if deleted {
			return nil, ErrKeyNotFound
		}
		if len(value) != 0 {
			return value, nil
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
			return record.Type != LogRecordDeleted, nil
		}
		b.mu.RUnlock()
	}

	// 从内存表获取
	tables := b.db.getMemTables()
	for _, table := range tables {
		deleted, value := table.get(key)
		if deleted {
			return false, nil
		}
		if len(value) != 0 {
			return true, nil
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

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 检查是否已提交
	if b.committed {
		return ErrBatchCommitted
	}

	// 等待内存表空间
	if err := b.db.waitMemtableSpace(); err != nil {
		return err
	}
	batchID := b.batchID.Generate()
	// 调用内存表的批量写入
	err := b.db.activeMem.putBatch(b.pendingWrites, batchID, b.options.WriteOptions)
	if err != nil {
		return err
	}

	b.committed = true
	return nil
}
