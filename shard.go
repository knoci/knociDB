package knocidb

import "sync"

// shard 是本地写入并发的最小单元。一个 shard 拥有独立的
// active/immutable memtable 和 WAL；索引与 ValueLog 仍复用分区文件。
type shard struct {
	id          uint32
	mu          sync.RWMutex
	flushMu     sync.Mutex
	active      *memtable
	immu        []*memtable
	flushNotify chan struct{}
	spaceFreed  chan struct{}
}

func newShard(id uint32) *shard {
	return &shard{
		id:          id,
		flushNotify: make(chan struct{}, 1),
		spaceFreed:  make(chan struct{}, 1),
	}
}

func (s *shard) notifyFlush() {
	if s.flushNotify == nil {
		return
	}
	select {
	case s.flushNotify <- struct{}{}:
	default:
	}
}

func (s *shard) notifySpaceFreed() {
	if s.spaceFreed == nil {
		return
	}
	select {
	case s.spaceFreed <- struct{}{}:
	default:
	}
}

// memTables 返回 newest-first 的内存表顺序，供点读使用。
// Iterator 使用 immu 的 oldest-first 顺序，保证同 key 的版本号更大者更新。
func (s *shard) memTables() []*memtable {
	tables := make([]*memtable, 0, len(s.immu)+1)
	tables = append(tables, s.active)
	for i := len(s.immu) - 1; i >= 0; i-- {
		tables = append(tables, s.immu[i])
	}
	return tables
}

func (db *DB) lockAllShards() []*shard {
	for _, s := range db.shards {
		s.mu.Lock()
	}
	return db.shards
}

func (db *DB) unlockAllShards(shards []*shard) {
	for i := len(shards) - 1; i >= 0; i-- {
		shards[i].mu.Unlock()
	}
}

func (db *DB) shardFor(key []byte) *shard {
	hashFn := db.options.KeyHashFunction
	if hashFn == nil {
		hashFn = DefaultOptions.KeyHashFunction
	}
	id := hashFn(key) % uint64(len(db.shards))
	return db.shards[id]
}

const (
	legacyTableIDShardShift = 16
	tableIDShardShift       = 32
	tableIDLocalMask        = uint64(0xffffffff)
)

func makeTableID(shardID uint32, localID uint32) uint64 {
	return (uint64(shardID) << tableIDShardShift) | uint64(localID)
}

func nextTableID(shardID uint32, current uint64) uint64 {
	nextLocal := uint32((current & tableIDLocalMask) + 1)
	if nextLocal == 0 {
		nextLocal = 1
	}
	return makeTableID(shardID, nextLocal)
}

func tableShardID(tableID uint64) uint32 {
	return uint32(tableID >> tableIDShardShift)
}

func tableLocalID(tableID uint64) uint32 {
	return uint32(tableID & tableIDLocalMask)
}
