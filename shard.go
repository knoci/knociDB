package knocidb

import "sync"

// shard 是本地写入并发的最小单元。一个 shard 拥有独立的
// active/immutable memtable 和 WAL；索引与 ValueLog 仍复用分区文件。
type shard struct {
	id     uint32
	mu     sync.RWMutex
	active *memtable
	immu   []*memtable
}

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

const tableIDShardShift = 16

func makeTableID(shardID, localID uint32) uint32 {
	return shardID<<tableIDShardShift | localID
}

func tableShardID(tableID uint32) uint32 {
	return tableID >> tableIDShardShift
}
