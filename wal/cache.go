package wal

import (
	"sync"
)

const blockCacheShards = 16

type blockCacheKey struct {
	segID    SegmentID
	blockNum uint32
}

type blockCacheEntry struct {
	key  blockCacheKey
	data []byte
}

type blockCacheShard struct {
	mu      sync.RWMutex
	maxCap  int
	entries map[blockCacheKey][]byte
	order   []blockCacheKey // FIFO eviction ring
	head    int
}

// blockCache 是面向不可变 32KB WAL/VLog 满块的分片内存缓存，用于加速热数据点读。
// 仅缓存已写满 32KB 的块（append-only 段中满块永不变更），天然无脏读风险。
type blockCache struct {
	shards [blockCacheShards]blockCacheShard
}

func newBlockCache(totalBytes int64) *blockCache {
	if totalBytes < blockSize {
		return nil
	}
	totalBlocks := int(totalBytes / blockSize)
	perShard := totalBlocks / blockCacheShards
	if perShard < 1 {
		perShard = 1
	}
	c := &blockCache{}
	for i := range c.shards {
		c.shards[i] = blockCacheShard{
			maxCap:  perShard,
			entries: make(map[blockCacheKey][]byte, perShard),
			order:   make([]blockCacheKey, 0, perShard),
		}
	}
	return c
}

func (c *blockCache) shardFor(k blockCacheKey) *blockCacheShard {
	h := uint64(k.segID)*2654435761 ^ uint64(k.blockNum)*2246822519
	return &c.shards[h&(blockCacheShards-1)]
}

func (c *blockCache) get(segID SegmentID, blockNum uint32) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	k := blockCacheKey{segID: segID, blockNum: blockNum}
	s := c.shardFor(k)
	s.mu.RLock()
	data, ok := s.entries[k]
	s.mu.RUnlock()
	return data, ok
}

func (c *blockCache) put(segID SegmentID, blockNum uint32, block []byte) {
	if c == nil || len(block) != blockSize {
		return
	}
	k := blockCacheKey{segID: segID, blockNum: blockNum}
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[k]; exists {
		return
	}
	copied := make([]byte, blockSize)
	copy(copied, block)
	if len(s.order) < s.maxCap {
		s.order = append(s.order, k)
	} else {
		evict := s.order[s.head]
		delete(s.entries, evict)
		s.order[s.head] = k
		s.head = (s.head + 1) % s.maxCap
	}
	s.entries[k] = copied
}

func (c *blockCache) evictSegment(segID SegmentID) {
	if c == nil {
		return
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for k := range s.entries {
			if k.segID == segID {
				delete(s.entries, k)
			}
		}
		s.mu.Unlock()
	}
}
