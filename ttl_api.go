package knocidb

import (
	"fmt"
	"time"
)

// PutWithTTL 写入一个带相对过期时间的 key/value（内嵌元数据，单分片原子落盘，无独立 TTL.DB 同步瓶颈）。
// ttl <= 0 返回错误；要移除 TTL 请使用 Persist。
func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	return db.putWithOptionsAndTTL(key, value, DefaultWriteOptions, time.Now().Add(ttl).UnixMilli())
}

// PutWithExpireAt 写入一个带绝对过期时间戳（UnixMilli）的 key/value，供 Raft 状态机确定性复制 TTL。
func (db *DB) PutWithExpireAt(key, value []byte, expireAt int64) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	return db.putWithOptionsAndTTL(key, value, DefaultWriteOptions, expireAt)
}

// SetTTL 给已经存在的 key 设置相对 TTL（在单分片写锁内原子读改写，消除 TOCTOU）。
// 返回 false 表示 key 不存在或已过期。
func (db *DB) SetTTL(key []byte, ttl time.Duration) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if ttl <= 0 {
		return false, fmt.Errorf("ttl must be positive")
	}
	if db.closed.Load() {
		return false, ErrDBClosed
	}

	s := db.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if db.closed.Load() {
		return false, ErrDBClosed
	}
	now := time.Now().UnixMilli()
	val, empty, _, found, err := db.getLiveRecordLocked(s, key, now, true)
	if err != nil || !found {
		return false, err
	}
	if err := db.waitMemtableSpace(s); err != nil {
		return false, err
	}
	recType := LogRecordNormal
	if empty {
		recType = LogRecordNormalEmpty
		val = []byte{}
	}
	expireAt := time.Now().Add(ttl).UnixMilli()
	record := &LogRecord{
		Key:      key,
		Value:    val,
		Type:     recType,
		ExpireAt: expireAt,
	}
	batchID := nextBatchID()
	if err := s.active.prepareRecord(record, batchID, 1, DefaultWriteOptions); err != nil {
		return false, err
	}
	s.active.applyRecordWithMode(record, db.seq.Add(1), db.activeIters.Load() == 0)
	db.ttlIndex.set(key, expireAt)
	return true, nil
}

// GetTTL 返回 key 的剩余 TTL（直接从 memtable/索引内嵌元数据读取，无需读 VLog）。
// exists 为 false 表示 key 不存在、已过期或没有设置 TTL。
func (db *DB) GetTTL(key []byte) (time.Duration, bool, error) {
	if len(key) == 0 {
		return 0, false, ErrKeyIsEmpty
	}
	if db.closed.Load() {
		return 0, false, ErrDBClosed
	}
	s := db.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if db.closed.Load() {
		return 0, false, ErrDBClosed
	}
	now := time.Now().UnixMilli()
	_, _, expireAt, found, err := db.getLiveRecordLocked(s, key, now, false)
	if err != nil || !found || expireAt <= 0 {
		return 0, false, err
	}
	left := time.Until(time.UnixMilli(expireAt))
	if left <= 0 {
		return 0, false, nil
	}
	return left, true, nil
}

// Persist 移除 key 的 TTL（在单分片写锁内原子读改写，消除 TOCTOU）。
// 返回 false 表示 key 不存在、已过期或本来就没有 TTL。
func (db *DB) Persist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if db.closed.Load() {
		return false, ErrDBClosed
	}

	s := db.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if db.closed.Load() {
		return false, ErrDBClosed
	}
	now := time.Now().UnixMilli()
	val, empty, expireAt, found, err := db.getLiveRecordLocked(s, key, now, true)
	if err != nil || !found || expireAt <= 0 {
		return false, err
	}
	if err := db.waitMemtableSpace(s); err != nil {
		return false, err
	}
	recType := LogRecordNormal
	if empty {
		recType = LogRecordNormalEmpty
		val = []byte{}
	}
	record := &LogRecord{
		Key:      key,
		Value:    val,
		Type:     recType,
		ExpireAt: 0,
	}
	batchID := nextBatchID()
	if err := s.active.prepareRecord(record, batchID, 1, DefaultWriteOptions); err != nil {
		return false, err
	}
	s.active.applyRecordWithMode(record, db.seq.Add(1), db.activeIters.Load() == 0)
	db.ttlIndex.remove(key)
	return true, nil
}
