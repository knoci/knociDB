package knocidb

import (
	"fmt"
	"time"
)

// PutWithTTL 写入一个带相对过期时间的 key/value。
// ttl <= 0 返回错误；要移除 TTL 请使用 Persist。
func (db *DB) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	batch := db.NewBatch(DefaultBatchOptions)
	defer batch.Close()
	if err := batch.PutWithTTL(key, value, ttl); err != nil {
		return err
	}
	return batch.Commit()
}

// SetTTL 给已经存在的 key 设置相对 TTL。
// 返回 false 表示 key 不存在。
func (db *DB) SetTTL(key []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("ttl must be positive")
	}
	batch := db.NewBatch(DefaultBatchOptions)
	defer batch.Close()
	exists, err := batch.Exist(key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if err := batch.SetTTL(key, ttl); err != nil {
		return false, err
	}
	if err := batch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// GetTTL 返回 key 的剩余 TTL。
// exists 为 false 表示 key 不存在或没有设置 TTL。
func (db *DB) GetTTL(key []byte) (time.Duration, bool, error) {
	exists, err := db.Exist(key)
	if err != nil || !exists {
		return 0, false, err
	}
	if db.ttl == nil {
		return 0, false, nil
	}
	expireAt, hasTTL, err := db.ttl.get(key)
	if err != nil || !hasTTL || expireAt == 0 {
		return 0, false, err
	}
	left := time.Until(time.UnixMilli(expireAt))
	if left <= 0 {
		return 0, false, nil
	}
	return left, true, nil
}

// Persist 移除 key 的 TTL。返回 false 表示 key 不存在或本来就没有 TTL。
func (db *DB) Persist(key []byte) (bool, error) {
	batch := db.NewBatch(DefaultBatchOptions)
	defer batch.Close()
	exists, err := batch.Exist(key)
	if err != nil || !exists {
		return false, err
	}
	if db.ttl == nil {
		return false, nil
	}
	_, hasTTL, err := db.ttl.get(key)
	if err != nil || !hasTTL {
		return false, err
	}
	if err := batch.Persist(key); err != nil {
		return false, err
	}
	if err := batch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
