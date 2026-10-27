package knocidb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const ttlDBFileName = "TTL.DB"

var ttlBucket = []byte("ttl")

// ttlStore 是核心层的 TTL 旁路索引。
// 它用独立 bbolt 文件保存 key -> expireAt(ms)，不改变 ValueLog 记录格式。
type ttlStore struct {
	db    *bolt.DB
	mu    sync.RWMutex
	cache map[string]int64
}

func openTTLStore(dir string) (*ttlStore, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, ttlDBFileName), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open ttl store: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(ttlBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init ttl bucket: %w", err)
	}
	t := &ttlStore{db: db, cache: make(map[string]int64)}
	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ttlBucket)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			t.cache[string(k)] = decodeExpireAt(v)
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load ttl cache: %w", err)
	}
	return t, nil
}

func (t *ttlStore) close() error {
	if t == nil || t.db == nil {
		return nil
	}
	return t.db.Close()
}

func (t *ttlStore) sync() error {
	if t == nil || t.db == nil {
		return nil
	}
	return t.db.Sync()
}

func encodeExpireAt(expireAt int64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(expireAt))
	return out
}

func decodeExpireAt(raw []byte) int64 {
	if len(raw) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw))
}

func (t *ttlStore) get(key []byte) (int64, bool, error) {
	if t == nil || t.db == nil {
		return 0, false, nil
	}
	t.mu.RLock()
	expireAt, exists := t.cache[string(key)]
	t.mu.RUnlock()
	return expireAt, exists, nil
}

func (t *ttlStore) has(key []byte) bool {
	if t == nil || t.db == nil {
		return false
	}
	t.mu.RLock()
	_, ok := t.cache[string(key)]
	t.mu.RUnlock()
	return ok
}

func (t *ttlStore) isExpired(key []byte, now int64) (bool, error) {
	expireAt, exists, err := t.get(key)
	if err != nil || !exists {
		return false, err
	}
	return expireAt > 0 && expireAt <= now, nil
}

// apply 批量写入 TTL；expireAt == 0 表示删除 TTL。
func (t *ttlStore) apply(changes map[string]int64) error {
	if t == nil || t.db == nil || len(changes) == 0 {
		return nil
	}
	if err := t.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ttlBucket)
		if bucket == nil {
			var err error
			bucket, err = tx.CreateBucketIfNotExists(ttlBucket)
			if err != nil {
				return err
			}
		}
		for key, expireAt := range changes {
			if expireAt == 0 {
				if err := bucket.Delete([]byte(key)); err != nil {
					return err
				}
				continue
			}
			if err := bucket.Put([]byte(key), encodeExpireAt(expireAt)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	t.mu.Lock()
	for key, expireAt := range changes {
		if expireAt == 0 {
			delete(t.cache, key)
		} else {
			t.cache[key] = expireAt
		}
	}
	t.mu.Unlock()
	return nil
}

func (t *ttlStore) expired(now int64) ([][]byte, error) {
	if t == nil || t.db == nil {
		return nil, nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	var keys [][]byte
	for key, expireAt := range t.cache {
		if expireAt > 0 && expireAt <= now {
			keys = append(keys, []byte(key))
		}
	}
	return keys, nil
}
