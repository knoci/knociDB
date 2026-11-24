package knocidb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/google/uuid"
	"github.com/knoci/knocidb/wal"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

const (
	defaultFileMode        os.FileMode = 0600
	defaultInitialMmapSize int         = 1024
)

// bolt db 存储索引数据的 bucket 名称
var indexBucketName = []byte("knocidb-index")

// BPTree 是 BoltDB 索引的实现
// BPTree 结构体用于管理 BoltDB 索引，每个分区对应一个 BoltDB 实例
// options：索引选项，trees：分区对应的 BoltDB 实例数组
type BPTree struct {
	options indexOptions
	trees   []*bbolt.DB
}

// openBTreeIndex 打开一个 BoltDB(磁盘 B 树)索引。
// 实际上，它为每个分区打开一个 BoltDB。
// 分区数量由索引选项指定。
func openBTreeIndex(options indexOptions) (*BPTree, error) {
	trees := make([]*bbolt.DB, options.partitionNum)
	closeOpened := func(n int) {
		for j := 0; j < n; j++ {
			if trees[j] != nil {
				_ = trees[j].Close()
			}
		}
	}

	for i := 0; i < options.partitionNum; i++ {
		// 打开 bolt db
		tree, err := bbolt.Open(
			filepath.Join(options.dirPath, fmt.Sprintf(indexFileExt, i)),
			defaultFileMode,
			&bbolt.Options{
				NoSync:          true,
				InitialMmapSize: defaultInitialMmapSize,
				FreelistType:    bbolt.FreelistMapType,
			},
		)
		if err != nil {
			closeOpened(i)
			return nil, err
		}

		// 开始一个可写事务来创建 bucket(如果不存在)
		tx, err := tree.Begin(true)
		if err != nil {
			_ = tree.Close()
			closeOpened(i)
			return nil, err
		}
		if _, err = tx.CreateBucketIfNotExists(indexBucketName); err != nil {
			_ = tx.Rollback()
			_ = tree.Close()
			closeOpened(i)
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			_ = tree.Close()
			closeOpened(i)
			return nil, err
		}
		trees[i] = tree
	}
	return &BPTree{trees: trees, options: options}, nil
}

func encodeIndexValue(pos *KeyPosition) []byte {
	uidBytes, _ := pos.uid.MarshalBinary()
	encPos := pos.position.Encode()
	if pos.expireAt <= 0 {
		out := make([]byte, len(uidBytes)+len(encPos))
		copy(out, uidBytes)
		copy(out[len(uidBytes):], encPos)
		return out
	}
	var expBuf [binary.MaxVarintLen64]byte
	expLen := binary.PutUvarint(expBuf[:], uint64(pos.expireAt))
	out := make([]byte, len(uidBytes)+len(encPos)+expLen)
	copy(out, uidBytes)
	copy(out[len(uidBytes):], encPos)
	copy(out[len(uidBytes)+len(encPos):], expBuf[:expLen])
	return out
}

func decodeIndexValue(key []byte, partition uint32, raw []byte) (*KeyPosition, error) {
	const uidLen = 16
	if len(raw) < uidLen {
		return nil, fmt.Errorf("invalid index value length %d", len(raw))
	}
	keyPos := &KeyPosition{
		key:       key,
		partition: partition,
	}
	if err := keyPos.uid.UnmarshalBinary(raw[:uidLen]); err != nil {
		return nil, err
	}
	pos, consumed := wal.DecodeChunkPositionWithSize(raw[uidLen:])
	keyPos.position = pos
	if consumed > 0 && uidLen+consumed < len(raw) {
		if exp, n := binary.Uvarint(raw[uidLen+consumed:]); n > 0 {
			keyPos.expireAt = int64(exp)
		}
	}
	return keyPos, nil
}

// Get 获取指定键的位置
func (bt *BPTree) Get(key []byte) (*KeyPosition, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	p := bt.options.getKeyPartition(key)
	tree := bt.trees[p]
	var keyPos *KeyPosition

	if err := tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			var err error
			keyPos, err = decodeIndexValue(key, uint32(p), value)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return keyPos, nil
}

// ApplyBatch 在每个分区内按升序 Key 在单个 bbolt 事务中同时应用 Put 和 Delete，
// 显著减少事务开销与 B+Tree COW 页分裂。
func (bt *BPTree) ApplyBatch(puts []*KeyPosition, deletes [][]byte) ([]*KeyPosition, error) {
	if len(puts) == 0 && len(deletes) == 0 {
		return nil, nil
	}
	partPuts := make([][]*KeyPosition, bt.options.partitionNum)
	for _, pos := range puts {
		partPuts[pos.partition] = append(partPuts[pos.partition], pos)
	}
	partDels := make([][][]byte, bt.options.partitionNum)
	for _, k := range deletes {
		if len(k) == 0 {
			return nil, ErrKeyIsEmpty
		}
		p := bt.options.getKeyPartition(k)
		partDels[p] = append(partDels[p], k)
	}

	deprecatedChan := make(chan []*KeyPosition, bt.options.partitionNum)
	g, ctx := errgroup.WithContext(context.Background())

	for i := 0; i < bt.options.partitionNum; i++ {
		partition := i
		if len(partPuts[partition]) == 0 && len(partDels[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			pList := partPuts[partition]
			dList := partDels[partition]
			sort.Slice(pList, func(a, b int) bool { return bytes.Compare(pList[a].key, pList[b].key) < 0 })
			sort.Slice(dList, func(a, b int) bool { return bytes.Compare(dList[a], dList[b]) < 0 })

			tree := bt.trees[partition]
			deprecated := make([]*KeyPosition, 0)
			err := tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(indexBucketName)
				for _, record := range pList {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						if len(record.key) == 0 {
							return ErrKeyIsEmpty
						}
						oldValue := bucket.Get(record.key)
						if oldValue != nil {
							oldPos, err := decodeIndexValue(record.key, record.partition, oldValue)
							if err != nil {
								return err
							}
							deprecated = append(deprecated, oldPos)
						}
						if err := bucket.Put(record.key, encodeIndexValue(record)); err != nil {
							if errors.Is(err, bbolt.ErrKeyRequired) {
								return ErrKeyIsEmpty
							}
							return err
						}
					}
				}
				for _, key := range dList {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						oldValue := bucket.Get(key)
						if err := bucket.Delete(key); err != nil {
							return err
						}
						if oldValue != nil {
							oldPos, err := decodeIndexValue(key, uint32(partition), oldValue)
							if err != nil {
								return err
							}
							deprecated = append(deprecated, oldPos)
						}
					}
				}
				return nil
			})
			deprecatedChan <- deprecated
			return err
		})
	}
	go func() {
		_ = g.Wait()
		close(deprecatedChan)
	}()

	var allDeprecated []*KeyPosition
	for dep := range deprecatedChan {
		allDeprecated = append(allDeprecated, dep...)
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return allDeprecated, nil
}

type indexCASUpdate struct {
	oldPos *wal.ChunkPosition
	newPos *KeyPosition
}

// CompareAndSwapBatch 仅当索引当前仍指向 oldPos 时将其原子更新为 newPos（保留现有 ExpireAt），
// 允许 VLog 在线压缩与并发 Memtable Flush 无全局停写和平共处。
func (bt *BPTree) CompareAndSwapBatch(partition int, updates []indexCASUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	sort.Slice(updates, func(i, j int) bool {
		return bytes.Compare(updates[i].newPos.key, updates[j].newPos.key) < 0
	})
	tree := bt.trees[partition]
	return tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		for _, u := range updates {
			curRaw := bucket.Get(u.newPos.key)
			if len(curRaw) == 0 {
				continue // 已被并发删除
			}
			curPos, err := decodeIndexValue(u.newPos.key, uint32(partition), curRaw)
			if err != nil {
				return err
			}
			if !sameChunkPosition(curPos.position, u.oldPos) {
				continue // 已被并发新写入覆盖
			}
			if u.newPos.expireAt == 0 {
				u.newPos.expireAt = curPos.expireAt
			}
			if err := bucket.Put(u.newPos.key, encodeIndexValue(u.newPos)); err != nil {
				return err
			}
		}
		return nil
	})
}

// PutBatch 将指定的键位置批量写入索引
func (bt *BPTree) PutBatch(positions []*KeyPosition) ([]*KeyPosition, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	for _, pos := range positions {
		if len(pos.key) == 0 {
			return nil, ErrKeyIsEmpty
		}
	}
	return bt.ApplyBatch(positions, nil)
}

// DeleteBatch 批量删除索引中的指定键。
func (bt *BPTree) DeleteBatch(keys [][]byte) ([]*KeyPosition, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	return bt.ApplyBatch(nil, keys)
}

// Close 释放所有 boltdb 数据库资源。
// 在关闭数据库并返回前，会阻塞等待所有未完成的事务结束。
func (bt *BPTree) Close() error {
	for _, tree := range bt.trees {
		err := tree.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// forEachKey 遍历所有分区的索引 key，用于启动时重建布隆过滤器。
func (bt *BPTree) forEachKey(fn func(key []byte) error) error {
	for _, tree := range bt.trees {
		if err := tree.View(func(tx *bbolt.Tx) error {
			cursor := tx.Bucket(indexBucketName).Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				if err := fn(k); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// forEachTTL 遍历所有分区带 TTL 的索引键，用于启动时重建内存过期跟踪器。
func (bt *BPTree) forEachTTL(fn func(key []byte, expireAt int64)) error {
	for p, tree := range bt.trees {
		if err := tree.View(func(tx *bbolt.Tx) error {
			cursor := tx.Bucket(indexBucketName).Cursor()
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				kp, err := decodeIndexValue(k, uint32(p), v)
				if err == nil && kp.expireAt > 0 {
					fn(bytes.Clone(k), kp.expireAt)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Sync 对数据库文件sync()。
func (bt *BPTree) Sync() error {
	for _, tree := range bt.trees {
		err := tree.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// bptreeIterator 实现 baseIterator 接口。
type bptreeIterator struct {
	key     []byte
	value   []byte
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	options IteratorOptions
}

// 创建一个基于 boltdb 的 btree 迭代器。
func newBptreeIterator(tx *bbolt.Tx, options IteratorOptions) *bptreeIterator {
	return &bptreeIterator{
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		options: options,
		tx:      tx,
	}
}

func prefixUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] < 0xff {
			bound := make([]byte, i+1)
			copy(bound, prefix[:i+1])
			bound[i]++
			return bound
		}
	}
	return nil
}

// Rewind 将迭代器定位到第一个键（前缀扫描采用 O(log N) Seek 定位，告别全库扫描）。
func (bi *bptreeIterator) Rewind() {
	if bi.options.Reverse {
		if len(bi.options.Prefix) == 0 {
			bi.key, bi.value = bi.cursor.Last()
			return
		}
		upper := prefixUpperBound(bi.options.Prefix)
		if upper == nil {
			bi.key, bi.value = bi.cursor.Last()
		} else {
			bi.key, _ = bi.cursor.Seek(upper)
			if bi.key == nil {
				bi.key, bi.value = bi.cursor.Last()
			} else {
				bi.key, bi.value = bi.cursor.Prev()
			}
		}
		if bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = nil, nil
		}
		return
	}

	if len(bi.options.Prefix) == 0 {
		bi.key, bi.value = bi.cursor.First()
		return
	}
	bi.key, bi.value = bi.cursor.Seek(bi.options.Prefix)
	if bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
		bi.key, bi.value = nil, nil
	}
}

// Seek 将迭代器移动到大于等于（或小于等于，若 reverse 为 true）指定 key 的位置。
func (bi *bptreeIterator) Seek(key []byte) {
	if bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Seek(key)
		if bi.key == nil {
			// key 大于所有索引键时回退到最大键。
			bi.key, bi.value = bi.cursor.Last()
		} else if !bytes.Equal(bi.key, key) {
			bi.key, bi.value = bi.cursor.Prev()
		}
		if len(bi.options.Prefix) > 0 && bi.key != nil {
			if bytes.Compare(bi.key, bi.options.Prefix) < 0 {
				bi.key, bi.value = nil, nil
				return
			}
			if !bytes.HasPrefix(bi.key, bi.options.Prefix) {
				bi.Rewind()
				if bi.key != nil && bytes.Compare(bi.key, key) > 0 {
					bi.key, bi.value = nil, nil
				}
			}
		}
		return
	}

	target := key
	if len(bi.options.Prefix) > 0 && bytes.Compare(target, bi.options.Prefix) < 0 {
		target = bi.options.Prefix
	}
	bi.key, bi.value = bi.cursor.Seek(target)
	if len(bi.options.Prefix) > 0 && bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
		bi.key, bi.value = nil, nil
	}
}

// Next 将迭代器移动到下一个键（离开前缀范围立即终止，O(1) 停止）。
func (bi *bptreeIterator) Next() {
	if bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Prev()
	} else {
		bi.key, bi.value = bi.cursor.Next()
	}
	if len(bi.options.Prefix) > 0 && bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
		bi.key, bi.value = nil, nil
	}
}

// Key 获取当前键。
func (bi *bptreeIterator) Key() []byte {
	return bi.key
}

// Value 获取当前值（ChunkPosition 字节切片 + 可选 ExpireAt 尾随字节）。
func (bi *bptreeIterator) Value() any {
	var uid uuid.UUID
	return bi.value[len(uid):]
}

// ExpireAt 返回当前索引记录的过期时间（UnixMilli，0 表示永不过期）。
func (bi *bptreeIterator) ExpireAt() int64 {
	const uidLen = 16
	if len(bi.value) <= uidLen {
		return 0
	}
	_, consumed := wal.DecodeChunkPositionWithSize(bi.value[uidLen:])
	if consumed > 0 && uidLen+consumed < len(bi.value) {
		if exp, n := binary.Uvarint(bi.value[uidLen+consumed:]); n > 0 {
			return int64(exp)
		}
	}
	return 0
}

// Valid 判断迭代器是否已耗尽。
func (bi *bptreeIterator) Valid() bool {
	return bi.key != nil
}

// Close 关闭迭代器。
func (bi *bptreeIterator) Close() error {
	return bi.tx.Rollback()
}
