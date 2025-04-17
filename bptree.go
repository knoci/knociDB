package knocidb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
	"knocidb/diskhash"
	"knocidb/wal"
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
func openBTreeIndex(options indexOptions, _ ...diskhash.MatchKeyFunc) (*BPTree, error) {
	trees := make([]*bbolt.DB, options.partitionNum)

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
			return nil, err
		}

		// 开始一个可写事务来创建 bucket(如果不存在)
		tx, err := tree.Begin(true)
		if err != nil {
			return nil, err
		}
		if _, err = tx.CreateBucketIfNotExists(indexBucketName); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		trees[i] = tree
	}

	return &BPTree{trees: trees, options: options}, nil
}

// Get 获取指定键的位置
func (bt *BPTree) Get(key []byte, _ ...diskhash.MatchKeyFunc) (*KeyPosition, error) {
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
			keyPos = new(KeyPosition)
			keyPos.key, keyPos.partition = key, uint32(p)
			err := keyPos.uid.UnmarshalBinary(value[:len(keyPos.uid)])
			if err != nil {
				return err
			}
			keyPos.position = wal.DecodeChunkPosition(value[len(keyPos.uid):])
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return keyPos, nil
}

// PutBatch 将指定的键位置批量写入索引
func (bt *BPTree) PutBatch(positions []*KeyPosition, _ ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(positions) == 0 {
		return nil, nil
	}

	// 按分区分组 positions
	partitionRecords := make([][]*KeyPosition, bt.options.partitionNum)
	for _, pos := range positions {
		p := pos.partition
		partitionRecords[p] = append(partitionRecords[p], pos)
	}

	// 创建通道用于收集被覆盖的旧条目
	deprecatedChan := make(chan []*KeyPosition, len(partitionRecords))
	g, ctx := errgroup.WithContext(context.Background())

	for i := range partitionRecords {
		partition := i
		if len(partitionRecords[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			// 获取该分区的 bolt db 实例
			tree := bt.trees[partition]
			partitionDeprecatedKeyPosition := make([]*KeyPosition, 0)
			err := tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(indexBucketName)
				// 将每条记录写入 bucket
				for _, record := range partitionRecords[partition] {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						uidBytes, _ := record.uid.MarshalBinary()
						encPos := record.position.Encode()
						//nolint:gocritic // 需要将 uidbytes 与 encPos 合并后存入 bptree
						valueBytes := append(uidBytes, encPos...)
						oldValue := bucket.Get(record.key)
						if err := bucket.Put(record.key, valueBytes); err != nil {
							if errors.Is(err, bbolt.ErrKeyRequired) {
								return ErrKeyIsEmpty
							}
							return err
						} else if oldValue != nil {
							keyPos := new(KeyPosition)
							keyPos.key, keyPos.partition = record.key, record.partition
							err := keyPos.uid.UnmarshalBinary(oldValue[:len(keyPos.uid)])
							if err != nil {
								return err
							}
							keyPos.position = wal.DecodeChunkPosition(oldValue[len(keyPos.uid):])
							partitionDeprecatedKeyPosition = append(partitionDeprecatedKeyPosition, keyPos)
						}
					}
				}
				return nil
			})
			// 将被覆盖的 uuid 切片发送到通道
			deprecatedChan <- partitionDeprecatedKeyPosition
			return err
		})
	}
	// 所有 goroutine 完成后关闭通道
	go func() {
		_ = g.Wait()
		close(deprecatedChan)
	}()

	var deprecatedKeyPosition []*KeyPosition
	for partitionDeprecatedKeyPosition := range deprecatedChan {
		deprecatedKeyPosition = append(deprecatedKeyPosition, partitionDeprecatedKeyPosition...)
	}

	// 等待所有 goroutine 完成
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return deprecatedKeyPosition, nil
}

// DeleteBatch 批量删除索引中的指定键。
func (bt *BPTree) DeleteBatch(keys [][]byte, _ ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// 按分区分组 keys
	partitionKeys := make([][][]byte, bt.options.partitionNum)
	for _, key := range keys {
		p := bt.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
	}

	// 创建通道用于收集被删除的旧条目
	deprecatedChan := make(chan []*KeyPosition, len(partitionKeys))

	// 从每个分区删除 keys
	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionKeys {
		partition := i
		if len(partitionKeys[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			tree := bt.trees[partition]
			partitionDeprecatedKeyPosition := make([]*KeyPosition, 0)
			err := tree.Update(func(tx *bbolt.Tx) error {
				// 获取该分区的 bolt db 实例
				bucket := tx.Bucket(indexBucketName)
				// 从 bucket 删除每个 key
				for _, key := range partitionKeys[partition] {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						if len(key) == 0 {
							return ErrKeyIsEmpty
						}
						oldValue := bucket.Get(key)
						if err := bucket.Delete(key); err != nil {
							return err
						} else if oldValue != nil {
							keyPos := new(KeyPosition)
							keyPos.key, keyPos.partition = key, uint32(partition)
							err := keyPos.uid.UnmarshalBinary(oldValue[:len(keyPos.uid)])
							if err != nil {
								return err
							}
							keyPos.position = wal.DecodeChunkPosition(oldValue[len(keyPos.uid):])
							partitionDeprecatedKeyPosition = append(partitionDeprecatedKeyPosition, keyPos)
						}
					}
				}
				return nil
			})
			// 将被删除的 uuid 切片发送到通道
			deprecatedChan <- partitionDeprecatedKeyPosition
			return err
		})
	}
	// 所有 goroutine 完成后关闭通道
	go func() {
		_ = g.Wait()
		close(deprecatedChan)
	}()

	var deprecatedKeyPosition []*KeyPosition
	for partitionDeprecatedKeyPosition := range deprecatedChan {
		deprecatedKeyPosition = append(deprecatedKeyPosition, partitionDeprecatedKeyPosition...)
	}

	// 等待所有 goroutine 完成
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return deprecatedKeyPosition, nil
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

// Sync 对数据库文件句柄执行 fdatasync()。
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

// Rewind 将迭代器定位到第一个键。
func (bi *bptreeIterator) Rewind() {
	if bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Last()
		if len(bi.options.Prefix) == 0 {
			return
		}
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Prev()
		}
	} else {
		bi.key, bi.value = bi.cursor.First()
		if len(bi.options.Prefix) == 0 {
			return
		}
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Next()
		}
	}
}

// Seek 将迭代器移动到大于等于（或小于等于，若 reverse 为 true）指定 key 的位置。
func (bi *bptreeIterator) Seek(key []byte) {
	bi.key, bi.value = bi.cursor.Seek(key)
	if !bytes.Equal(bi.key, key) && bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Prev()
	}
	if len(bi.options.Prefix) == 0 {
		return
	}
	if !bytes.HasPrefix(bi.Key(), bi.options.Prefix) {
		bi.Next()
	}
}

// Next 将迭代器移动到下一个键。
func (bi *bptreeIterator) Next() {
	if bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Prev()
		if len(bi.options.Prefix) == 0 {
			return
		}
		// 前缀扫描
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Prev()
		}
	} else {
		bi.key, bi.value = bi.cursor.Next()
		if len(bi.options.Prefix) == 0 {
			return
		}
		// 前缀扫描
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Next()
		}
	}
}

// Key 获取当前键。
func (bi *bptreeIterator) Key() []byte {
	return bi.key
}

// Value 获取当前值。
func (bi *bptreeIterator) Value() any {
	var uid uuid.UUID
	return bi.value[len(uid):]
}

// Valid 判断迭代器是否已耗尽。
func (bi *bptreeIterator) Valid() bool {
	return bi.key != nil
}

// Close 关闭迭代器。
func (bi *bptreeIterator) Close() error {
	return bi.tx.Rollback()
}
