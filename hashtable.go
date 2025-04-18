package knocidb

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"knocidb/diskhash"
)

// diskhash需要固定大小的值
// 所以我们将slotValueLength设置为`binary.MaxVarintLen32*3 + binary.MaxVarintLen64`。
// 这是wal.chunkPosition编码后的最大长度。
const slotValueLength = binary.MaxVarintLen32*3 + binary.MaxVarintLen64

// HashTable是diskhash索引的实现。
type HashTable struct {
	options indexOptions
	tables  []*diskhash.Table
}

// openHashIndex为每个分区打开一个diskhash。
// 分区数量由索引选项指定。
func openHashIndex(options indexOptions) (*HashTable, error) {
	tables := make([]*diskhash.Table, options.partitionNum)

	for i := 0; i < options.partitionNum; i++ {
		dishHashOptions := diskhash.DefaultOptions
		dishHashOptions.DirPath = filepath.Join(options.dirPath, fmt.Sprintf(indexFileExt, i))
		dishHashOptions.SlotValueLength = slotValueLength
		table, err := diskhash.Open(dishHashOptions)
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}

	return &HashTable{
		options: options,
		tables:  tables,
	}, nil
}

// PutBatch将批量记录写入索引。
func (ht *HashTable) PutBatch(positions []*KeyPosition, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	partitionRecords := make([][]*KeyPosition, ht.options.partitionNum)
	matchKeys := make([][]diskhash.MatchKeyFunc, ht.options.partitionNum)
	for i, pos := range positions {
		p := pos.partition
		partitionRecords[p] = append(partitionRecords[p], pos)
		matchKeys[p] = append(matchKeys[p], matchKeyFunc[i])
	}

	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionRecords {
		partition := i
		if len(partitionRecords[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			// 获取此分区的hashtable实例
			table := ht.tables[partition]
			matchKey := matchKeys[partition]
			for i, record := range partitionRecords[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if len(record.key) == 0 {
						return ErrKeyIsEmpty
					}
					encPos := record.position.EncodeFixedSize()
					if err := table.Put(record.key, encPos, matchKey[i]); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return nil, g.Wait()
}

// Get通过键获取块位置。
func (ht *HashTable) Get(key []byte, matchKeyFunc ...diskhash.MatchKeyFunc) (*KeyPosition, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	p := ht.options.getKeyPartition(key)
	table := ht.tables[p]
	err := table.Get(key, matchKeyFunc[0])
	if err != nil {
		return nil, err
	}
	// hashTable不会使用keyPosition，所以返回nil, nil
	return nil, nil
}

// DeleteBatch从索引中删除批量记录。
func (ht *HashTable) DeleteBatch(keys [][]byte, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	partitionKeys := make([][][]byte, ht.options.partitionNum)
	matchKeys := make([][]*diskhash.MatchKeyFunc, ht.options.partitionNum)
	for i, key := range keys {
		p := ht.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
		matchKeys[p] = append(matchKeys[p], &matchKeyFunc[i])
	}
	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionKeys {
		partition := i
		if len(partitionKeys[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			table := ht.tables[partition]
			matchKey := matchKeys[partition]
			for i, key := range partitionKeys[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if len(key) == 0 {
						return ErrKeyIsEmpty
					}
					if err := table.Delete(key, *matchKey[i]); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return nil, g.Wait()
}

// Sync将索引数据同步到磁盘。
func (ht *HashTable) Sync() error {
	for _, table := range ht.tables {
		err := table.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close关闭索引。
func (ht *HashTable) Close() error {
	for _, table := range ht.tables {
		err := table.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
