package knocidb

import (
	"github.com/knoci/knocidb/diskhash"
	"github.com/knoci/knocidb/wal"
	"os"
	"testing"
)

func TestHashTableBasicOperations1(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hashtable_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    Hash,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			if len(key) == 0 {
				return 0
			}
			return uint64(key[0]) % 2
		},
	}

	hashTable, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable: %v", err)
	}
	defer hashTable.Close()

	if len(hashTable.tables) != options.partitionNum {
		t.Errorf("Expected %d partitions, got %d", options.partitionNum, len(hashTable.tables))
	}
}

func TestHashTablePutBatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hashtable_put_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    Hash,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			if len(key) == 0 {
				return 0
			}
			return uint64(key[0]) % 2
		},
	}

	hashTable, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable: %v", err)
	}
	defer hashTable.Close()

	key1 := []byte("key1")
	key2 := []byte("key2")

	pos1 := &KeyPosition{
		key:       key1,
		partition: 0,
		position: &wal.ChunkPosition{
			SegmentId:   1,
			ChunkOffset: 100,
			ChunkSize:   200,
		},
	}

	pos2 := &KeyPosition{
		key:       key2,
		partition: 1,
		position: &wal.ChunkPosition{
			SegmentId:   2,
			ChunkOffset: 300,
			ChunkSize:   400,
		},
	}

	matchFunc1 := func(slot diskhash.Slot) (bool, error) {
		return true, nil
	}

	matchFunc2 := func(slot diskhash.Slot) (bool, error) {
		return true, nil
	}

	_, err = hashTable.PutBatch([]*KeyPosition{pos1, pos2}, matchFunc1, matchFunc2)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}

	_, err = hashTable.PutBatch([]*KeyPosition{})
	if err != nil {
		t.Fatalf("Failed to put empty batch: %v", err)
	}

	emptyKeyPos := &KeyPosition{
		key:       []byte{},
		partition: 0,
		position: &wal.ChunkPosition{
			SegmentId:   3,
			ChunkOffset: 500,
			ChunkSize:   600,
		},
	}

	_, err = hashTable.PutBatch([]*KeyPosition{emptyKeyPos}, matchFunc1)
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

func TestHashTableGet(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hashtable_get_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    Hash,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			if len(key) == 0 {
				return 0
			}
			return uint64(key[0]) % 2
		},
	}

	hashTable, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable: %v", err)
	}
	defer hashTable.Close()

	key := []byte("testkey")

	matchFunc := func(slot diskhash.Slot) (bool, error) {
		return true, nil
	}

	_, err = hashTable.Get(key, matchFunc)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	_, err = hashTable.Get([]byte{}, matchFunc)
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

func TestHashTableDeleteBatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hashtable_delete_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    Hash,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			if len(key) == 0 {
				return 0
			}
			return uint64(key[0]) % 2
		},
	}

	hashTable, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable: %v", err)
	}
	defer hashTable.Close()

	key1 := []byte("key1")
	key2 := []byte("key2")

	matchFunc1 := func(slot diskhash.Slot) (bool, error) {
		return true, nil
	}

	matchFunc2 := func(slot diskhash.Slot) (bool, error) {
		return true, nil
	}

	_, err = hashTable.DeleteBatch([][]byte{key1, key2}, matchFunc1, matchFunc2)
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}

	_, err = hashTable.DeleteBatch([][]byte{})
	if err != nil {
		t.Fatalf("Failed to delete empty batch: %v", err)
	}

	_, err = hashTable.DeleteBatch([][]byte{[]byte{}}, matchFunc1)
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

func TestHashTableSync(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "hashtable_sync_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    Hash,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			return 0
		},
	}

	hashTable, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable: %v", err)
	}
	defer hashTable.Close()

	err = hashTable.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}
}

func TestHashTableClose(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "hashtable_close_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    Hash,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			return 0
		},
	}

	hashTable, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable: %v", err)
	}

	err = hashTable.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
}

func TestHashTableContextCancellation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hashtable_context_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    Hash,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			return 0
		},
	}

	hashTable, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable: %v", err)
	}
	defer hashTable.Close()

	positions := make([]*KeyPosition, 100)
	matchFuncs := make([]diskhash.MatchKeyFunc, 100)

	for i := 0; i < 100; i++ {
		positions[i] = &KeyPosition{
			key:       []byte{byte(i)},
			partition: 0,
			position: &wal.ChunkPosition{
				SegmentId:   uint32(i),
				ChunkOffset: int64(i * 100),
				ChunkSize:   uint32(i * 200),
			},
		}

		matchFuncs[i] = func(slot diskhash.Slot) (bool, error) {
			return true, nil
		}
	}
	_, _ = hashTable.PutBatch(positions, matchFuncs...)
}
