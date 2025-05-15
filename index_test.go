package knocidb

import (
	"github.com/knoci/knocidb/diskhash"
	"github.com/knoci/knocidb/wal"
	"os"
	"testing"
)

func TestIndexOptionsGetKeyPartition(t *testing.T) {

	options := indexOptions{
		indexType:    BTree,
		dirPath:      os.TempDir(),
		partitionNum: 4,
		keyHashFunction: func(key []byte) uint64 {
			// 简单的哈希函数，用于测试
			if len(key) == 0 {
				return 0
			}
			return uint64(key[0]) % 4
		},
	}

	testCases := []struct {
		key      []byte
		expected int
	}{
		{[]byte{0}, 0},
		{[]byte{1}, 1},
		{[]byte{2}, 2},
		{[]byte{3}, 3},
		{[]byte{4}, 0},
		{[]byte{5}, 1},
		{[]byte{}, 0},
	}

	for _, tc := range testCases {
		result := options.getKeyPartition(tc.key)
		if result != tc.expected {
			t.Errorf("getKeyPartition(%v) = %d, expected %d", tc.key, result, tc.expected)
		}
	}
}

func TestOpenIndex(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "index_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    BTree,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			return 0
		},
	}

	index, err := openIndex(options)
	if err != nil {
		t.Fatalf("Failed to open BTree index: %v", err)
	}

	_, ok := index.(*BPTree)
	if !ok {
		t.Error("Expected BPTree index type")
	}

	err = index.Close()
	if err != nil {
		t.Errorf("Failed to close BTree index: %v", err)
	}

	err = index.Close()
	if err != nil {
		t.Errorf("Failed to close Hash index: %v", err)
	}
}

func createTestKeyPosition(key []byte, partition uint32) *KeyPosition {
	return &KeyPosition{
		key:       key,
		partition: partition,
		position: &wal.ChunkPosition{
			SegmentId:   1,
			ChunkOffset: 100,
			ChunkSize:   200,
		},
	}
}

func TestBPTreeBasicOperations(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "bptree_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := indexOptions{
		indexType:    BTree,
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			if len(key) == 0 {
				return 0
			}
			return uint64(key[0]) % 2
		},
	}

	index, err := openIndex(options)
	if err != nil {
		t.Fatalf("Failed to open BPTree index: %v", err)
	}
	defer index.Close()

	key1 := []byte("key1")
	key2 := []byte("key2")

	keyPos1 := createTestKeyPosition(key1, 0)
	keyPos2 := createTestKeyPosition(key2, 1)

	_, err = index.PutBatch([]*KeyPosition{keyPos1, keyPos2})

	_, err = index.Get(key1)

	_, err = index.DeleteBatch([][]byte{key1})

	_, err = index.Get(key1)

	err = index.Sync()
	if err != nil {
		t.Errorf("Failed to sync index: %v", err)
	}
}

func TestHashTableBasicOperations2(t *testing.T) {

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

	index, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable index: %v", err)
	}
	defer index.Close()

	key1 := []byte("key1")
	key2 := []byte("key2")

	keyPos1 := createTestKeyPosition(key1, 0)
	keyPos2 := createTestKeyPosition(key2, 1)

	matchFunc := func(slot diskhash.Slot) (bool, error) {
		return true, nil
	}

	_, err = index.PutBatch([]*KeyPosition{keyPos1, keyPos2}, matchFunc, matchFunc)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}

	_, err = index.Get(key1, matchFunc)
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}

	_, err = index.DeleteBatch([][]byte{key1}, matchFunc)
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}

	err = index.Sync()
	if err != nil {
		t.Errorf("Failed to sync index: %v", err)
	}
}

func TestEmptyKeyHandling(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "empty_key_test")
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

	index, err := openHashIndex(options)
	if err != nil {
		t.Fatalf("Failed to open HashTable index: %v", err)
	}
	defer index.Close()

	emptyKey := []byte{}
	keyPos := createTestKeyPosition(emptyKey, 0)

	matchFunc := func(slot diskhash.Slot) (bool, error) {
		return true, nil
	}

	_, err = index.PutBatch([]*KeyPosition{keyPos}, matchFunc)
	if err == nil {
		t.Error("Expected error for empty key in PutBatch")
	}

	_, err = index.Get(emptyKey, matchFunc)
	if err == nil {
		t.Error("Expected error for empty key in Get")
	}

	_, err = index.DeleteBatch([][]byte{emptyKey}, matchFunc)
	if err == nil {
		t.Error("Expected error for empty key in DeleteBatch")
	}
}
