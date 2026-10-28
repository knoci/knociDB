package knocidb

import (
	"os"
	"testing"

	"github.com/knoci/knocidb/wal"
)

func TestIndexOptionsGetKeyPartition(t *testing.T) {
	options := indexOptions{
		dirPath:      os.TempDir(),
		partitionNum: 4,
		keyHashFunction: func(key []byte) uint64 {
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
		if got := options.getKeyPartition(tc.key); got != tc.expected {
			t.Errorf("getKeyPartition(%v) = %d, expected %d", tc.key, got, tc.expected)
		}
	}
}

func TestOpenIndex(t *testing.T) {
	tempDir := t.TempDir()
	options := indexOptions{
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
	if _, ok := index.(*BPTree); !ok {
		t.Error("Expected BPTree index type")
	}
	if err := index.Close(); err != nil {
		t.Errorf("Failed to close BTree index: %v", err)
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
	tempDir := t.TempDir()
	options := indexOptions{
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

	if _, err = index.PutBatch([]*KeyPosition{keyPos1, keyPos2}); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if _, err = index.Get(key1); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err = index.DeleteBatch([][]byte{key1}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if _, err = index.Get(key1); err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if err = index.Sync(); err != nil {
		t.Errorf("Failed to sync index: %v", err)
	}
}

func TestBPTreeEmptyKeyHandling(t *testing.T) {
	tempDir := t.TempDir()
	options := indexOptions{
		dirPath:      tempDir,
		partitionNum: 2,
		keyHashFunction: func(key []byte) uint64 {
			return 0
		},
	}
	index, err := openIndex(options)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	emptyKey := []byte{}
	keyPos := createTestKeyPosition(emptyKey, 0)
	if _, err := index.PutBatch([]*KeyPosition{keyPos}); err == nil {
		t.Error("Expected error for empty key in PutBatch")
	}
	if _, err := index.Get(emptyKey); err == nil {
		t.Error("Expected error for empty key in Get")
	}
	if _, err := index.DeleteBatch([][]byte{emptyKey}); err == nil {
		t.Error("Expected error for empty key in DeleteBatch")
	}
}
