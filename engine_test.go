package knocidb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestM2_GhostTTLClearedOnOverwriteAndDelete(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.TTLScanInterval = 0
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("ghost-ttl-key")
	require.NoError(t, db.PutWithTTL(key, []byte("v1"), 120*time.Millisecond))
	// Overwrite with normal Put must clear embedded TTL (no ghost TTL resurrection).
	require.NoError(t, db.Put(key, []byte("v2")))

	_, hasTTL, err := db.GetTTL(key)
	require.NoError(t, err)
	require.False(t, hasTTL)

	time.Sleep(180 * time.Millisecond)
	val, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val)

	// Delete + Put must also have no TTL.
	key2 := []byte("ghost-ttl-key2")
	require.NoError(t, db.PutWithTTL(key2, []byte("a"), 120*time.Millisecond))
	require.NoError(t, db.Delete(key2))
	require.NoError(t, db.Put(key2, []byte("b")))
	time.Sleep(180 * time.Millisecond)
	val2, err := db.Get(key2)
	require.NoError(t, err)
	require.Equal(t, []byte("b"), val2)
}

func TestM2_TTLSweeperNoTOCTOUDeleteRefreshedKey(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.TTLScanInterval = 0
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("refresh-key")
	require.NoError(t, db.PutWithTTL(key, []byte("v1"), 50*time.Millisecond))
	time.Sleep(80 * time.Millisecond)

	// Simulate sweeper having captured an expired candidate, then a concurrent Put refreshes the key before sweepExpiredKey runs.
	nowMs := time.Now().UnixMilli()
	require.NoError(t, db.Put(key, []byte("refreshed")))
	db.sweepExpiredKey(key, nowMs)

	val, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("refreshed"), val)
}

func TestM2_MVCCZeroFreezeIteratorIsolation(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.TTLScanInterval = 0
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Put([]byte("a"), []byte("1")))
	require.NoError(t, db.Put([]byte("b"), []byte("2")))

	beforeActiveID := db.shards[0].active.options.tableID
	it, err := db.NewIterator(IteratorOptions{})
	require.NoError(t, err)
	defer it.Close()

	// Creating an iterator must NOT freeze/rotate the active memtable.
	require.Equal(t, beforeActiveID, db.shards[0].active.options.tableID)

	// Concurrent writes after iterator creation must NOT leak into the open iterator snapshot.
	require.NoError(t, db.Put([]byte("a"), []byte("1-new")))
	require.NoError(t, db.Put([]byte("c"), []byte("3-new")))
	require.NoError(t, db.Delete([]byte("b")))

	got := map[string]string{}
	for it.Rewind(); it.Valid(); it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	require.NoError(t, it.Err())
	require.Equal(t, map[string]string{"a": "1", "b": "2"}, got)
}

func TestM2_PrefixAndReverseSeekBoundary(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.TTLScanInterval = 0
	opts.MemtableSize = 64 * KB
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	for i := 0; i < 50; i++ {
		require.NoError(t, db.Put([]byte(fmt.Sprintf("alpha-%03d", i)), []byte("va")))
		require.NoError(t, db.Put([]byte(fmt.Sprintf("beta-%03d", i)), []byte("vb")))
	}
	// Flush to BPTree so both memtable and BPTree iterators are exercised.
	for _, s := range db.shards {
		db.flushMemtable(s.active)
	}

	// Prefix scan
	it, err := db.NewIterator(IteratorOptions{Prefix: []byte("beta-")})
	require.NoError(t, err)
	var count int
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Equal(t, 50, count)

	// Reverse Seek past maximum key must land on the largest key ("beta-049").
	rev, err := db.NewIterator(IteratorOptions{Reverse: true})
	require.NoError(t, err)
	rev.Seek([]byte("zzz"))
	require.True(t, rev.Valid())
	require.Equal(t, []byte("beta-049"), rev.Key())
	require.NoError(t, rev.Close())
}

func TestM2_OnlineCASCompactionWithConcurrentWrites(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.PartitionNum = 2
	opts.MemtableSize = 32 * KB
	opts.ValueLogFileSize = 64 * KB
	opts.AutoCompactSupport = false
	opts.TTLScanInterval = 0

	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = 'a'
	}
	for i := 0; i < 300; i++ {
		require.NoError(t, db.Put([]byte(fmt.Sprintf("k-%04d", i)), payload))
	}
	// Overwrite half to create garbage and force flushes.
	for i := 0; i < 150; i++ {
		require.NoError(t, db.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte("v-overwrite-1")))
	}
	for _, s := range db.shards {
		db.flushMemtable(s.active)
	}

	// Run online compaction concurrently with live updates to the same key range.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = db.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-live-%04d", i)))
		}
	}()
	require.NoError(t, db.Compact())
	wg.Wait()

	// Verify none of the concurrent live writes were clobbered by compaction!
	for i := 0; i < 100; i++ {
		val, err := db.Get([]byte(fmt.Sprintf("k-%04d", i)))
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("v-live-%04d", i)), val)
	}
	for i := 100; i < 150; i++ {
		val, err := db.Get([]byte(fmt.Sprintf("k-%04d", i)))
		require.NoError(t, err)
		require.Equal(t, []byte("v-overwrite-1"), val)
	}
	for i := 150; i < 300; i++ {
		val, err := db.Get([]byte(fmt.Sprintf("k-%04d", i)))
		require.NoError(t, err)
		require.Equal(t, payload, val)
	}
}
