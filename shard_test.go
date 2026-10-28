package knocidb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDefaultPartitionCount(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if len(db.shards) != 5 {
		t.Fatalf("got %d shards, want 5", len(db.shards))
	}
}

func TestBatchDoesNotLockBeforeCommit(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	batch := db.NewBatch(DefaultBatchOptions)
	defer batch.Close()
	if err := batch.Put([]byte("pending"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- db.Put([]byte("other"), []byte("v")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("a batch that has not committed must not block other writes")
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentShardWrites(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const writers = 32
	const perWriter = 40
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("w-%02d-k-%03d", w, i))
				if err := db.Put(key, key); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			key := []byte(fmt.Sprintf("w-%02d-k-%03d", w, i))
			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			if string(got) != string(key) {
				t.Fatalf("value mismatch for %s", key)
			}
		}
	}
}

func TestConcurrentDeleteAndRead(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("race-key"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := db.Get([]byte("race-key")); err != nil && err != ErrKeyNotFound {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := db.Delete([]byte("race-key")); err != nil {
			errs <- err
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("race-key")); err != ErrKeyNotFound {
		t.Fatalf("deleted key: %v", err)
	}
}

func TestShardedCommitLogReopen(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("reopen-%03d", i))
		if err := db.Put(key, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("reopen-%03d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if string(got) != string(key) {
			t.Fatalf("value mismatch for %s", key)
		}
	}
}

func TestMultiShardBatchReopen(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch(DefaultBatchOptions)
	shards := make(map[uint32]struct{})
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("multi-shard-%02d", i))
		shards[db.shardFor(key).id] = struct{}{}
		if err := batch.Put(key, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if len(shards) < 2 {
		t.Fatalf("test keys hit only %d shard(s)", len(shards))
	}
	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("multi-shard-%02d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
	}
}

func TestLegacyWALCommitLogMigration(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("legacy-%02d", i))
		if err := db.Put(key, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, commitLogName)); err != nil {
		t.Fatal(err)
	}
	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("legacy-%02d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("legacy get %s: %v", key, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// The seeded commit log must keep the migrated WAL batches readable.
	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("legacy-%02d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("seeded get %s: %v", key, err)
		}
	}
}
