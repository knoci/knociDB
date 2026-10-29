package knocidb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	smachine "github.com/lni/dragonboat/v4/statemachine"
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
	// 补写后的 commit log 必须保证迁移后的 WAL 批次仍可恢复。
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

func TestIteratorSnapshotDoesNotBlockWrites(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("snapshot-key"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	it, err := db.NewIterator(IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	done := make(chan error, 1)
	go func() { done <- db.Put([]byte("snapshot-key"), []byte("v2")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("an open iterator must not block writes")
	}

	var got string
	for it.Rewind(); it.Valid(); it.Next() {
		if string(it.Key()) == "snapshot-key" {
			got = string(it.Value())
		}
	}
	if got != "v1" {
		t.Fatalf("iterator saw %q, want snapshot value v1", got)
	}
	value, err := db.Get([]byte("snapshot-key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "v2" {
		t.Fatalf("committed write not visible: %q", value)
	}
}

func TestCommitLogRewrite(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("rewrite-%03d", i))
		if err := db.Put(key, key); err != nil {
			t.Fatal(err)
		}
	}
	ids := db.batchRefSnapshot()
	if err := db.commitLog.rewrite(ids); err != nil {
		t.Fatal(err)
	}
	if got := db.commitLog.recordCount(); got != len(ids) {
		t.Fatalf("commit records = %d, want %d", got, len(ids))
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
		key := []byte(fmt.Sprintf("rewrite-%03d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
	}
}

func TestCommitLogPruning(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	const count = commitLogPruneThreshold + 100
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("prune-%05d", i))
		if err := db.Put(key, key); err != nil {
			t.Fatal(err)
		}
	}
	for _, sh := range db.shards {
		db.flushMemtable(sh.active)
	}
	db.maybePruneCommitLog()
	if got := db.commitLog.recordCount(); got != 0 {
		t.Fatalf("commit log still has %d records after all WALs flushed", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("prune-%05d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
	}
}

func TestClusterBatchAcrossShards(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sm := NewKVStateMachine(1, 1, db)
	cmds := make([]Command, 0, 100)
	for i := 0; i < 100; i++ {
		cmds = append(cmds, Command{Op: OpPut, Key: []byte(fmt.Sprintf("raft-%03d", i)), Value: []byte("v")})
	}
	payload, err := json.Marshal(BatchCommand{Op: OpBatch, Commands: cmds})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Update(smachine.Entry{Index: 1, Cmd: payload}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("raft-%03d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
	}
}

func TestIteratorBlocksCompactionNotWrites(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("compact-iterator-%03d", i))
		if err := db.Put(key, key); err != nil {
			t.Fatal(err)
		}
	}
	// 压缩前确保 value 已进入 value log。
	for _, sh := range db.shards {
		db.flushMemtable(sh.active)
	}
	it, err := db.NewIterator(IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- db.Compact() }()
	select {
	case err := <-done:
		t.Fatalf("compaction must wait for the iterator, got %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	// 写入不应被迭代器或待执行的压缩阻塞。
	if err := db.Put([]byte("after-iterator"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish after iterator close")
	}
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("compact-iterator-%03d", i))
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if string(value) != string(key) {
			t.Fatalf("value mismatch for %s", key)
		}
	}
}

func TestBatchIDsAreUnique(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const count = 1000
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("unique-batch-%04d", i))
		if err := db.Put(key, key); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[uint64]struct{})
	for _, sh := range db.shards {
		for id := range sh.active.batchIDs {
			seen[id] = struct{}{}
		}
	}
	if len(seen) != count {
		t.Fatalf("got %d unique batch IDs, want %d", len(seen), count)
	}
}

func TestSingleShardBatchRecoveryWithoutCommit(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = t.TempDir()
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("single-shard-no-commit")
	if err := db.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "COMMIT"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("single-shard batch wrote %d bytes to COMMIT", info.Size())
	}
	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "value" {
		t.Fatalf("got %q", got)
	}
}
