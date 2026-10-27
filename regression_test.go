package knocidb

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	smachine "github.com/lni/dragonboat/v4/statemachine"
)

func openRegressionDB(t *testing.T, mutate func(*Options)) *DB {
	t.Helper()
	dir := t.TempDir()
	opt := DefaultOptions
	opt.DirPath = dir
	opt.SnapshotPath = dir + "/snapshots"
	opt.MemtableSize = 1 << 20
	opt.ValueLogFileSize = 64 << 20
	opt.AutoCompactSupport = false
	opt.EnableBloomFilter = false
	if mutate != nil {
		mutate(&opt)
	}
	db, err := Open(opt)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func TestRegressionBloomRestart(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions
	opt.DirPath = dir
	opt.SnapshotPath = dir + "/snapshots"
	opt.MemtableSize = 1 << 20
	opt.EnableBloomFilter = true

	db, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	v, err := db2.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("unexpected value: %q", v)
	}
}

func TestRegressionIncrementalCompactKeepsDelete(t *testing.T) {
	db := openRegressionDB(t, nil)
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)

	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected not found before compact, got %v", err)
	}
	if err := db.CompactWithDiscardtable(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted key resurrected, err=%v", err)
	}
	if pos, _ := db.index.Get([]byte("k")); pos != nil {
		t.Fatal("index still contains deleted key")
	}
}

func TestRegressionCompactChunkBoundary(t *testing.T) {
	db := openRegressionDB(t, nil)
	defer db.Close()

	// 16(uuid)+4(keyLen)+2(key)+32732 = 32754，+7 chunk 头 = 32761；
	// 距离 32KB block 末尾正好 7 字节，会触发 padding。
	value := bytes.Repeat([]byte("x"), 32732)
	if err := db.Put([]byte("k1"), value); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k2"), value); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("k1"), []byte("k2")} {
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %s after compact: %v", key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value mismatch for %s", key)
		}
	}
}

func TestRegressionHashIndexDiscard(t *testing.T) {
	db := openRegressionDB(t, func(opt *Options) {
		opt.IndexType = Hash
	})
	defer db.Close()

	if err := db.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if db.vlog.discardNumber == 0 {
		t.Fatal("hash index did not record old uuid as discarded")
	}
	if v, err := db.Get([]byte("k")); err != nil || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("unexpected get: %q %v", v, err)
	}

	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if err := db.CompactWithDiscardtable(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted hash key resurrected, err=%v", err)
	}
}

func TestRegressionEmptyValue(t *testing.T) {
	db := openRegressionDB(t, nil)
	defer db.Close()

	if err := db.Put([]byte("empty"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get([]byte("empty")); err != nil || len(v) != 0 {
		t.Fatalf("empty value before flush: %q %v", v, err)
	}
	if ok, err := db.Exist([]byte("empty")); err != nil || !ok {
		t.Fatalf("exist before flush: %v %v", ok, err)
	}

	db.flushMemtable(db.activeMem)
	if v, err := db.Get([]byte("empty")); err != nil || len(v) != 0 {
		t.Fatalf("empty value after flush: %q %v", v, err)
	}
	if ok, err := db.Exist([]byte("empty")); err != nil || !ok {
		t.Fatalf("exist after flush: %v %v", ok, err)
	}

	if err := db.Delete([]byte("empty")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if _, err := db.Get([]byte("empty")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted empty value still found: %v", err)
	}
}

func TestRegressionDoubleClose(t *testing.T) {
	db := openRegressionDB(t, nil)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestRegressionAutoCompactChannel(t *testing.T) {
	db := openRegressionDB(t, func(opt *Options) {
		opt.AutoCompactSupport = true
	})
	if db.compactChan == nil {
		t.Fatal("compactChan is nil")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegressionRaftBatchCommand(t *testing.T) {
	db := openRegressionDB(t, nil)
	defer db.Close()
	sm := NewKVStateMachine(1, 1, db)

	batch := BatchCommand{
		Op: OpBatch,
		Commands: []Command{
			{Op: OpPut, Key: []byte("b1"), Value: []byte("v1")},
			{Op: OpPut, Key: []byte("b2"), Value: []byte("v2")},
			{Op: OpDelete, Key: []byte("b1")},
		},
	}
	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Update(smachine.Entry{Index: 1, Cmd: data}); err != nil {
		t.Fatalf("update batch: %v", err)
	}
	if _, err := db.Get([]byte("b1")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("b1 should be deleted, err=%v", err)
	}
	if v, err := db.Get([]byte("b2")); err != nil || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("b2: %q %v", v, err)
	}
}

func TestRegressionAddNodeNoDeadlock(t *testing.T) {
	rdb := &ClusterDB{config: ClusterOptions{NodeID: 1}}
	done := make(chan error, 1)
	go func() { done <- rdb.AddNode(2, "localhost:1") }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNodeNotStarted) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AddNode deadlocked")
	}
}

func TestRegressionRaftSnapshotSelfContained(t *testing.T) {
	srcDir := t.TempDir()
	srcOpt := DefaultOptions
	srcOpt.DirPath = srcDir
	srcOpt.SnapshotPath = srcDir + "/snapshots"
	srcOpt.EnableBloomFilter = false
	srcDB, err := Open(srcOpt)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcDB.Put([]byte("snap-k"), []byte("snap-v")); err != nil {
		t.Fatal(err)
	}
	sm := NewKVStateMachine(1, 1, srcDB)
	defer sm.Close()

	var buf bytes.Buffer
	if err := sm.SaveSnapshot(&buf, nil, nil); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	dstDir := t.TempDir()
	dstOpt := DefaultOptions
	dstOpt.DirPath = dstDir
	dstOpt.SnapshotPath = dstDir + "/snapshots"
	dstOpt.EnableBloomFilter = false
	dstDB, err := Open(dstOpt)
	if err != nil {
		t.Fatal(err)
	}
	dstSM := NewKVStateMachine(1, 1, dstDB)
	if err := dstSM.RecoverFromSnapshot(bytes.NewReader(buf.Bytes()), nil, nil); err != nil {
		t.Fatalf("recover snapshot: %v", err)
	}
	defer dstSM.Close()

	v, err := dstSM.db.Get([]byte("snap-k"))
	if err != nil || !bytes.Equal(v, []byte("snap-v")) {
		t.Fatalf("snapshot data: %q %v", v, err)
	}
}
func TestRegressionRecoverCompactionTemp(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions
	opt.DirPath = dir
	opt.SnapshotPath = dir + "/snapshots"
	opt.EnableBloomFilter = false
	opt.MemtableSize = 1 << 20
	opt.ValueLogFileSize = 16 << 20

	db, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟压缩崩溃：旧正式文件被删，只剩 .temp。
	files, err := filepath.Glob(filepath.Join(dir, "*.VLOG.0"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no vlog file")
	}
	for _, path := range files {
		if err := os.Rename(path, path+".temp"); err != nil {
			t.Fatal(err)
		}
	}

	db2, err := Open(opt)
	if err != nil {
		t.Fatalf("open after temp crash: %v", err)
	}
	defer db2.Close()
	v, err := db2.Get([]byte("k"))
	if err != nil || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("recover temp: %q %v", v, err)
	}
}
func TestRegressionHashBloomRestart(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions
	opt.DirPath = dir
	opt.SnapshotPath = dir + "/snapshots"
	opt.MemtableSize = 1 << 20
	opt.EnableBloomFilter = true
	opt.IndexType = Hash

	db, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("hk"), []byte("hv")); err != nil {
		t.Fatal(err)
	}
	db.flushMemtable(db.activeMem)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	v, err := db2.Get([]byte("hk"))
	if err != nil || !bytes.Equal(v, []byte("hv")) {
		t.Fatalf("hash bloom restart: %q %v", v, err)
	}
}
