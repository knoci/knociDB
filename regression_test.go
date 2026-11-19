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
	db := openRegressionDB(t, func(opt *Options) {
		opt.PartitionNum = 1
		opt.ValueLogFileSize = 80 << 10
	})
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
	if err := db.flushMemtableE(db.activeMem); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k3"), value); err != nil {
		t.Fatal(err)
	}
	if err := db.flushMemtableE(db.activeMem); err != nil {
		t.Fatal(err)
	}

	// 压缩输出需要轮转到第二个新段。
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")} {
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %s after compact: %v", key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value mismatch for %s", key)
		}
	}
}

type compactFaultIndex struct {
	Index
	getCalls int
	putCalls int
	failGet  int
	failPut  int
	failSync bool
}

var errCompactTestFault = errors.New("injected compaction failure")

func (i *compactFaultIndex) Get(key []byte) (*KeyPosition, error) {
	i.getCalls++
	if i.getCalls == i.failGet {
		return nil, errCompactTestFault
	}
	return i.Index.Get(key)
}

func (i *compactFaultIndex) PutBatch(positions []*KeyPosition) ([]*KeyPosition, error) {
	i.putCalls++
	if i.putCalls == i.failPut {
		return nil, errCompactTestFault
	}
	return i.Index.PutBatch(positions)
}

func (i *compactFaultIndex) Sync() error {
	if i.failSync {
		return errCompactTestFault
	}
	return i.Index.Sync()
}

func TestRegressionCompactFailurePreservesValues(t *testing.T) {
	for _, mode := range []struct {
		name    string
		compact func(*DB) error
	}{
		{"full", (*DB).Compact},
		{"discard", (*DB).CompactWithDiscardtable},
	} {
		t.Run(mode.name, func(t *testing.T) {
			for _, fault := range []struct {
				name  string
				index compactFaultIndex
			}{
				{"lookup_during_copy", compactFaultIndex{failGet: 2}},
				{"second_index_update", compactFaultIndex{failPut: 2}},
				{"index_sync", compactFaultIndex{failSync: true}},
			} {
				t.Run(fault.name, func(t *testing.T) {
					db := openRegressionDB(t, func(opt *Options) {
						opt.PartitionNum = 1
						opt.CompactBatchCapacity = 1
					})
					defer db.Close()
					keys := []string{"alpha", "beta", "gamma"}
					for _, key := range keys {
						if err := db.Put([]byte(key), []byte(key)); err != nil {
							t.Fatal(err)
						}
					}
					if err := db.flushMemtableE(db.shards[0].active); err != nil {
						t.Fatal(err)
					}
					old, err := db.index.Get([]byte(keys[0]))
					if err != nil || old == nil {
						t.Fatalf("initial index position: %v, %v", old, err)
					}
					index := db.index
					fault.index.Index = index
					db.index = &fault.index
					err = mode.compact(db)
					db.index = index
					if !errors.Is(err, errCompactTestFault) {
						t.Fatalf("compaction error = %v, want injected failure", err)
					}
					checkValues := func(db *DB) {
						t.Helper()
						for _, key := range keys {
							value, err := db.Get([]byte(key))
							if err != nil || string(value) != key {
								t.Fatalf("get %s after failed compaction: %q, %v", key, value, err)
							}
						}
					}
					checkValues(db)
					if _, err := db.vlog.read(old); err != nil {
						t.Fatalf("old segment reclaimed after failure: %v", err)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, err := Open(db.options)
					if err != nil {
						t.Fatal(err)
					}
					defer reopened.Close()
					checkValues(reopened)
					if err := mode.compact(reopened); err != nil {
						t.Fatalf("retry compaction: %v", err)
					}
					checkValues(reopened)
					if _, err := reopened.vlog.read(old); err == nil {
						t.Fatal("successful retry retained the old segment")
					}
				})
			}
		})
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
