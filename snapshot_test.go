package knocidb

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestExportAndImportSnapshot(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "snapdb")
	snapDir := filepath.Join(base, "snapshots")
	os.MkdirAll(dir, 0755)
	os.MkdirAll(snapDir, 0755)

	db, err := Open(Options{
		DirPath:      dir,
		SnapshotPath: snapDir,
	})
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}

	key1 := []byte("test_key1")
	value1 := []byte("test_value1")
	key2 := []byte("test_key2")
	value2 := []byte("test_value2")
	key3 := []byte("test_key3")
	value3 := []byte("test_value3")
	key4 := []byte("test_key4")
	value4 := []byte("test_value4")
	key5 := []byte("test_key5")
	value5 := []byte("test_value5")

	if err := db.Put(key1, value1); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := db.Put(key2, value2); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := db.Put(key3, value3); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := db.Put(key4, value4); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := db.Put(key5, value5); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if err := db.ExportSnapshot(); err != nil {
		t.Fatalf("export snapshot failed: %v", err)
	}

	snaps, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("read snapshot dir failed: %v", err)
	}
	var snapFile string
	for _, f := range snaps {
		if filepath.Ext(f.Name()) == ".ZST" {
			snapFile = filepath.Join(snapDir, f.Name())
			break
		}
	}
	if snapFile == "" {
		t.Fatal("no snapshot file found")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db before import failed: %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove db dir failed: %v", err)
	}

	lockFile := filepath.Join(dir, fileLockName)
	if _, err := os.Stat(lockFile); err == nil {
		if err := os.Remove(lockFile); err != nil {
			t.Fatalf("failed to remove lock file: %v", err)
		}
	}
	os.MkdirAll(dir, 0755)

	time.Sleep(100 * time.Millisecond)

	log.Println("Reopening database...")
	db, err = Open(Options{
		DirPath:      dir,
		SnapshotPath: snapDir,
	})
	if err != nil {
		t.Fatalf("reopen db failed: %v", err)
	}

	log.Println("Importing snapshot...")
	if err := db.ImportSnapshot(snapFile); err != nil {
		t.Fatalf("import snapshot failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close imported db failed: %v", err)
	}

	db, err = Open(Options{
		DirPath:      dir,
		SnapshotPath: snapDir,
	})
	if err != nil {
		t.Fatalf("reopen db after import failed: %v", err)
	}
	defer db.Close()
	log.Println("Database reopened successfully after import")

	gotValue1, err := db.Get(key1)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(gotValue1) != string(value1) {
		t.Fatalf("value mismatch, want %s, got %s", value1, gotValue1)
	}

	gotValue2, err := db.Get(key2)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(gotValue2) != string(value2) {
		t.Fatalf("value mismatch, want %s, got %s", value2, gotValue2)
	}

	gotValue3, err := db.Get(key3)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(gotValue3) != string(value3) {
		t.Fatalf("value mismatch, want %s, got %s", value3, gotValue3)
	}

	gotValue4, err := db.Get(key4)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(gotValue4) != string(value4) {
		t.Fatalf("value mismatch, want %s, got %s", value4, gotValue4)
	}

	gotValue5, err := db.Get(key5)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(gotValue5) != string(value5) {
		t.Fatalf("value mismatch, want %s, got %s", value5, gotValue5)
	}
}

func TestSnapshotCleanup(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "snapdb2")
	snapDir := filepath.Join(base, "snapshots2")
	os.MkdirAll(dir, 0755)
	os.MkdirAll(snapDir, 0755)
	db, err := Open(Options{
		DirPath:      dir,
		SnapshotPath: snapDir,
	})
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	// 连续导出多次快照
	for i := 0; i < maxSnapshots+5; i++ {
		if err := db.ExportSnapshot(); err != nil {
			t.Fatalf("exportsnapshot failed: %v", err)
		}
		db, err = Open(Options{
			DirPath:      dir,
			SnapshotPath: snapDir,
		})
		if err != nil {
			t.Fatalf("open db failed: %v", err)
		}
		time.Sleep(1000 * time.Millisecond)
	}
	db.Close()
	snaps, _ := os.ReadDir(snapDir)
	cnt := 0
	for _, f := range snaps {
		if filepath.Ext(f.Name()) == ".ZST" {
			cnt++
		}
	}
	if cnt > maxSnapshots {
		t.Fatalf("snapshot cleanup failed, got %d", cnt)
	}
}

func TestExportSnapshotError(t *testing.T) {
	db := &DB{closed: true}
	err := db.ExportSnapshot()
	if err == nil {
		t.Fatalf("should fail when db is closed")
	}
}

func TestImportSnapshotError(t *testing.T) {
	db := &DB{closed: true}
	err := db.ImportSnapshot("not_exist.zst")
	if err == nil {
		t.Fatalf("should fail when db is closed")
	}
	defer os.RemoveAll("testdata")
}

func TestAsyncSnapshotRoundTrip(t *testing.T) {
	base := t.TempDir()
	dbDir := filepath.Join(base, "db")
	snapDir := filepath.Join(base, "snapshots")

	options := DefaultOptions
	options.DirPath = dbDir
	options.SnapshotPath = snapDir
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("async-key-%03d", i))
		if err := db.Put(key, []byte(fmt.Sprintf("value-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	task, err := db.StartSnapshot()
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := task.Wait(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := os.Stat(task.ManifestPath()); err != nil {
		t.Fatalf("snapshot manifest missing: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restoreDir := filepath.Join(base, "restore")
	if err := importSnapshotToDir(task.Path(), restoreDir); err != nil {
		t.Fatal(err)
	}
	restoreOptions := DefaultOptions
	restoreOptions.DirPath = restoreDir
	restoreOptions.SnapshotPath = filepath.Join(base, "restore-snapshots")
	restored, err := Open(restoreOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("async-key-%03d", i))
		want := []byte(fmt.Sprintf("value-%03d", i))
		got, err := restored.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if string(got) != string(want) {
			t.Fatalf("value mismatch for %s: want %s got %s", key, want, got)
		}
	}
}

func TestAsyncSnapshotClosed(t *testing.T) {
	base := t.TempDir()
	options := DefaultOptions
	options.DirPath = filepath.Join(base, "db")
	options.SnapshotPath = filepath.Join(base, "snapshots")
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.StartSnapshot(); !errors.Is(err, ErrDBClosed) {
		t.Fatalf("want ErrDBClosed, got %v", err)
	}
}

func TestAsyncSnapshotWithConcurrentWrites(t *testing.T) {
	base := t.TempDir()
	options := DefaultOptions
	options.DirPath = filepath.Join(base, "db")
	options.SnapshotPath = filepath.Join(base, "snapshots")
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("base-%03d", i)), []byte("base")); err != nil {
			t.Fatal(err)
		}
	}
	task, err := db.StartSnapshot()
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = db.Put([]byte(fmt.Sprintf("concurrent-%03d", i)), []byte("concurrent"))
		}
	}()
	if err := task.Wait(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	wg.Wait()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	restoreDir := filepath.Join(base, "restore")
	if err := importSnapshotToDir(task.Path(), restoreDir); err != nil {
		t.Fatal(err)
	}
	restoreOptions := DefaultOptions
	restoreOptions.DirPath = restoreDir
	restoreOptions.SnapshotPath = filepath.Join(base, "restore-snapshots")
	restored, err := Open(restoreOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("base-%03d", i))
		if _, err := restored.Get(key); err != nil {
			t.Fatalf("base key %s missing after restore: %v", key, err)
		}
	}
}

func TestAsyncSnapshotCloseWaitsForTask(t *testing.T) {
	base := t.TempDir()
	options := DefaultOptions
	options.DirPath = filepath.Join(base, "db")
	options.SnapshotPath = filepath.Join(base, "snapshots")
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("close-key"), []byte("close-value")); err != nil {
		t.Fatal(err)
	}
	task, err := db.StartSnapshot()
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := task.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(task.Path()); err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}
}

func TestAsyncSnapshotInProgress(t *testing.T) {
	base := t.TempDir()
	options := DefaultOptions
	options.DirPath = filepath.Join(base, "db")
	options.SnapshotPath = filepath.Join(base, "snapshots")
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	db.flushLock.Lock()
	task, err := db.StartSnapshot()
	if err != nil {
		db.flushLock.Unlock()
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.StartSnapshot(); !errors.Is(err, ErrSnapshotInProgress) {
		db.flushLock.Unlock()
		_ = task.Wait()
		_ = db.Close()
		t.Fatalf("want ErrSnapshotInProgress, got %v", err)
	}
	db.flushLock.Unlock()
	if err := task.Wait(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncSnapshotRejectsCorruptManifest(t *testing.T) {
	base := t.TempDir()
	options := DefaultOptions
	options.DirPath = filepath.Join(base, "db")
	options.SnapshotPath = filepath.Join(base, "snapshots")
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	task, err := db.StartSnapshot()
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := task.Wait(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(task.ManifestPath(), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := importSnapshotToDir(task.Path(), filepath.Join(base, "restore")); err == nil {
		t.Fatal("corrupt manifest should be rejected")
	}
}
