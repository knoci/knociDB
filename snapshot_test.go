package knocidb

import (
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExportAndImportSnapshot(t *testing.T) {
	dir := "testdata/snapdb"
	snapDir := "testdata/snapshots"
	os.RemoveAll(dir)
	os.RemoveAll(snapDir)
	os.MkdirAll(dir, 0755)
	os.MkdirAll(snapDir, 0755)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(snapDir)

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

	db = nil

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
	dir := "testdata/snapdb2"
	snapDir := "testdata/snapshots2"
	os.RemoveAll(dir)
	os.RemoveAll(snapDir)
	os.MkdirAll(dir, 0755)
	os.MkdirAll(snapDir, 0755)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(snapDir)
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
