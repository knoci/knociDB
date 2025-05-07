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

	// 写入测试数据
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

	// 导出快照
	if err := db.ExportSnapshot(); err != nil {
		t.Fatalf("export snapshot failed: %v", err)
	}

	// 获取最新快照文件
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

	// 清空数据目录并确保锁文件被删除
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove db dir failed: %v", err)
	}
	// 检查并删除可能残留的锁文件
	lockFile := filepath.Join(dir, fileLockName)
	if _, err := os.Stat(lockFile); err == nil {
		if err := os.Remove(lockFile); err != nil {
			t.Fatalf("failed to remove lock file: %v", err)
		}
	}
	os.MkdirAll(dir, 0755)

	// 添加短暂延迟确保资源完全释放
	time.Sleep(100 * time.Millisecond)

	// 重新打开数据库
	log.Println("Reopening database...")
	db, err = Open(Options{
		DirPath:      dir,
		SnapshotPath: snapDir,
	})
	if err != nil {
		t.Fatalf("reopen db failed: %v", err)
	}

	// 导入快照
	log.Println("Importing snapshot...")
	if err := db.ImportSnapshot(snapFile); err != nil {
		t.Fatalf("import snapshot failed: %v", err)
	}

	// ImportSnapshot已经关闭了数据库，所以不需要再次关闭
	// 设置为nil以避免defer db.Close()被调用
	db = nil

	// 重新打开数据库
	db, err = Open(Options{
		DirPath:      dir,
		SnapshotPath: snapDir,
	})
	if err != nil {
		t.Fatalf("reopen db after import failed: %v", err)
	}
	defer db.Close()
	log.Println("Database reopened successfully after import")

	// 验证数据
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
