package knocidb

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLatestSnapshotFile(t *testing.T) {
	dir := t.TempDir()
	if _, err := latestSnapshotFile(dir); err == nil {
		t.Fatal("empty dir should fail")
	}
	for _, name := range []string{"20260101000000.ZST", "20260102000000.ZST", "ignore.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := latestSnapshotFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(got) != "20260102000000.ZST" {
		t.Fatalf("latest = %s", got)
	}
}

func TestReadSnapshotIntsTruncated(t *testing.T) {
	if _, err := readInt32(bytes.NewReader([]byte{1})); err == nil {
		t.Fatal("short int32 should fail")
	}
	if _, err := readInt64(bytes.NewReader([]byte{1, 2})); err == nil {
		t.Fatal("short int64 should fail")
	}
}

func TestImportSnapshotMissingAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	if err := importSnapshotToDir(filepath.Join(dir, "missing.ZST"), filepath.Join(dir, "out")); err == nil {
		t.Fatal("missing snapshot should fail")
	}
	corrupt := filepath.Join(dir, "corrupt.ZST")
	if err := os.WriteFile(corrupt, []byte("not a zstd snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := importSnapshotToDir(corrupt, filepath.Join(dir, "out2")); err == nil {
		t.Fatal("corrupt snapshot should fail")
	}
}

func TestSnapshotArchiveRejectsTruncation(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	if err := os.MkdirAll(src, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "INDEX.0"), []byte("index"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(base, "snap.ZST")
	if err := writeSnapshotArchive(src, archive); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archive, data[:len(data)-5], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := importSnapshotToDir(archive, filepath.Join(base, "restore")); err == nil {
		t.Fatal("truncated archive should be rejected")
	}
}

func TestCleanupSnapshotsRemovesManifest(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "20200101000000.ZST")
	if err := os.WriteFile(old, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(old+snapshotManifestExt, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	db := &DB{}
	db.cleanupSnapshots(dir)
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old snapshot still exists: %v", err)
	}
	if _, err := os.Stat(old + snapshotManifestExt); !os.IsNotExist(err) {
		t.Fatalf("old snapshot manifest still exists: %v", err)
	}
}

func TestRestoreFiles(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	if err := os.MkdirAll(src, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	restoreFiles(src, dst)
	if _, err := os.Stat(filepath.Join(dst, "a")); err != nil {
		t.Fatal(err)
	}
	restoreFiles(filepath.Join(base, "missing"), dst)
}

func TestExportSnapshotEmptyPath(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = ""
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ExportSnapshot(); err == nil {
		t.Fatal("empty SnapshotPath should fail")
	}
}
