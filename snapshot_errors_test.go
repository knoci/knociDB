package knocidb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/knoci/knocidb/util"
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

func TestSnapshotImportValidationKeepsDatabaseOpen(t *testing.T) {
	db := openRegressionDB(t, nil)
	defer db.Close()
	if err := db.Put([]byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.ImportSnapshot(filepath.Join(t.TempDir(), "missing.ZST")); err == nil {
		t.Fatal("expected import to fail")
	}
	checkRestoredKey(t, db, "old", "value")
	other, err := Open(db.GetOptions())
	if err == nil {
		_ = other.Close()
	}
	if !errors.Is(err, ErrDatabaseIsUsing) {
		t.Fatalf("database must still hold its lock: %v", err)
	}
}

func TestSnapshotImportHoldsLockThroughReplacement(t *testing.T) {
	db := openRegressionDB(t, nil)
	defer db.Close()
	ran, err := db.closeAndRun(func() error {
		other, err := Open(db.GetOptions())
		if err == nil {
			_ = other.Close()
		}
		return err
	})
	if !ran || !errors.Is(err, ErrDatabaseIsUsing) {
		t.Fatalf("replacement must hold the database lock: %v, %v", ran, err)
	}
}

func TestSnapshotImportRollsBackPartialInstall(t *testing.T) {
	db, archive := snapshotRestoreFixture(t)
	options := db.GetOptions()
	obstruction := filepath.Join(options.DirPath, "z-conflict")
	if err := os.Mkdir(obstruction, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := db.ImportSnapshot(archive); err == nil {
		t.Fatal("expected conflicting directory to interrupt the install")
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkRestoredKey(t, reopened, "old", "before")
	checkRestoredKey(t, reopened, "new", "")
	if st, err := os.Stat(obstruction); err != nil || !st.IsDir() {
		t.Fatalf("existing directory was not preserved: %v, %v", st, err)
	}
	if _, err := os.Stat(filepath.Join(options.DirPath, snapshotRestoreDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active restore journal was not retired: %v", err)
	}
}

func TestOpenRollsBackInterruptedSnapshotImport(t *testing.T) {
	db, archive := snapshotRestoreFixture(t)
	options := db.GetOptions()
	dir := options.DirPath
	ran, err := db.closeAndRun(func() error {
		stage, err := prepareSnapshotRestore(archive, dir)
		if err != nil {
			return err
		}
		active := filepath.Join(dir, snapshotRestoreDir)
		if err := util.RenameDurable(stage, active); err != nil {
			return err
		}
		if err := installSnapshotFiles(dir, active); err != nil {
			return err
		}
		// The first rollback move has completed, but the journal is still active.
		return util.RenameDurable(filepath.Join(dir, commitLogName), filepath.Join(active, snapshotRestoreNew, commitLogName))
	})
	if !ran || err != nil {
		t.Fatalf("simulate interrupted install: %v, %v", ran, err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkRestoredKey(t, reopened, "old", "before")
	checkRestoredKey(t, reopened, "new", "")
	if err := reopened.ImportSnapshot(archive); err != nil {
		t.Fatalf("retry import: %v", err)
	}
	restored, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	checkRestoredKey(t, restored, "old", "")
	checkRestoredKey(t, restored, "new", "after")
}

func snapshotRestoreFixture(t *testing.T) (*DB, string) {
	t.Helper()
	source := openRegressionDB(t, nil)
	t.Cleanup(func() { _ = source.Close() })
	if err := source.Put([]byte("new"), []byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source.GetOptions().DirPath, "z-conflict"), []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := source.ExportSnapshot(); err != nil {
		t.Fatal(err)
	}
	archive, err := latestSnapshotFile(source.GetOptions().SnapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	db := openRegressionDB(t, nil)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Put([]byte("old"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	return db, archive
}

func checkRestoredKey(t *testing.T, db *DB, key, expected string) {
	t.Helper()
	value, err := db.Get([]byte(key))
	if expected == "" {
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("get %q: %q, %v, want ErrKeyNotFound", key, value, err)
		}
	} else if err != nil || string(value) != expected {
		t.Fatalf("get %q: %q, %v, want %q", key, value, err, expected)
	}
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

func TestImportSnapshotValidArchive(t *testing.T) {
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
	out := filepath.Join(base, "out")
	if err := importSnapshotToDir(archive, out); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(out, "INDEX.0"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "index" {
		t.Fatalf("unexpected content: %q", got)
	}
}

func TestImportSnapshotTargetIsFile(t *testing.T) {
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
	target := filepath.Join(base, "target")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := importSnapshotToDir(archive, target); err == nil {
		t.Fatal("target file should fail")
	}
}
