package knocidb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStartSnapshotRequiresPath(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SnapshotPath = ""
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.StartSnapshot(); err == nil {
		t.Fatal("empty SnapshotPath should fail")
	}
}

func TestSnapshotManifestValidation(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "snap.ZST")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := archive + snapshotManifestExt
	if err := validateSnapshotManifest(archive); err != nil {
		t.Fatalf("missing manifest should be allowed: %v", err)
	}

	write := func(v any) {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write(SnapshotManifest{Version: 999, Archive: "snap.ZST", ArchiveSize: 7})
	if err := validateSnapshotManifest(archive); err == nil {
		t.Fatal("wrong version should fail")
	}

	write(SnapshotManifest{Version: snapshotManifestVersion, Archive: "other.ZST", ArchiveSize: 7})
	if err := validateSnapshotManifest(archive); err == nil {
		t.Fatal("wrong archive should fail")
	}

	write(SnapshotManifest{Version: snapshotManifestVersion, Archive: "snap.ZST", ArchiveSize: 8})
	if err := validateSnapshotManifest(archive); err == nil {
		t.Fatal("wrong size should fail")
	}

	write(SnapshotManifest{Version: snapshotManifestVersion, Archive: "snap.ZST", ArchiveSize: 7})
	if err := validateSnapshotManifest(archive); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}

	if err := os.WriteFile(manifestPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateSnapshotManifest(archive); err == nil {
		t.Fatal("malformed JSON should fail")
	}
}

func TestWriteSnapshotManifestError(t *testing.T) {
	if err := writeSnapshotManifest(filepath.Join(t.TempDir(), "missing", "x"), &SnapshotManifest{}); err == nil {
		t.Fatal("write into missing dir should fail")
	}
}

func TestCopyFileSizeErrors(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFileSize(src, filepath.Join(dir, "dst"), 2); err != nil {
		t.Fatal(err)
	}
	if err := copyFileSize(filepath.Join(dir, "missing"), filepath.Join(dir, "dst2"), 1); err == nil {
		t.Fatal("missing source should fail")
	}
	if err := copyFileSize(src, filepath.Join(dir, "dst3"), 10); err == nil {
		t.Fatal("short source should fail")
	}
}
