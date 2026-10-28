package knocidb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDBManifestLifecycle(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = filepath.Join(dir, "snapshots")
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("manifest-key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, dbManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var first dbManifest
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatal(err)
	}
	if first.Version != dbManifestVersion || first.Generation == 0 {
		t.Fatalf("bad manifest: %+v", first)
	}
	foundIndex := false
	for _, file := range first.Files {
		if file.Name == "INDEX.0" {
			foundIndex = true
		}
		if file.Name == dbManifestName {
			t.Fatal("manifest must not list itself")
		}
	}
	if !foundIndex {
		t.Fatal("manifest did not list index file")
	}

	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, dbManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var second dbManifest
	if err := json.Unmarshal(data, &second); err != nil {
		t.Fatal(err)
	}
	if second.Generation <= first.Generation {
		t.Fatalf("manifest generation did not advance: %d -> %d", first.Generation, second.Generation)
	}
}

func TestDBManifestCorruptIsRegenerated(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.SnapshotPath = filepath.Join(dir, "snapshots")
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, dbManifestName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err = Open(options)
	if err != nil {
		t.Fatalf("open with corrupt manifest should recover: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, dbManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest dbManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("manifest was not regenerated: %v", err)
	}
}
