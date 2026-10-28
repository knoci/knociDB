package knocidb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	dbManifestName    = "MANIFEST"
	dbManifestVersion = 1
)

// dbManifest 是数据库目录的文件清单。它不是事务日志，
// 而是用于记录某次提交点对应的索引/VLOG/WAL 文件名和大小。
type dbManifest struct {
	Version    int              `json:"version"`
	Generation uint64           `json:"generation"`
	CreatedAt  time.Time        `json:"created_at"`
	UpdatedAt  time.Time        `json:"updated_at"`
	Files      []dbManifestFile `json:"files"`
}

type dbManifestFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func readDBManifest(dir string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(dir, dbManifestName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var manifest dbManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return 0, err
	}
	if manifest.Version != dbManifestVersion {
		return 0, fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}
	return manifest.Generation, nil
}

func (db *DB) writeDBManifest() error {
	entries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	files := make([]dbManifestFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == dbManifestName || name == fileLockName || name == dbManifestName+".tmp" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		files = append(files, dbManifestFile{Name: name, Size: info.Size()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	now := time.Now().UTC()
	manifest := dbManifest{
		Version:    dbManifestVersion,
		Generation: db.manifestGeneration + 1,
		CreatedAt:  now,
		UpdatedAt:  now,
		Files:      files,
	}
	data, err := json.MarshalIndent(&manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(db.options.DirPath, dbManifestName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	path := filepath.Join(db.options.DirPath, dbManifestName)
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	db.manifestGeneration = manifest.Generation
	if dir, err := os.Open(db.options.DirPath); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
