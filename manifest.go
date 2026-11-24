package knocidb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/knoci/knocidb/util"
)

const (
	dbManifestName    = "MANIFEST"
	dbManifestVersion = 1
)

// dbManifest 是数据库目录的文件清单与拓扑元数据。
type dbManifest struct {
	Version      int              `json:"version"`
	Generation   uint64           `json:"generation"`
	PartitionNum int              `json:"partition_num,omitempty"`
	HasCommitLog bool             `json:"has_commit_log,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
	Files        []dbManifestFile `json:"files"`
}

type dbManifestFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func readDBManifestInfo(dir string) (*dbManifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, dbManifestName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest dbManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if manifest.Version != dbManifestVersion {
		return nil, fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}
	return &manifest, nil
}

func readDBManifest(dir string) (uint64, error) {
	manifest, err := readDBManifestInfo(dir)
	if err != nil || manifest == nil {
		return 0, err
	}
	return manifest.Generation, nil
}

func (db *DB) writeDBManifest() error {
	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
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
		if errors.Is(err, os.ErrNotExist) {
			// 并发 flush 可能刚好删除了自己的 WAL 段，清单跳过已消失文件即可。
			continue
		}
		if err != nil {
			return err
		}
		files = append(files, dbManifestFile{Name: name, Size: info.Size()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	now := time.Now().UTC()
	createdAt := db.manifestCreatedAt
	if createdAt.IsZero() {
		createdAt = now
		db.manifestCreatedAt = createdAt
	}
	manifest := dbManifest{
		Version:      dbManifestVersion,
		Generation:   db.manifestGeneration + 1,
		PartitionNum: db.options.PartitionNum,
		HasCommitLog: true,
		CreatedAt:    createdAt,
		UpdatedAt:    now,
		Files:        files,
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
	if err := util.RenameDurable(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	db.manifestGeneration = manifest.Generation
	return util.SyncDir(db.options.DirPath)
}
