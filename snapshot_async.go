package knocidb

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

var (
	// ErrSnapshotUnsupportedIndex 表示当前索引类型不支持在线快照。
	ErrSnapshotUnsupportedIndex = errors.New("online snapshot requires B+Tree index")
	// ErrSnapshotInProgress 表示已经有一个快照在运行。
	ErrSnapshotInProgress = errors.New("snapshot already in progress")
	// ErrSnapshotExists 表示同一秒的快照文件已存在。
	ErrSnapshotExists = errors.New("snapshot file already exists")
)

const (
	snapshotManifestExt     = ".MANIFEST"
	snapshotManifestVersion = 1
)

// SnapshotManifest 记录快照归档的元数据；它与 .ZST 分开存放，
// 用于在恢复/备份时校验归档身份和大小。
type SnapshotManifest struct {
	Version     int            `json:"version"`
	CreatedAt   time.Time      `json:"created_at"`
	Archive     string         `json:"archive"`
	ArchiveSize int64          `json:"archive_size"`
	Files       []SnapshotFile `json:"files"`
}

// SnapshotFile 是快照归档内的一个文件条目。
type SnapshotFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// SnapshotTask 表示一个后台快照任务。
type SnapshotTask struct {
	path         string
	manifestPath string
	done         chan struct{}
	mu           sync.Mutex
	err          error
}

// Path 返回快照目标文件路径。
func (t *SnapshotTask) Path() string { return t.path }

// ManifestPath 返回快照 manifest 文件路径。
func (t *SnapshotTask) ManifestPath() string { return t.manifestPath }

// Wait 等待快照完成。
func (t *SnapshotTask) Wait() error {
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// StartSnapshot 启动一个 B+Tree-only 后台快照。
//
// 快照使用 flushLock 阻止 flush/compact 删除正在读取的 VLOG/WAL 文件；
// 普通 Put/Delete 仍可继续写入 active memtable 和 WAL。当前版本是
// “短 check point + 后台拷贝压缩”，不是 Redis fork 的 COW 快照。
func (db *DB) StartSnapshot() (*SnapshotTask, error) {
	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()
	if db.closed || db.snapshotsClosed {
		return nil, ErrDBClosed
	}
	if db.snapshotRunning {
		return nil, ErrSnapshotInProgress
	}
	if db.options.SnapshotPath == "" {
		return nil, fmt.Errorf("SnapshotPath is empty")
	}
	timestamp := time.Now().Format(snapshotLayout)
	path := filepath.Join(db.options.SnapshotPath, timestamp+snapshotExt)
	if _, err := os.Stat(path); err == nil {
		return nil, ErrSnapshotExists
	}
	db.snapshotRunning = true
	db.snapshotWG.Add(1)
	task := &SnapshotTask{path: path, manifestPath: path + snapshotManifestExt, done: make(chan struct{})}
	go db.runSnapshot(task)
	return task, nil
}

func (db *DB) runSnapshot(task *SnapshotTask) {
	defer db.snapshotWG.Done()
	defer func() {
		db.snapshotMu.Lock()
		db.snapshotRunning = false
		db.snapshotMu.Unlock()
	}()
	err := db.snapshotTo(task.path)
	task.mu.Lock()
	task.err = err
	task.mu.Unlock()
	close(task.done)
}

func (db *DB) snapshotTo(path string) error {
	snapDir := db.options.SnapshotPath
	if err := os.MkdirAll(snapDir, os.ModePerm); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	tmpDir := filepath.Join(snapDir, "tmp_snapshot_"+filepath.Base(path))
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	if err := db.Sync(); err != nil {
		return fmt.Errorf("sync before snapshot: %w", err)
	}
	var sizes map[string]int64
	checkpointErr := func() error {
		shards := db.lockAllShards()
		defer db.unlockAllShards(shards)
		sizes, _ = db.snapshotFileSizes()
		return nil
	}()
	if checkpointErr != nil {
		return checkpointErr
	}
	copyErr := func() error {
		db.flushLock.Lock()
		defer db.flushLock.Unlock()
		return db.copySnapshotFiles(tmpDir, sizes)
	}()
	if copyErr != nil {
		return copyErr
	}
	if err := writeSnapshotArchive(tmpDir, path); err != nil {
		return err
	}
	manifest, err := buildSnapshotManifest(path, tmpDir)
	if err != nil {
		return err
	}
	if err := writeSnapshotManifest(path+snapshotManifestExt, manifest); err != nil {
		return err
	}
	if err := uploadSnapshotWithOptions(db.options, path); err != nil {
		return fmt.Errorf("upload snapshot to object storage failed: %w", err)
	}
	db.cleanupSnapshots(snapDir)
	return nil
}

func (db *DB) copySnapshotFiles(dst string, sizes map[string]int64) error {
	bt, ok := db.index.(*BPTree)
	if !ok {
		return ErrSnapshotUnsupportedIndex
	}
	// 索引：bbolt 读事务保证文件内容一致；flushLock 保证没有 flush 写入。
	for i, tree := range bt.trees {
		tx, err := tree.Begin(false)
		if err != nil {
			return fmt.Errorf("begin index snapshot %d: %w", i, err)
		}
		name := filepath.Join(dst, fmt.Sprintf(indexFileExt, i))
		f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		_, werr := tx.WriteTo(f)
		_ = tx.Rollback()
		if werr != nil {
			_ = f.Close()
			return fmt.Errorf("write index snapshot %d: %w", i, werr)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}

	entries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case name == fileLockName, name == ttlDBFileName, name == discardMetaName:
			continue
		case strings.Contains(name, ".BLOOM"):
			// Bloom 可由 Open 重建，避免复制自动保存中的文件。
			continue
		case name == commitLogName:
			if err := copyFileSize(filepath.Join(db.options.DirPath, name), filepath.Join(dst, name), sizes[name]); err != nil {
				return err
			}
		case strings.Contains(name, ".SEG."):
			if err := copyFileSize(filepath.Join(db.options.DirPath, name), filepath.Join(dst, name), sizes[name]); err != nil {
				return err
			}
		case strings.Contains(name, ".VLOG."):
			if strings.Contains(name, ".temp") {
				continue
			}
			if err := copyFileSize(filepath.Join(db.options.DirPath, name), filepath.Join(dst, name), sizes[name]); err != nil {
				return err
			}
		}
	}
	if err := db.ttl.writeTo(filepath.Join(dst, ttlDBFileName)); err != nil {
		return fmt.Errorf("snapshot ttl: %w", err)
	}
	if err := storeDiscardEntryMeta(filepath.Join(dst, discardMetaName), db.vlog.discardNumber, db.vlog.totalNumber); err != nil {
		return fmt.Errorf("snapshot discard metadata: %w", err)
	}
	return nil
}

func (db *DB) snapshotFileSizes() (map[string]int64, error) {
	sizes := make(map[string]int64)
	entries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.Contains(name, ".SEG.") && !strings.Contains(name, ".VLOG.") && name != commitLogName {
			continue
		}
		if strings.Contains(name, ".temp") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		sizes[name] = info.Size()
	}
	return sizes, nil
}

func copyFileSize(src, dst string, size int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(out, in, size); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func validateSnapshotManifest(snapPath string) error {
	data, err := os.ReadFile(snapPath + snapshotManifestExt)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot manifest: %w", err)
	}
	var manifest SnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("parse snapshot manifest: %w", err)
	}
	if manifest.Version != snapshotManifestVersion {
		return fmt.Errorf("unsupported snapshot manifest version %d", manifest.Version)
	}
	if manifest.Archive != filepath.Base(snapPath) {
		return fmt.Errorf("snapshot manifest archive mismatch: %q", manifest.Archive)
	}
	info, err := os.Stat(snapPath)
	if err != nil {
		return err
	}
	if info.Size() != manifest.ArchiveSize {
		return fmt.Errorf("snapshot manifest size mismatch: %d != %d", info.Size(), manifest.ArchiveSize)
	}
	return nil
}

func buildSnapshotManifest(snapFile, srcDir string) (*SnapshotManifest, error) {
	stat, err := os.Stat(snapFile)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return nil, err
	}
	files := make([]SnapshotFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		files = append(files, SnapshotFile{Name: entry.Name(), Size: info.Size()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	return &SnapshotManifest{
		Version:     snapshotManifestVersion,
		CreatedAt:   time.Now().UTC(),
		Archive:     filepath.Base(snapFile),
		ArchiveSize: stat.Size(),
		Files:       files,
	}, nil
}

func writeSnapshotManifest(path string, manifest *SnapshotManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
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
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func writeSnapshotArchive(srcDir, snapFile string) error {
	tmpFile := snapFile + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmpFile)
		}
	}()
	encoder, err := zstd.NewWriter(f)
	if err != nil {
		return err
	}
	crc := crc32.NewIEEE()
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		_ = encoder.Close()
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(srcDir, name)
		in, err := os.Open(path)
		if err != nil {
			_ = encoder.Close()
			return err
		}
		stat, err := in.Stat()
		if err != nil {
			_ = in.Close()
			_ = encoder.Close()
			return err
		}
		nameBytes := []byte(name)
		if err := writeInt32(encoder, int32(len(nameBytes))); err != nil {
			_ = in.Close()
			_ = encoder.Close()
			return err
		}
		if _, err := encoder.Write(nameBytes); err != nil {
			_ = in.Close()
			_ = encoder.Close()
			return err
		}
		if err := writeInt64(encoder, stat.Size()); err != nil {
			_ = in.Close()
			_ = encoder.Close()
			return err
		}
		if _, err := crc.Write(nameBytes); err != nil {
			_ = in.Close()
			_ = encoder.Close()
			return err
		}
		if _, err := crc.Write([]byte(fmt.Sprintf("%d", stat.Size()))); err != nil {
			_ = in.Close()
			_ = encoder.Close()
			return err
		}
		if _, err := io.Copy(encoder, io.TeeReader(in, crc)); err != nil {
			_ = in.Close()
			_ = encoder.Close()
			return err
		}
		if err := in.Close(); err != nil {
			_ = encoder.Close()
			return err
		}
	}
	if err := writeInt32(encoder, -1); err != nil {
		_ = encoder.Close()
		return err
	}
	if err := binaryWriteUint32(encoder, crc.Sum32()); err != nil {
		_ = encoder.Close()
		return err
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpFile, snapFile); err != nil {
		return err
	}
	ok = true
	if dir, err := os.Open(filepath.Dir(snapFile)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func binaryWriteUint32(w io.Writer, v uint32) error {
	var buf [4]byte
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	_, err := w.Write(buf[:])
	return err
}
