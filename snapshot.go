package knocidb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/knoci/knocidb/util"
)

const (
	snapshotExt    = ".ZST"
	snapshotLayout = "20060102150405"
	maxSnapshots   = 10
	snapshotExpire = 365 * 24 * time.Hour
)

var snapshotSeq atomic.Uint64

func nextSnapshotStamp() string {
	now := time.Now().UTC()
	seq := snapshotSeq.Add(1)
	return fmt.Sprintf("%s.%09d-%06d", now.Format(snapshotLayout), now.Nanosecond(), seq%1000000)
}

func parseSnapshotTimestamp(name string) (time.Time, error) {
	base := strings.TrimSuffix(name, snapshotExt)
	if len(base) >= len(snapshotLayout) {
		return time.Parse(snapshotLayout, base[:len(snapshotLayout)])
	}
	return time.Parse(snapshotLayout, base)
}

// ExportSnapshot 导出离线快照。导出会关闭数据库，调用方需要之后重新 Open。
func (db *DB) ExportSnapshot() error {
	if db.closed.Load() {
		return fmt.Errorf("export snapshot failed: database is closed")
	}
	if db.options.SnapshotPath == "" {
		return fmt.Errorf("snapshot path is empty")
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("export snapshot failed: close database: %w", err)
	}
	if err := os.MkdirAll(db.options.SnapshotPath, os.ModePerm); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}

	timestamp := nextSnapshotStamp()
	snapFile := filepath.Join(db.options.SnapshotPath, timestamp+snapshotExt)
	tmpCopyDir := filepath.Join(db.options.SnapshotPath, "tmp_snapshot_copy_"+timestamp)
	if err := os.MkdirAll(tmpCopyDir, os.ModePerm); err != nil {
		return fmt.Errorf("create temp copy dir: %w", err)
	}
	defer os.RemoveAll(tmpCopyDir)

	files, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return fmt.Errorf("read db dir: %w", err)
	}
	for _, file := range files {
		name := file.Name()
		if name == fileLockName || file.IsDir() {
			continue
		}
		src := filepath.Join(db.options.DirPath, name)
		info, err := os.Stat(src)
		if err != nil {
			return fmt.Errorf("stat file for copy: %w", err)
		}
		if err := copyFileSize(src, filepath.Join(tmpCopyDir, name), info.Size()); err != nil {
			return fmt.Errorf("copy file to temp dir: %w", err)
		}
	}

	if err := writeSnapshotArchive(tmpCopyDir, snapFile); err != nil {
		return fmt.Errorf("write snapshot archive: %w", err)
	}
	if manifest, err := buildSnapshotManifest(snapFile, tmpCopyDir); err == nil {
		if err := writeSnapshotManifest(snapFile+snapshotManifestExt, manifest); err != nil {
			return fmt.Errorf("write snapshot manifest: %w", err)
		}
	}
	log.Printf("Snapshot exported: %s", snapFile)
	if err := uploadSnapshotWithOptions(db.options, snapFile); err != nil {
		return fmt.Errorf("upload snapshot to object storage failed: %w", err)
	}
	db.cleanupSnapshots(db.options.SnapshotPath)
	return nil
}

// latestSnapshotFile 返回快照目录中文件名最大的正式 .ZST 快照文件，
// 跳过 recover-*.ZST 等临时恢复文件。
func latestSnapshotFile(snapDir string) (string, error) {
	files, err := os.ReadDir(snapDir)
	if err != nil {
		return "", err
	}
	var names []string
	for _, f := range files {
		name := f.Name()
		if !f.IsDir() && strings.HasSuffix(name, snapshotExt) && !strings.HasPrefix(name, "recover-") {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no snapshot file in %s", snapDir)
	}
	sort.Strings(names)
	return filepath.Join(snapDir, names[len(names)-1]), nil
}

// cleanupSnapshots 清理过期和多余的快照
func (db *DB) cleanupSnapshots(snapDir string) {
	files, err := os.ReadDir(snapDir)
	if err != nil {
		log.Printf("failed to read snapshot dir: %v", err)
		return
	}
	var snaps []os.DirEntry
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), snapshotExt) && !strings.HasPrefix(f.Name(), "recover-") {
			snaps = append(snaps, f)
		}
	}
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].Name() < snaps[j].Name()
	})
	now := time.Now().UTC()
	var toDelete []string
	for i, f := range snaps {
		name := f.Name()
		if len(snaps)-i > maxSnapshots {
			toDelete = append(toDelete, filepath.Join(snapDir, name))
			continue
		}
		ts, err := parseSnapshotTimestamp(name)
		if err == nil && now.Sub(ts) > snapshotExpire {
			toDelete = append(toDelete, filepath.Join(snapDir, name))
		}
	}
	for _, path := range toDelete {
		if err := os.Remove(path); err != nil {
			log.Printf("failed to remove old snapshot: %v", err)
		} else {
			log.Printf("Removed old snapshot: %s", path)
		}
		if err := os.Remove(path + snapshotManifestExt); err != nil && !os.IsNotExist(err) {
			log.Printf("failed to remove snapshot manifest: %v", err)
		}
	}
}

// ImportSnapshot 校验并暂存快照，然后关闭当前实例并替换数据文件。
func (db *DB) ImportSnapshot(snapPath string) error {
	if db.fileLock == nil || db.isClosing() {
		return fmt.Errorf("import snapshot: %w", ErrDBClosed)
	}
	dir := db.options.DirPath
	stage, err := prepareSnapshotRestore(snapPath, dir)
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)

	ran, err := db.closeAndRun(func() error { return publishSnapshotRestore(dir, stage) })
	if !ran {
		return fmt.Errorf("import snapshot: %w", ErrDBClosed)
	}
	return err
}

func (db *DB) isClosing() bool {
	db.snapshotMu.Lock()
	defer db.snapshotMu.Unlock()
	return db.snapshotsClosed
}

// importSnapshotToDir 在 Open 持有目录锁时供对象存储恢复使用。
func importSnapshotToDir(snapPath, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	stage, err := prepareSnapshotRestore(snapPath, dir)
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	return publishSnapshotRestore(dir, stage)
}

const (
	snapshotRestoreDir = ".snapshot-restore"
	snapshotRestoreNew = "new"
	snapshotRestoreOld = "old"
	snapshotRestoreLog = "files.json"
)

func prepareSnapshotRestore(snapPath, dir string) (string, error) {
	if err := validateSnapshotManifest(snapPath); err != nil {
		return "", err
	}
	archive, err := os.Open(snapPath)
	if err != nil {
		return "", err
	}
	defer archive.Close()

	stage, err := os.MkdirTemp(dir, ".snapshot-stage-")
	if err != nil {
		return "", err
	}
	prepared := false
	defer func() {
		if !prepared {
			_ = os.RemoveAll(stage)
		}
	}()

	newDir := filepath.Join(stage, snapshotRestoreNew)
	if err := os.Mkdir(newDir, 0o700); err != nil {
		return "", err
	}
	if err := os.Mkdir(filepath.Join(stage, snapshotRestoreOld), 0o700); err != nil {
		return "", err
	}
	names, err := extractSnapshot(archive, newDir)
	if err != nil {
		return "", fmt.Errorf("extract snapshot: %w", err)
	}
	journal, err := os.OpenFile(filepath.Join(stage, snapshotRestoreLog), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	err = json.NewEncoder(journal).Encode(names)
	if err == nil {
		err = journal.Sync()
	}
	if err := errors.Join(err, journal.Close()); err != nil {
		return "", err
	}
	if err := errors.Join(util.SyncDir(newDir), util.SyncDir(stage)); err != nil {
		return "", err
	}
	prepared = true
	return stage, nil
}

func extractSnapshot(archive io.Reader, dir string) ([]string, error) {
	decoder, err := zstd.NewReader(archive)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	var names []string
	crc := crc32.NewIEEE()
	for {
		length, err := readInt32(decoder)
		if err != nil {
			return nil, err
		}
		if length == -1 {
			break
		}
		if length <= 0 || length > 1024 {
			return nil, fmt.Errorf("invalid snapshot filename length: %d", length)
		}
		rawName := make([]byte, length)
		if _, err := io.ReadFull(decoder, rawName); err != nil {
			return nil, err
		}
		name := string(rawName)
		if !validSnapshotFilename(name) {
			return nil, fmt.Errorf("invalid snapshot filename: %q", name)
		}
		size, err := readInt64(decoder)
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, fmt.Errorf("invalid snapshot filesize: %d", size)
		}
		_, _ = crc.Write(rawName)
		_, _ = crc.Write([]byte(strconv.FormatInt(size, 10)))
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		_, err = io.CopyN(file, io.TeeReader(decoder, crc), size)
		if err == nil {
			err = file.Sync()
		}
		if err := errors.Join(err, file.Close()); err != nil {
			return nil, fmt.Errorf("write snapshot file %q: %w", name, err)
		}
		names = append(names, name)
	}
	var expected [4]byte
	if _, err := io.ReadFull(decoder, expected[:]); err != nil {
		return nil, err
	}
	if crc.Sum32() != binary.LittleEndian.Uint32(expected[:]) {
		return nil, fmt.Errorf("snapshot CRC mismatch")
	}
	// Consume the frame footer so decompression errors are also reported.
	if n, err := io.ReadFull(decoder, expected[:1]); n != 0 || err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unexpected data after snapshot checksum")
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("snapshot contains no files")
	}
	return names, nil
}

func validSnapshotFilename(name string) bool {
	return filepath.IsLocal(name) && !strings.HasSuffix(name, ".") && !strings.HasSuffix(name, " ") &&
		!strings.ContainsAny(name, "/\\:") &&
		!strings.EqualFold(name, fileLockName) &&
		!strings.HasPrefix(strings.ToLower(name), snapshotRestoreDir)
}

// publishSnapshotRestore keeps the rollback journal active until installation
// and all directory changes are synced.
func publishSnapshotRestore(dir, stage string) error {
	active := filepath.Join(dir, snapshotRestoreDir)
	if _, err := os.Lstat(active); err == nil {
		return fmt.Errorf("an interrupted snapshot import must be recovered first")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := util.RenameDurable(stage, active); err != nil {
		return err
	}

	if err := installSnapshotFiles(dir, active); err != nil {
		return errors.Join(err, recoverSnapshotRestore(dir))
	}
	if err := retireSnapshotRestore(dir, active); err != nil {
		return errors.Join(err, recoverSnapshotRestore(dir))
	}
	return nil
}

func installSnapshotFiles(dir, active string) error {
	if err := util.SyncDir(dir); err != nil {
		return err
	}
	oldDir := filepath.Join(active, snapshotRestoreOld)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || strings.EqualFold(name, fileLockName) {
			continue
		}
		if err := util.RenameDurable(filepath.Join(dir, name), filepath.Join(oldDir, name)); err != nil {
			return fmt.Errorf("save previous database file %q: %w", name, err)
		}
	}
	if err := errors.Join(util.SyncDir(oldDir), util.SyncDir(dir)); err != nil {
		return err
	}

	newDir := filepath.Join(active, snapshotRestoreNew)
	entries, err = os.ReadDir(newDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if err := util.RenameDurable(filepath.Join(newDir, name), filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("install snapshot file %q: %w", name, err)
		}
	}
	return errors.Join(util.SyncDir(dir), util.SyncDir(newDir))
}

func recoverSnapshotRestore(dir string) error {
	active := filepath.Join(dir, snapshotRestoreDir)
	if _, err := os.Lstat(active); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}

	data, err := os.ReadFile(filepath.Join(active, snapshotRestoreLog))
	if err != nil {
		return err
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return err
	}

	// A missing staged file was installed. Move it back before restoring any
	// old files, so a second rollback after interruption is safe to repeat.
	newDir := filepath.Join(active, snapshotRestoreNew)
	for _, name := range names {
		if !validSnapshotFilename(name) {
			return fmt.Errorf("invalid snapshot journal filename: %q", name)
		}
		staged := filepath.Join(newDir, name)
		if _, err := os.Lstat(staged); errors.Is(err, os.ErrNotExist) {
			if err := util.RenameDurable(filepath.Join(dir, name), staged); err != nil {
				return fmt.Errorf("roll back snapshot file %q: %w", name, err)
			}
		} else if err != nil {
			return err
		}
	}
	if err := errors.Join(util.SyncDir(newDir), util.SyncDir(dir)); err != nil {
		return err
	}

	oldDir := filepath.Join(active, snapshotRestoreOld)
	entries, err := os.ReadDir(oldDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if err := util.RenameDurable(filepath.Join(oldDir, name), filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("restore previous database file %q: %w", name, err)
		}
	}
	if err := errors.Join(util.SyncDir(dir), util.SyncDir(oldDir)); err != nil {
		return err
	}
	return retireSnapshotRestore(dir, active)
}

func retireSnapshotRestore(dir, active string) error {
	done, err := os.MkdirTemp(dir, ".snapshot-done-")
	if err != nil {
		return err
	}
	if err := util.RenameDurable(active, filepath.Join(done, "restore")); err != nil {
		_ = os.Remove(done)
		return err
	}
	if err := errors.Join(util.SyncDir(done), util.SyncDir(dir)); err != nil {
		return err
	}
	if err := os.RemoveAll(done); err != nil {
		log.Printf("remove completed snapshot restore directory: %v", err)
	}
	return nil
}

// 工具函数：写入int32/int64，读取int32/int64
func writeInt32(w io.Writer, v int32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	_, err := w.Write(buf)
	return err
}
func writeInt64(w io.Writer, v int64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	_, err := w.Write(buf)
	return err
}
func readInt32(r io.Reader) (int32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(buf)), nil
}
func readInt64(r io.Reader) (int64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buf)), nil
}
