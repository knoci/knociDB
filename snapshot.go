package knocidb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	snapshotExt    = ".ZST"
	snapshotLayout = "20060102150405"
	maxSnapshots   = 10
	snapshotExpire = 365 * 24 * time.Hour
)

// ExportSnapshot 导出离线快照。导出会关闭数据库，调用方需要之后重新 Open。
func (db *DB) ExportSnapshot() error {
	if db.closed {
		return fmt.Errorf("export snapshot failed: database is closed")
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("export snapshot failed: close database: %w", err)
	}
	if db.options.SnapshotPath == "" {
		return fmt.Errorf("snapshot path is empty")
	}
	if err := os.MkdirAll(db.options.SnapshotPath, os.ModePerm); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}

	timestamp := time.Now().Format(snapshotLayout)
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
	log.Printf("Snapshot exported: %s", snapFile)
	if err := uploadSnapshotWithOptions(db.options, snapFile); err != nil {
		return fmt.Errorf("upload snapshot to object storage failed: %w", err)
	}
	db.cleanupSnapshots(db.options.SnapshotPath)
	return nil
}

// latestSnapshotFile 返回快照目录中文件名最大的 .ZST 文件。
func latestSnapshotFile(snapDir string) (string, error) {
	files, err := os.ReadDir(snapDir)
	if err != nil {
		return "", err
	}
	var names []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), snapshotExt) {
			names = append(names, f.Name())
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
		if strings.HasSuffix(f.Name(), snapshotExt) {
			snaps = append(snaps, f)
		}
	}
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].Name() < snaps[j].Name()
	})
	now := time.Now()
	var toDelete []string
	for i, f := range snaps {
		name := f.Name()
		if len(snaps)-i > maxSnapshots {
			toDelete = append(toDelete, filepath.Join(snapDir, name))
			continue
		}
		ts, err := time.Parse(snapshotLayout, strings.TrimSuffix(name, snapshotExt))
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

// ImportSnapshot 导入快照并恢复数据库，保证跨平台兼容性和原子性操作
func (db *DB) ImportSnapshot(snapPath string) error {
	if db.closed {
		return fmt.Errorf("Import snapshot failed: database is closed")
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("Close failed: %v", err)
	}
	return importSnapshotToDir(snapPath, db.options.DirPath)
}

// importSnapshotToDir 把 .ZST 快照解压并原子替换到目标目录。
// 它不持有 DB，因此可用于 Open 之前的对象存储恢复。
func importSnapshotToDir(snapPath, dir string) error {
	// 验证快照文件是否存在
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		return fmt.Errorf("snapshot file not found: %s", snapPath)
	}
	if err := validateSnapshotManifest(snapPath); err != nil {
		return err
	}

	timestamp := time.Now().Format(snapshotLayout)

	// 创建临时目录用于解压快照
	tmpDir := filepath.Join(os.TempDir(), "knocidb_import_"+timestamp)
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir) // 确保临时目录最终被清理

	// 打开快照文件
	f, err := os.Open(snapPath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %v", err)
	}
	defer f.Close()

	// 创建zstd解码器
	decoder, err := zstd.NewReader(f)
	if err != nil {
		return fmt.Errorf("failed to create zstd decoder: %v", err)
	}
	defer decoder.Close()

	// 创建CRC计算对象
	crc := crc32.NewIEEE()
	var expectedCRC uint32

	// 读取并解压文件
	for {
		// 读取文件名长度
		nameLen, err := readInt32(decoder)
		if err != nil {
			if err == io.EOF {
				break // 正常结束
			}
			return fmt.Errorf("failed to read file name length: %v", err)
		}
		if nameLen == -1 {
			break // 结束标记
		}
		if nameLen < 0 {
			return fmt.Errorf("invalid snapshot file name length: %d", nameLen)
		}

		// 读取文件名
		nameBytes := make([]byte, nameLen)
		if _, err = io.ReadFull(decoder, nameBytes); err != nil {
			return fmt.Errorf("failed to read file name: %v", err)
		}
		fileName := string(nameBytes)

		// 读取文件大小
		fileSize, err := readInt64(decoder)
		if err != nil {
			return fmt.Errorf("failed to read file size: %v", err)
		}
		if fileSize < 0 {
			return fmt.Errorf("invalid snapshot file size: %d", fileSize)
		}

		// 创建目标文件
		filePath := filepath.Join(tmpDir, fileName)
		out, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %v", err)
		}

		// CRC 同样覆盖 name + size + content
		if _, err = crc.Write(nameBytes); err != nil {
			_ = out.Close()
			return fmt.Errorf("failed to calculate CRC for file: %v", err)
		}
		if _, err = crc.Write([]byte(fmt.Sprintf("%d", fileSize))); err != nil {
			_ = out.Close()
			return fmt.Errorf("failed to calculate CRC for file: %v", err)
		}
		if _, err = io.CopyN(out, io.TeeReader(decoder, crc), fileSize); err != nil {
			_ = out.Close()
			return fmt.Errorf("failed to copy file content: %v", err)
		}
		if err = out.Close(); err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}
	}

	// 读取并验证CRC校验位
	crcBytes := make([]byte, 4)
	if _, err := io.ReadFull(decoder, crcBytes); err != nil {
		return fmt.Errorf("failed to read CRC value: %v", err)
	}
	expectedCRC = binary.LittleEndian.Uint32(crcBytes)

	actualCRC := crc.Sum32()
	if actualCRC != expectedCRC {
		return fmt.Errorf("CRC check failed: expected %v, got %v", expectedCRC, actualCRC)
	}

	// 确保数据库目录存在
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create database directory: %v", err)
	}

	// 创建临时目录用于原子替换
	tmpReplaceDir := filepath.Join(os.TempDir(), "knocidb_replace_"+timestamp)
	if err = os.MkdirAll(tmpReplaceDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create temp replace directory: %v", err)
	}
	defer os.RemoveAll(tmpReplaceDir)

	// 将当前数据库目录中的文件移动到临时替换目录（用于回滚）
	dbFiles, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read database directory: %v", err)
	}
	for _, file := range dbFiles {
		name := file.Name()
		if name == fileLockName || file.IsDir() {
			continue
		}
		oldPath := filepath.Join(dir, name)
		newPath := filepath.Join(tmpReplaceDir, name)
		if err = os.Rename(oldPath, newPath); err != nil {
			// 尝试回滚已移动的文件
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("failed to move existing file: %v", err)
		}
	}

	// 将解压的文件从临时目录复制到数据库目录
	tmpFiles, err := os.ReadDir(tmpDir)
	if err != nil {
		// 尝试回滚
		restoreFiles(tmpReplaceDir, dir)
		return fmt.Errorf("failed to read temp directory: %v", err)
	}

	for _, file := range tmpFiles {
		name := file.Name()
		fromPath := filepath.Join(tmpDir, name)
		toPath := filepath.Join(dir, name)
		in, err := os.Open(fromPath)
		if err != nil {
			// 尝试回滚
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("failed to open file for copy: %v", err)
		}
		out, err := os.Create(toPath)
		if err != nil {
			inner_err := in.Close()
			if inner_err != nil {
				return fmt.Errorf("unexpected error: %w", inner_err)
			}
			// 尝试回滚
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("failed to create file in database dir: %v", err)
		}
		_, err = io.Copy(out, in)
		err = in.Close()
		err = out.Close()
		if err != nil {
			// 尝试回滚
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("failed to copy file to database dir: %v", err)
		}
	}

	// 导入成功，清理临时替换目录
	err = os.RemoveAll(tmpReplaceDir)
	if err != nil {
		return fmt.Errorf("unexpected error: %w", err)
	}
	log.Printf("Snapshot imported successfully: %s", snapPath)
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

// restoreFiles 从源目录恢复文件到目标目录，用于导入快照失败时的回滚操作
func restoreFiles(srcDir, dstDir string) {
	files, err := os.ReadDir(srcDir)
	if err != nil {
		log.Printf("failed to read source directory for restore: %v", err)
		return
	}
	for _, file := range files {
		name := file.Name()
		if file.IsDir() {
			continue
		}
		srcPath := filepath.Join(srcDir, name)
		dstPath := filepath.Join(dstDir, name)
		if err := os.Rename(srcPath, dstPath); err != nil {
			log.Printf("failed to restore file %s: %v", name, err)
		}
	}
}
