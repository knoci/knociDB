package knocidb

import (
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	snapshotExt    = ".ZST"
	snapshotLayout = "20060102150405"
	maxSnapshots   = 10
	snapshotExpire = 365 * 24 * time.Hour
)

// ExportSnapshot 导出快照，压缩并清理过期快照，返回错误信息。
func (db *DB) ExportSnapshot() error {
	if db.closed {
		return fmt.Errorf("Export snapshot failed: database is closed")
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("Export snapshot failed: database closed failed")
	}
	dir := db.options.DirPath
	snapDir := db.options.SnapshotPath
	if snapDir == "" {
		return fmt.Errorf("SnapshotPath is empty")
	}
	if err := os.MkdirAll(snapDir, os.ModePerm); err != nil {
		return fmt.Errorf("Failed to create snapshot directory: %v", err)
	}

	timestamp := time.Now().Format(snapshotLayout)
	snapFile := filepath.Join(snapDir, timestamp+snapshotExt)

	// 复制数据目录到临时目录，避免影响compact
	tmpCopyDir := filepath.Join(snapDir, "tmp_snapshot_copy_"+timestamp)
	if err := os.MkdirAll(tmpCopyDir, os.ModePerm); err != nil {
		return fmt.Errorf("Failed to create temp copy dir: %v", err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("Failed to read db dir: %v", err)
	}
	for _, file := range files {
		name := file.Name()
		if name == fileLockName || file.IsDir() {
			continue
		}
		fromPath := filepath.Join(dir, name)
		toPath := filepath.Join(tmpCopyDir, name)
		in, err := os.Open(fromPath)
		if err != nil {
			return fmt.Errorf("Failed to open file for copy: %v", err)
		}
		out, err := os.Create(toPath)
		if err != nil {
			inner_err := in.Close()
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			return fmt.Errorf("Failed to create file in temp copy: %v", err)
		}
		_, err = io.Copy(out, in)
		if err != nil {
			return fmt.Errorf("Failed to copy file to temp dir: %v", err)
		}
		err = in.Close()
		if err != nil {
			return fmt.Errorf("Unexpected error occur: %v", err)
		}
		err = out.Close()
		if err != nil {
			return fmt.Errorf("Unexpected error occur: %v", err)
		}
	}

	// 创建临时快照文件
	tmpFile := snapFile + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		inner_err := os.RemoveAll(tmpCopyDir)
		if inner_err != nil {
			return fmt.Errorf("Unexpected error occur: %v", inner_err)
		}
		return fmt.Errorf("Failed to create snapshot file: %v", err)
	}
	defer f.Close()

	encoder, err := zstd.NewWriter(f)
	if err != nil {
		inner_err := os.RemoveAll(tmpCopyDir)
		if inner_err != nil {
			return fmt.Errorf("Unexpected error occur: %v", inner_err)
		}
		return fmt.Errorf("Failed to create zstd encoder: %v", err)
	}
	defer encoder.Close()

	// 创建CRC计算对象
	crc := crc32.NewIEEE()

	// 写入CRC校验位前的数据
	copyFiles, err := os.ReadDir(tmpCopyDir)
	if err != nil {
		inner_err := os.RemoveAll(tmpCopyDir)
		if inner_err != nil {
			return fmt.Errorf("Unexpected error occur: %v", inner_err)
		}
		return fmt.Errorf("Failed to read temp copy dir: %v", err)
	}
	for _, file := range copyFiles {
		name := file.Name()
		path := filepath.Join(tmpCopyDir, name)
		in, err := os.Open(path)
		if err != nil {
			inner_err := os.RemoveAll(tmpCopyDir)
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			return fmt.Errorf("Failed to open file for snapshot: %v", err)
		}
		nameBytes := []byte(name)
		nameLen := int32(len(nameBytes))
		if err := writeInt32(encoder, nameLen); err != nil {
			inner_err := in.Close()
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			inner_err = os.RemoveAll(tmpCopyDir)
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			return fmt.Errorf("Failed to write file name length: %v", err)
		}
		if _, err := encoder.Write(nameBytes); err != nil {
			inner_err := in.Close()
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			inner_err = os.RemoveAll(tmpCopyDir)
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			return fmt.Errorf("Failed to write file name: %v", err)
		}
		stat, err := in.Stat()
		if err != nil {
			inner_err := in.Close()
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			inner_err = os.RemoveAll(tmpCopyDir)
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			return fmt.Errorf("Failed to stat file: %v", err)
		}
		fileSize := stat.Size()
		if err := writeInt64(encoder, fileSize); err != nil {
			inner_err := in.Close()
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			inner_err = os.RemoveAll(tmpCopyDir)
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			return fmt.Errorf("Failed to write file size: %v", err)
		}
		_, err = io.Copy(encoder, in)
		// 更新CRC计算对象
		_, err = crc.Write(nameBytes)
		if err != nil {
			return fmt.Errorf("Failed to calculate CRC for file: %v", err)
		}
		_, err = crc.Write([]byte(fmt.Sprintf("%d", fileSize)))
		if err != nil {
			return fmt.Errorf("Failed to calculate CRC for file: %v", err)
		}
		_, err = io.Copy(crc, in)
		if err != nil {
			return fmt.Errorf("Failed to calculate CRC for file: %v", err)
		}
		err = in.Close()
		if err != nil {
			inner_err := os.RemoveAll(tmpCopyDir)
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			return fmt.Errorf("Failed to copy file to snapshot: %v", err)
		}
	}

	// 计算CRC校验位
	crcValue := crc.Sum32()

	// 写入CRC校验位
	if err := binary.Write(encoder, binary.LittleEndian, crcValue); err != nil {
		inner_err := os.RemoveAll(tmpCopyDir)
		if inner_err != nil {
			return fmt.Errorf("Unexpected error occur: %v", inner_err)
		}
		return fmt.Errorf("Failed to write CRC value: %v", err)
	}

	err = encoder.Close()
	err = f.Close()
	err = os.RemoveAll(tmpCopyDir)
	if err != nil {
		return fmt.Errorf("Unexpected error occur: %v", err)
	}
	if err := os.Rename(tmpFile, snapFile); err != nil {
		return fmt.Errorf("Failed to rename snapshot file: %v", err)
	}
	log.Printf("Snapshot exported: %s", snapFile)
	db.cleanupSnapshots(snapDir)
	return nil
}

// cleanupSnapshots 清理过期和多余的快照
func (db *DB) cleanupSnapshots(snapDir string) {
	files, err := os.ReadDir(snapDir)
	if err != nil {
		log.Printf("Failed to read snapshot dir: %v", err)
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
			log.Printf("Failed to remove old snapshot: %v", err)
		} else {
			log.Printf("Removed old snapshot: %s", path)
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

	// 验证快照文件是否存在
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		return fmt.Errorf("Snapshot file not found: %s", snapPath)
	}

	dir := db.options.DirPath
	timestamp := time.Now().Format(snapshotLayout)

	// 创建临时目录用于解压快照
	tmpDir := filepath.Join(os.TempDir(), "knocidb_import_"+timestamp)
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return fmt.Errorf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir) // 确保临时目录最终被清理

	// 打开快照文件
	f, err := os.Open(snapPath)
	if err != nil {
		return fmt.Errorf("Failed to open snapshot file: %v", err)
	}
	defer f.Close()

	// 创建zstd解码器
	decoder, err := zstd.NewReader(f)
	if err != nil {
		return fmt.Errorf("Failed to create zstd decoder: %v", err)
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
			return fmt.Errorf("Failed to read file name length: %v", err)
		}
		if nameLen > 255 {
			expectedCRC = uint32(nameLen)
			break
		}

		// 读取文件名
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(decoder, nameBytes); err != nil {
			return fmt.Errorf("Failed to read file name: %v", err)
		}
		fileName := string(nameBytes)

		// 读取文件大小
		fileSize, err := readInt64(decoder)
		if err != nil {
			return fmt.Errorf("Failed to read file size: %v", err)
		}

		// 创建目标文件
		filePath := filepath.Join(tmpDir, fileName)
		out, err := os.Create(filePath)
		defer out.Close()
		if err != nil {
			return fmt.Errorf("Failed to create file: %v", err)
		}

		// 复制文件内容
		_, err = io.CopyN(out, decoder, fileSize)
		if err != nil {
			return fmt.Errorf("Failed to copy file content: %v", err)
		}

		// 更新CRC计算对象
		_, err = crc.Write(nameBytes)
		if err != nil {
			return fmt.Errorf("Failed to calculate CRC for file: %v", err)
		}
		_, err = crc.Write([]byte(fmt.Sprintf("%d", fileSize)))
		if err != nil {
			return fmt.Errorf("Failed to calculate CRC for file: %v", err)
		}
		_, err = io.Copy(crc, out)
		if err != nil {
			return fmt.Errorf("Failed to calculate CRC for file: %v", err)
		}
	}

	// 读取并验证CRC校验位
	actualCRC := crc.Sum32()
	if actualCRC != expectedCRC {
		return fmt.Errorf("CRC check failed: expected %v, got %v", expectedCRC, actualCRC)
	}

	// 确保数据库目录存在
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("Failed to create database directory: %v", err)
	}

	// 创建临时目录用于原子替换
	tmpReplaceDir := filepath.Join(os.TempDir(), "knocidb_replace_"+timestamp)
	if err := os.MkdirAll(tmpReplaceDir, os.ModePerm); err != nil {
		return fmt.Errorf("Failed to create temp replace directory: %v", err)
	}
	defer os.RemoveAll(tmpReplaceDir)

	// 将当前数据库目录中的文件移动到临时替换目录（用于回滚）
	dbFiles, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("Failed to read database directory: %v", err)
	}
	for _, file := range dbFiles {
		name := file.Name()
		if name == fileLockName || file.IsDir() {
			continue
		}
		oldPath := filepath.Join(dir, name)
		newPath := filepath.Join(tmpReplaceDir, name)
		if err := os.Rename(oldPath, newPath); err != nil {
			// 尝试回滚已移动的文件
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("Failed to move existing file: %v", err)
		}
	}

	// 将解压的文件从临时目录复制到数据库目录
	tmpFiles, err := os.ReadDir(tmpDir)
	if err != nil {
		// 尝试回滚
		restoreFiles(tmpReplaceDir, dir)
		return fmt.Errorf("Failed to read temp directory: %v", err)
	}

	for _, file := range tmpFiles {
		name := file.Name()
		fromPath := filepath.Join(tmpDir, name)
		toPath := filepath.Join(dir, name)
		in, err := os.Open(fromPath)
		if err != nil {
			// 尝试回滚
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("Failed to open file for copy: %v", err)
		}
		out, err := os.Create(toPath)
		if err != nil {
			inner_err := in.Close()
			if inner_err != nil {
				return fmt.Errorf("Unexpected error occur: %v", inner_err)
			}
			// 尝试回滚
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("Failed to create file in database dir: %v", err)
		}
		_, err = io.Copy(out, in)
		err = in.Close()
		err = out.Close()
		if err != nil {
			// 尝试回滚
			restoreFiles(tmpReplaceDir, dir)
			return fmt.Errorf("Failed to copy file to database dir: %v", err)
		}
	}

	// 导入成功，清理临时替换目录
	err = os.RemoveAll(tmpReplaceDir)
	if err != nil {
		return fmt.Errorf("Unexpected error occur: %v", err)
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
		log.Printf("Failed to read source directory for restore: %v", err)
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
			log.Printf("Failed to restore file %s: %v", name, err)
		}
	}
}
