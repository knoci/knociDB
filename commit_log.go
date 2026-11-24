package knocidb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/knoci/knocidb/util"
)

// COMMIT 记录跨 shard 批次的提交点；恢复时只回放已提交批次的 WAL。
// 文件是 append-only，flush 后由 maybePruneCommitLog 按活跃 WAL 重写。
const commitLogName = "COMMIT"

type commitLog struct {
	path    string
	file    *os.File
	mu      sync.Mutex
	sync    bool
	records int
}

type commitRecord struct {
	id  uint64
	crc uint32
}

func encodeCommitRecord(id uint64) []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[:8], id)
	binary.LittleEndian.PutUint32(buf[8:], crc32.ChecksumIEEE(buf[:8]))
	return buf
}

func decodeCommitRecord(buf []byte) (commitRecord, bool) {
	if len(buf) < 12 {
		return commitRecord{}, false
	}
	id := binary.LittleEndian.Uint64(buf[:8])
	crc := binary.LittleEndian.Uint32(buf[8:12])
	if crc != crc32.ChecksumIEEE(buf[:8]) {
		return commitRecord{}, false
	}
	return commitRecord{id: id, crc: crc}, true
}

func openCommitLog(dir string, syncWrites bool) (*commitLog, map[uint64]struct{}, bool, error) {
	path := filepath.Join(dir, commitLogName)
	_, statErr := os.Stat(path)
	legacy := os.IsNotExist(statErr)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, false, err
	}
	committed := make(map[uint64]struct{})
	offset := int64(0)
	buf := make([]byte, 12)
	for {
		_, err := io.ReadFull(f, buf)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			_ = f.Close()
			return nil, nil, false, err
		}
		rec, ok := decodeCommitRecord(buf)
		if !ok {
			break
		}
		committed[rec.id] = struct{}{}
		offset += 12
	}
	info, statErr := f.Stat()
	if statErr != nil {
		_ = f.Close()
		return nil, nil, false, statErr
	}
	if offset < info.Size() {
		if err := f.Truncate(offset); err != nil {
			_ = f.Close()
			return nil, nil, false, fmt.Errorf("truncate commit log: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, nil, false, fmt.Errorf("sync truncated commit log: %w", err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, nil, false, err
	}
	if legacy {
		_ = f.Sync()
		_ = util.SyncDir(dir)
	}
	cl := &commitLog{path: path, file: f, sync: syncWrites, records: len(committed)}
	return cl, committed, legacy, nil
}

const commitLogPruneThreshold = 4096

func (db *DB) maybePruneCommitLog() {
	if db.commitLog == nil || db.commitLog.recordCount() < commitLogPruneThreshold {
		return
	}
	db.flushLock.Lock()
	defer db.flushLock.Unlock()
	if db.commitLog.recordCount() < commitLogPruneThreshold {
		return
	}
	// 裁剪与提交使用同一把锁，避免用过期的 WAL 引用覆盖新提交。
	db.batchRefMu.Lock()
	defer db.batchRefMu.Unlock()
	if err := db.commitLog.rewrite(db.batchRefs); err != nil {
		// 裁剪失败不影响写入；下次 flush 再试。
		return
	}
}

func (db *DB) commitBatch(id uint64, shardCount int, syncWrite bool) error {
	db.batchRefMu.Lock()
	defer db.batchRefMu.Unlock()
	if err := db.commitLog.append(id, syncWrite); err != nil {
		return err
	}
	db.batchRefs[id] = shardCount
	return nil
}

func (db *DB) releaseBatchRefs(ids map[uint64]struct{}) {
	db.batchRefMu.Lock()
	for id := range ids {
		if db.batchRefs[id] <= 1 {
			delete(db.batchRefs, id)
		} else {
			db.batchRefs[id]--
		}
	}
	db.batchRefMu.Unlock()
}

func (c *commitLog) append(id uint64, syncWrite bool) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file == nil {
		return fmt.Errorf("commit log is not open")
	}
	if _, err := c.file.Write(encodeCommitRecord(id)); err != nil {
		return err
	}
	c.records++
	if c.sync || syncWrite {
		return c.file.Sync()
	}
	return nil
}

func (c *commitLog) recordCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.records
}

func (c *commitLog) rewrite(refs map[uint64]int) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tmpPath := c.path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	var writeErr error
	for id := range refs {
		if _, writeErr = f.Write(encodeCommitRecord(id)); writeErr != nil {
			break
		}
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if closeErr := f.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return writeErr
	}
	if c.file != nil {
		_ = c.file.Sync()
		_ = c.file.Close()
		c.file = nil
	}
	if err := util.RenameDurable(tmpPath, c.path); err != nil {
		// Keep the old path usable for the next append attempt.
		if reopened, openErr := os.OpenFile(c.path, os.O_RDWR, 0o600); openErr == nil {
			_, _ = reopened.Seek(0, io.SeekEnd)
			c.file = reopened
		}
		_ = os.Remove(tmpPath)
		return err
	}
	if err := util.SyncDir(filepath.Dir(c.path)); err != nil {
		return err
	}
	reopened, err := os.OpenFile(c.path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if _, err := reopened.Seek(0, io.SeekEnd); err != nil {
		_ = reopened.Close()
		return err
	}
	c.file = reopened
	c.records = len(refs)
	return nil
}

func (c *commitLog) Sync() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file == nil {
		return nil
	}
	return c.file.Sync()
}

func (c *commitLog) close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file == nil {
		return nil
	}
	err := c.file.Sync()
	if closeErr := c.file.Close(); err == nil {
		err = closeErr
	}
	c.file = nil
	return err
}
