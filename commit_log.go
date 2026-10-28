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
)

const commitLogName = "COMMIT"

type commitLog struct {
	file *os.File
	mu   sync.Mutex
	sync bool
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
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, nil, false, err
	}
	cl := &commitLog{file: f, sync: syncWrites}
	return cl, committed, legacy, nil
}

func (c *commitLog) append(id uint64, syncWrite bool) error {
	if c == nil || c.file == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.file.Write(encodeCommitRecord(id)); err != nil {
		return err
	}
	if c.sync || syncWrite {
		return c.file.Sync()
	}
	return nil
}

func (c *commitLog) Sync() error {
	if c == nil || c.file == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.file.Sync()
}

func (c *commitLog) close() error {
	if c == nil || c.file == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.file.Sync()
	if closeErr := c.file.Close(); err == nil {
		err = closeErr
	}
	c.file = nil
	return err
}
