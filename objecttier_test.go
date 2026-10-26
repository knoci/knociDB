package knocidb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knoci/knocidb/wal"
	"github.com/stretchr/testify/require"
)

type fakeObjectStore struct {
	mu         sync.Mutex
	objects    map[string][]byte
	failUpload bool
	closed     bool
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{objects: make(map[string][]byte)}
}

func (f *fakeObjectStore) Upload(localPath, objectKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fmt.Errorf("fake store closed")
	}
	if f.failUpload {
		return fmt.Errorf("injected upload failure")
	}
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	f.objects[objectKey] = data
	return nil
}

func (f *fakeObjectStore) Download(objectKey, localPath string) error {
	f.mu.Lock()
	data, ok := f.objects[objectKey]
	f.mu.Unlock()
	if !ok {
		return fmt.Errorf("object not found: %s", objectKey)
	}
	if err := os.MkdirAll(filepath.Dir(localPath), os.ModePerm); err != nil {
		return err
	}
	return os.WriteFile(localPath, data, 0o600)
}

func (f *fakeObjectStore) List() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (f *fakeObjectStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeObjectStore) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestObjectTierSnapshotRoundTrip(t *testing.T) {
	base := t.TempDir()
	dbDir := filepath.Join(base, "db")
	snapDir := filepath.Join(base, "snapshots")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))
	fake := newFakeObjectStore()

	options := DefaultOptions
	options.DirPath = dbDir
	options.SnapshotPath = snapDir
	options.RaftPath = filepath.Join(base, "raft")
	options.ObjectTier = ObjectTierOptions{
		Enabled:  true,
		Store:    fake,
		Prefix:   "test-db",
		Provider: "s3",
	}

	db, err := Open(options)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("snapshot-key"), []byte("snapshot-value")))
	require.NoError(t, db.ExportSnapshot())

	keys := fake.keys()
	require.NotEmpty(t, keys)
	var snapshotKey string
	for _, key := range keys {
		if strings.HasPrefix(key, "test-db/snapshots/") && strings.HasSuffix(key, snapshotExt) {
			snapshotKey = key
		}
	}
	require.NotEmpty(t, snapshotKey, "snapshot should be uploaded, keys=%v", keys)

	require.NoError(t, os.RemoveAll(dbDir))
	db2, err := Open(options)
	require.NoError(t, err)
	defer db2.Close()
	value, err := db2.Get([]byte("snapshot-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("snapshot-value"), value)
}

func TestObjectTierColdSegmentUploadAndNoMemtableWAL(t *testing.T) {
	base := t.TempDir()
	dbDir := filepath.Join(base, "db")
	snapDir := filepath.Join(base, "snapshots")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))
	fake := newFakeObjectStore()

	options := DefaultOptions
	options.DirPath = dbDir
	options.SnapshotPath = snapDir
	options.RaftPath = filepath.Join(base, "raft")
	options.ValueLogFileSize = 8 * KB
	options.MemtableSize = 32 * KB
	options.WaitMemSpaceTimeout = time.Second
	options.ObjectTier = ObjectTierOptions{
		Enabled:        true,
		Store:          fake,
		Prefix:         "cold-db",
		UploadInterval: 10 * time.Millisecond,
	}

	db, err := Open(options)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	value := make([]byte, 512)
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		require.NoError(t, db.Put(key, value))
	}
	require.NoError(t, db.Close())

	keys := fake.keys()
	var vlogCount int
	for _, key := range keys {
		if strings.Contains(key, ".SEG") {
			t.Fatalf("memtable WAL must not be uploaded: %s", key)
		}
		if strings.HasPrefix(key, "cold-db/segments/") && strings.Contains(key, ".VLOG.") {
			vlogCount++
		}
	}
	require.Greater(t, vlogCount, 0, "closed VLOG segments should be uploaded, keys=%v", keys)
}

func TestObjectTierSnapshotUploadFailure(t *testing.T) {
	base := t.TempDir()
	dbDir := filepath.Join(base, "db")
	snapDir := filepath.Join(base, "snapshots")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))
	fake := newFakeObjectStore()
	fake.failUpload = true

	options := DefaultOptions
	options.DirPath = dbDir
	options.SnapshotPath = snapDir
	options.RaftPath = filepath.Join(base, "raft")
	options.ObjectTier = ObjectTierOptions{
		Enabled: true,
		Store:   fake,
		Prefix:  "fail-db",
	}

	db, err := Open(options)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("k"), []byte("v")))
	err = db.ExportSnapshot()
	require.Error(t, err)
	require.Contains(t, err.Error(), "upload snapshot")
}

func TestObjectTierDisabledDoesNotUpload(t *testing.T) {
	base := t.TempDir()
	options := DefaultOptions
	options.DirPath = filepath.Join(base, "db")
	options.SnapshotPath = filepath.Join(base, "snapshots")
	options.RaftPath = filepath.Join(base, "raft")

	db, err := Open(options)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("k"), []byte("v")))
	require.NoError(t, db.ExportSnapshot())
}

var _ wal.ObjectStorage = (*fakeObjectStore)(nil)
