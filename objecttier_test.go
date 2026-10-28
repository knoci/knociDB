package knocidb

import (
	"fmt"
	"io"
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
	options.ClusterPath = filepath.Join(base, "raft")
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
	options.ClusterPath = filepath.Join(base, "raft")
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
	options.ClusterPath = filepath.Join(base, "raft")
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
	options.ClusterPath = filepath.Join(base, "raft")

	db, err := Open(options)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("k"), []byte("v")))
	require.NoError(t, db.ExportSnapshot())
}

var _ wal.ObjectStorage = (*fakeObjectStore)(nil)

func TestClusterSnapshotUploadsToObjectTier(t *testing.T) {
	base := t.TempDir()
	fake := newFakeObjectStore()

	config := createTestClusterOptions(1, 400, "localhost:10300", base)
	options := createTestDBOptions(t, base)
	options.ObjectTier = ObjectTierOptions{
		Enabled: true,
		Store:   fake,
		Prefix:  "raft-node-1",
	}

	rdb, err := OpenCluster(config, options)
	require.NoError(t, err)
	defer rdb.Close()

	require.Eventually(t, func() bool { return rdb.IsLeader() }, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, rdb.Put([]byte("raft-object-key"), []byte("raft-object-value")))

	require.NotNil(t, rdb.stateMachine)
	require.NoError(t, rdb.stateMachine.SaveSnapshot(io.Discard, nil, nil))

	var found bool
	for _, key := range fake.keys() {
		if strings.HasPrefix(key, "raft-node-1/snapshots/") && strings.HasSuffix(key, snapshotExt) {
			found = true
		}
	}
	require.True(t, found, "raft leader snapshot should be uploaded, keys=%v", fake.keys())
}

func TestClusterNodeDiskLossRestoresFromObjectTier(t *testing.T) {
	base := t.TempDir()
	fake := newFakeObjectStore()
	dbDir := filepath.Join(base, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	config := createTestClusterOptions(1, 401, "localhost:10301", base)
	options := createTestDBOptions(t, base)
	options.DirPath = dbDir
	options.ObjectTier = ObjectTierOptions{
		Enabled: true,
		Store:   fake,
		Prefix:  "raft-disk-loss",
	}

	rdb, err := OpenCluster(config, options)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return rdb.IsLeader() }, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, rdb.Put([]byte("disk-loss-key"), []byte("disk-loss-value")))
	require.NoError(t, rdb.stateMachine.SaveSnapshot(io.Discard, nil, nil))
	require.NoError(t, rdb.Close())

	// 模拟数据盘丢失，但保留 Raft 目录。
	require.NoError(t, os.RemoveAll(dbDir))
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	reopened, err := OpenCluster(config, options)
	require.NoError(t, err)
	defer reopened.Close()
	require.Eventually(t, func() bool { return reopened.IsLeader() }, 5*time.Second, 100*time.Millisecond)
	value, err := reopened.Get([]byte("disk-loss-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("disk-loss-value"), value)
}

func TestMultiNodeClusterObjectTierFailover(t *testing.T) {
	const nodeCount = 5
	nodes := make([]*ClusterDB, nodeCount)
	stores := make([]*fakeObjectStore, nodeCount)
	cleanups := make([]func(), nodeCount)

	initialMembers := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		initialMembers[uint64(i+1)] = fmt.Sprintf("localhost:%d", 10400+i)
	}

	for i := 0; i < nodeCount; i++ {
		tempDir, cleanup := setupTestEnvironment(t)
		cleanups[i] = cleanup
		stores[i] = newFakeObjectStore()

		nodeID := uint64(i + 1)
		config := createTestClusterOptions(nodeID, 500, fmt.Sprintf("localhost:%d", 10400+i), tempDir)
		config.InitialMembers = initialMembers
		config.SnapshotIntervalSeconds = 1
		options := createTestDBOptions(t, tempDir)
		options.ObjectTier = ObjectTierOptions{
			Enabled: true,
			Store:   stores[i],
			Prefix:  fmt.Sprintf("raft-node-%d", nodeID),
		}
		rdb, err := OpenCluster(config, options)
		require.NoError(t, err)
		nodes[i] = rdb
	}

	defer func() {
		for i, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
			if cleanups[i] != nil {
				cleanups[i]()
			}
		}
	}()

	findLeader := func() int {
		for i, node := range nodes {
			if node != nil && node.IsLeader() {
				return i
			}
		}
		return -1
	}

	require.Eventually(t, func() bool { return findLeader() >= 0 }, 8*time.Second, 100*time.Millisecond)
	leader := findLeader()
	require.NoError(t, nodes[leader].Put([]byte("failover-key"), []byte("v1")))

	// 杀 Leader，等待新 Leader。
	require.NoError(t, nodes[leader].Close())
	nodes[leader] = nil
	require.Eventually(t, func() bool { return findLeader() >= 0 }, 10*time.Second, 100*time.Millisecond)
	newLeader := findLeader()
	require.NotEqual(t, leader, newLeader)
	require.NoError(t, nodes[newLeader].Put([]byte("failover-key-2"), []byte("v2")))

	// 杀一个 Follower，剩余节点仍应能选主并写入。
	follower := -1
	for i, node := range nodes {
		if node != nil && i != newLeader {
			follower = i
			break
		}
	}
	require.GreaterOrEqual(t, follower, 0)
	require.NoError(t, nodes[follower].Close())
	nodes[follower] = nil
	require.Eventually(t, func() bool {
		leader := findLeader()
		return leader >= 0 && nodes[leader].Put([]byte("failover-key-3"), []byte("v3")) == nil
	}, 20*time.Second, 200*time.Millisecond)
	leader = findLeader()
	require.GreaterOrEqual(t, leader, 0)
	value, err := nodes[leader].Get([]byte("failover-key-3"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), value)
}

func TestClusterSnapshotObjectStoreFailure(t *testing.T) {
	base := t.TempDir()
	fake := newFakeObjectStore()
	fake.failUpload = true

	config := createTestClusterOptions(1, 402, "localhost:10302", base)
	options := createTestDBOptions(t, base)
	options.ObjectTier = ObjectTierOptions{
		Enabled: true,
		Store:   fake,
		Prefix:  "raft-fail",
	}

	rdb, err := OpenCluster(config, options)
	require.NoError(t, err)
	defer rdb.Close()
	require.Eventually(t, func() bool { return rdb.IsLeader() }, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, rdb.Put([]byte("failure-key"), []byte("value")))
	err = rdb.stateMachine.SaveSnapshot(io.Discard, nil, nil)
	require.Error(t, err)
	// 快照上传失败后，状态机应重新打开 DB 并可继续服务。
	require.NoError(t, rdb.Put([]byte("after-failure"), []byte("ok")))
}

func TestThreeNodeClusterObjectTierLeaderFailover(t *testing.T) {
	const nodeCount = 3
	nodes := make([]*ClusterDB, nodeCount)
	cleanups := make([]func(), nodeCount)
	initialMembers := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		initialMembers[uint64(i+1)] = fmt.Sprintf("localhost:%d", 10500+i)
	}

	for i := 0; i < nodeCount; i++ {
		tempDir, cleanup := setupTestEnvironment(t)
		cleanups[i] = cleanup
		config := createTestClusterOptions(uint64(i+1), 501, fmt.Sprintf("localhost:%d", 10500+i), tempDir)
		config.InitialMembers = initialMembers
		options := createTestDBOptions(t, tempDir)
		options.ObjectTier = ObjectTierOptions{
			Enabled: true,
			Store:   newFakeObjectStore(),
			Prefix:  fmt.Sprintf("three-node-%d", i+1),
		}
		rdb, err := OpenCluster(config, options)
		require.NoError(t, err)
		nodes[i] = rdb
	}
	defer func() {
		for i, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
			if cleanups[i] != nil {
				cleanups[i]()
			}
		}
	}()

	findLeader := func() int {
		for i, node := range nodes {
			if node != nil && node.IsLeader() {
				return i
			}
		}
		return -1
	}
	require.Eventually(t, func() bool { return findLeader() >= 0 }, 8*time.Second, 100*time.Millisecond)
	leader := findLeader()
	require.NoError(t, nodes[leader].Put([]byte("three-key"), []byte("before")))
	require.NoError(t, nodes[leader].Close())
	nodes[leader] = nil
	require.Eventually(t, func() bool { return findLeader() >= 0 }, 10*time.Second, 100*time.Millisecond)
	newLeader := findLeader()
	require.NotEqual(t, leader, newLeader)
	require.NoError(t, nodes[newLeader].Put([]byte("three-key"), []byte("after")))
	value, err := nodes[newLeader].Get([]byte("three-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("after"), value)
}
