package knocidb

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaftSnapshotUploadsToObjectTier(t *testing.T) {
	base := t.TempDir()
	fake := newFakeObjectStore()

	config := createTestRaftConfig(1, 400, "localhost:10300", base)
	options := createTestDBOptions(t, base)
	options.ObjectTier = ObjectTierOptions{
		Enabled: true,
		Store:   fake,
		Prefix:  "raft-node-1",
	}

	rdb, err := OpenRaft(config, options)
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

func TestRaftNodeDiskLossRestoresFromObjectTier(t *testing.T) {
	base := t.TempDir()
	fake := newFakeObjectStore()
	dbDir := filepath.Join(base, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	config := createTestRaftConfig(1, 401, "localhost:10301", base)
	options := createTestDBOptions(t, base)
	options.DirPath = dbDir
	options.ObjectTier = ObjectTierOptions{
		Enabled: true,
		Store:   fake,
		Prefix:  "raft-disk-loss",
	}

	rdb, err := OpenRaft(config, options)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return rdb.IsLeader() }, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, rdb.Put([]byte("disk-loss-key"), []byte("disk-loss-value")))
	require.NoError(t, rdb.stateMachine.SaveSnapshot(io.Discard, nil, nil))
	require.NoError(t, rdb.Close())

	// 模拟数据盘丢失，但保留 Raft 目录。
	require.NoError(t, os.RemoveAll(dbDir))
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	reopened, err := OpenRaft(config, options)
	require.NoError(t, err)
	defer reopened.Close()
	require.Eventually(t, func() bool { return reopened.IsLeader() }, 5*time.Second, 100*time.Millisecond)
	value, err := reopened.Get([]byte("disk-loss-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("disk-loss-value"), value)
}
