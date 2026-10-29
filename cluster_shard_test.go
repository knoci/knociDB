package knocidb

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func freeClusterAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func TestShardedClusterSingleNode(t *testing.T) {
	tempDir := t.TempDir()
	addr := freeClusterAddress(t)
	config := createTestClusterOptions(1, 200, addr, tempDir)
	config.ShardCount = 2
	config.ShardMapEpoch = 1
	options := createTestDBOptions(t, tempDir)
	nm, err := OpenCluster(config, options)
	require.NoError(t, err)
	defer nm.Close()

	require.Eventually(t, func() bool {
		shards := nm.Shards()
		return len(shards) == 2 && shards[0].LeaderID != 0 && shards[1].LeaderID != 0
	}, 20*time.Second, 100*time.Millisecond)

	shards := nm.Shards()
	require.Equal(t, uint64(202), nm.MetadataGroupID())
	require.Equal(t, uint64(1), nm.ShardMapEpoch())
	members, err := nm.Members()
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, uint64(1), members[0].NodeID)
	require.Equal(t, uint32(0), shards[0].ID)
	require.Equal(t, uint64(200), shards[0].GroupID)
	require.Equal(t, uint32(1), shards[1].ID)
	require.Equal(t, uint64(201), shards[1].GroupID)

	var key0, key1, key0b []byte
	for i := 0; i < 500 && (key0 == nil || key1 == nil || key0b == nil); i++ {
		key := []byte(fmt.Sprintf("shard-key-%03d", i))
		switch nm.shardFor(key) {
		case 0:
			if key0 == nil {
				key0 = key
			} else if key0b == nil {
				key0b = key
			}
		case 1:
			if key1 == nil {
				key1 = key
			}
		}
	}
	require.NotNil(t, key0)
	require.NotNil(t, key1)
	require.NotNil(t, key0b)

	require.NoError(t, nm.Put(key0, []byte("v0")))
	require.NoError(t, nm.Put(key1, []byte("v1")))
	for key, want := range map[string][]byte{string(key0): []byte("v0"), string(key1): []byte("v1")} {
		got, err := nm.Get([]byte(key))
		require.NoError(t, err)
		require.True(t, bytes.Equal(want, got))
		leader, err := nm.LeaderOf([]byte(key))
		require.NoError(t, err)
		require.Equal(t, uint64(1), leader)
	}

	// Cross-shard scan is explicitly rejected.
	_, scanErr := nm.Scan(nil, nil)
	require.ErrorIs(t, scanErr, ErrCrossShardScan)
	require.NoError(t, nm.BatchWrite(nil, 0))

	// A batch with keys in the same shard is accepted.
	require.NoError(t, nm.BatchWrite([]Command{
		{Op: OpPut, Key: key0, Value: []byte("b0")},
		{Op: OpPut, Key: key0b, Value: []byte("b0x")},
	}, 1))
	got, err := nm.Get(key0)
	require.NoError(t, err)
	require.Equal(t, []byte("b0"), got)

	// A batch spanning shards is rejected before proposing.
	err = nm.BatchWrite([]Command{
		{Op: OpPut, Key: key0, Value: []byte("x")},
		{Op: OpPut, Key: key1, Value: []byte("y")},
	}, 2)
	require.ErrorIs(t, err, ErrCrossShardBatch)
}

func TestClusterScanSingleShard(t *testing.T) {
	dir := t.TempDir()
	addr := freeClusterAddress(t)
	config := createTestClusterOptions(1, 210, addr, dir)
	options := createTestDBOptions(t, dir)
	nm, err := OpenCluster(config, options)
	require.NoError(t, err)
	defer nm.Close()

	require.Eventually(t, func() bool {
		leader, err := nm.GetLeaderID()
		return err == nil && leader == 1
	}, 20*time.Second, 100*time.Millisecond)

	require.NoError(t, nm.Put([]byte("a"), []byte("1")))
	require.NoError(t, nm.Put([]byte("b"), []byte("2")))
	it, err := nm.Scan([]byte("a"), []byte("z"))
	require.NoError(t, err)
	defer it.Close()

	got := make(map[string]string)
	for it.Valid() {
		got[string(it.Key())] = string(it.Value())
		it.Next()
	}
	require.Equal(t, "1", got["a"])
	require.Equal(t, "2", got["b"])
}

func TestClusterShardObjectTierSnapshot(t *testing.T) {
	dir := t.TempDir()
	addr := freeClusterAddress(t)
	config := createTestClusterOptions(1, 220, addr, dir)
	config.ShardCount = 2
	options := createTestDBOptions(t, dir)
	fake := newFakeObjectStore()
	options.ObjectTier = ObjectTierOptions{
		Enabled:        true,
		Provider:       "s3",
		Bucket:         "bucket",
		Prefix:         "cluster-test",
		UploadInterval: time.Hour,
		Store:          fake,
	}
	nm, err := OpenCluster(config, options)
	require.NoError(t, err)
	defer nm.Close()
	require.Eventually(t, func() bool {
		shards := nm.Shards()
		return len(shards) == 2 && shards[0].LeaderID != 0 && shards[1].LeaderID != 0
	}, 20*time.Second, 100*time.Millisecond)

	require.NoError(t, nm.Put([]byte("object-a"), []byte("1")))
	require.NoError(t, nm.Put([]byte("object-b"), []byte("2")))
	for _, sh := range nm.shards {
		require.NotNil(t, sh.sm)
		require.NoError(t, sh.sm.SaveSnapshot(io.Discard, nil, nil))
	}
	keys := fake.keys()
	var shard0Snap, shard1Snap bool
	for _, key := range keys {
		if strings.Contains(key, "cluster-test/shard-0/snapshots/") {
			shard0Snap = true
		}
		if strings.Contains(key, "cluster-test/shard-1/snapshots/") {
			shard1Snap = true
		}
	}
	require.True(t, shard0Snap, "keys: %v", keys)
	require.True(t, shard1Snap, "keys: %v", keys)
}
