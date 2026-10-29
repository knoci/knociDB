package knocidb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupTestEnvironment(t *testing.T) (string, func()) {
	tempDir, err := os.MkdirTemp("", "cluster-test-")
	require.NoError(t, err)

	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	return tempDir, cleanup
}

func createTestDB(t *testing.T, dirPath string) *DB {
	options := DefaultOptions
	options.DirPath = dirPath
	options.ClusterPath = filepath.Join(dirPath, "raft")
	options.SnapshotPath = filepath.Join(dirPath, "snapshot")

	err := os.MkdirAll(options.DirPath, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(options.ClusterPath, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(options.SnapshotPath, os.ModePerm)
	require.NoError(t, err)
	db, err := Open(options)
	return db
}

func createTestDBOptions(t *testing.T, dirPath string) Options {
	options := DefaultOptions
	options.DirPath = dirPath
	options.ClusterPath = filepath.Join(dirPath, "raft")
	options.SnapshotPath = filepath.Join(dirPath, "snapshot")

	err := os.MkdirAll(options.DirPath, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(options.ClusterPath, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(options.SnapshotPath, os.ModePerm)
	require.NoError(t, err)

	return options
}

func createTestClusterOptions(nodeID, clusterID uint64, raftAddress, dataDir string) ClusterOptions {
	config := DefaultClusterOptions(nodeID, clusterID, raftAddress, dataDir)
	config.TickMs = 50
	config.ElectionRTTMs = 5
	config.HeartbeatRTTMs = 1
	config.SnapshotIntervalSeconds = 10

	config.InitialMembers = map[uint64]string{
		nodeID: raftAddress,
	}

	return config
}

func TestValidateClusterOptions(t *testing.T) {
	t.Run("Valid config", func(t *testing.T) {
		config := DefaultClusterOptions(1, 1, "localhost:10000", "/tmp/raft")
		err := ValidateClusterOptions(&config)
		assert.NoError(t, err)
		assert.Equal(t, 5*time.Second, config.RPCTimeout)
		assert.Equal(t, "localhost:10000", config.InitialMembers[1])
		_, hasZero := config.InitialMembers[0]
		assert.False(t, hasZero)
	})

	t.Run("Invalid NodeID", func(t *testing.T) {
		config := DefaultClusterOptions(0, 1, "localhost:10000", "/tmp/raft")
		err := ValidateClusterOptions(&config)
		assert.Error(t, err)
	})

	t.Run("Invalid ClusterID", func(t *testing.T) {
		config := DefaultClusterOptions(1, 0, "localhost:10000", "/tmp/raft")
		err := ValidateClusterOptions(&config)
		assert.Error(t, err)
	})

	t.Run("Empty RaftAddress", func(t *testing.T) {
		config := DefaultClusterOptions(1, 1, "", "/tmp/raft")
		err := ValidateClusterOptions(&config)
		assert.Error(t, err)
	})

	t.Run("Empty DataDir", func(t *testing.T) {
		config := DefaultClusterOptions(1, 1, "localhost:10000", "")
		err := ValidateClusterOptions(&config)
		assert.Error(t, err)
	})

	t.Run("No InitialMembers", func(t *testing.T) {
		config := DefaultClusterOptions(1, 1, "localhost:10000", "/tmp/raft")
		config.InitialMembers = map[uint64]string{}
		err := ValidateClusterOptions(&config)
		assert.Error(t, err)
	})
}

func TestKVStateMachine(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)

	db := createTestDB(t, tempDir)
	defer cleanup()

	sm := NewKVStateMachine(1, 1, db)
	assert.NotNil(t, sm)

	t.Run("Lookup", func(t *testing.T) {

		err := db.Put([]byte("test-key"), []byte("test-value"))
		require.NoError(t, err)

		cmd := Command{
			Op:    OpPut,
			Key:   []byte("test-key"),
			Value: nil,
		}
		data, err := json.Marshal(cmd)
		require.NoError(t, err)

		result, err := sm.Lookup(data)
		require.NoError(t, err)
		value, ok := result.([]byte)
		assert.True(t, ok)
		assert.Equal(t, []byte("test-value"), value)
	})

	t.Run("GetHash", func(t *testing.T) {
		hash, err := sm.GetHash()
		require.NoError(t, err)
		assert.Equal(t, uint64(0), hash) // 初始状态下应该是0
	})

	t.Run("Close", func(t *testing.T) {
		err := sm.Close()
		require.NoError(t, err)
	})
}

func TestCommandSerialization(t *testing.T) {
	t.Run("Command", func(t *testing.T) {
		cmd := Command{
			Op:    OpPut,
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}

		data, err := json.Marshal(cmd)
		require.NoError(t, err)

		var decodedCmd Command
		err = json.Unmarshal(data, &decodedCmd)
		require.NoError(t, err)

		assert.Equal(t, cmd.Op, decodedCmd.Op)
		assert.Equal(t, cmd.Key, decodedCmd.Key)
		assert.Equal(t, cmd.Value, decodedCmd.Value)
	})

	t.Run("BatchCommand", func(t *testing.T) {
		batchCmd := BatchCommand{
			Op: OpBatch,
			Commands: []Command{
				{
					Op:    OpPut,
					Key:   []byte("key1"),
					Value: []byte("value1"),
				},
				{
					Op:    OpDelete,
					Key:   []byte("key2"),
					Value: nil,
				},
			},
			BatchID: 12345,
		}

		data, err := json.Marshal(batchCmd)
		require.NoError(t, err)

		var decodedBatchCmd BatchCommand
		err = json.Unmarshal(data, &decodedBatchCmd)
		require.NoError(t, err)

		assert.Equal(t, batchCmd.Op, decodedBatchCmd.Op)
		assert.Equal(t, batchCmd.BatchID, decodedBatchCmd.BatchID)
		assert.Equal(t, len(batchCmd.Commands), len(decodedBatchCmd.Commands))
		for i, cmd := range batchCmd.Commands {
			assert.Equal(t, cmd.Op, decodedBatchCmd.Commands[i].Op)
			assert.Equal(t, cmd.Key, decodedBatchCmd.Commands[i].Key)
			assert.Equal(t, cmd.Value, decodedBatchCmd.Commands[i].Value)
		}
	})
}

func TestClusterDB(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	config := createTestClusterOptions(1, 100, "localhost:10000", tempDir)
	option := createTestDBOptions(t, tempDir)

	nm, err := OpenCluster(config, option)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	t.Run("IsLeader", func(t *testing.T) {
		isLeader := nm.IsLeader()
		// 单节点集群应该成为Leader
		assert.True(t, isLeader)
	})

	t.Run("GetLeaderID", func(t *testing.T) {
		leaderID, err := nm.GetLeaderID()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), leaderID)
	})

	t.Run("PutAndGet", func(t *testing.T) {
		err := nm.Put([]byte("raft-key"), []byte("raft-value"))
		require.NoError(t, err)

		value, err := nm.Get([]byte("raft-key"))
		require.NoError(t, err)
		assert.Equal(t, []byte("raft-value"), value)
	})

	t.Run("Delete", func(t *testing.T) {
		err := nm.Put([]byte("delete-key"), []byte("delete-value"))
		require.NoError(t, err)

		err = nm.Delete([]byte("delete-key"))
		require.NoError(t, err)

		_, err = nm.Get([]byte("delete-key"))
		assert.Error(t, err)
	})

	t.Run("GetClusterMembership", func(t *testing.T) {
		members, err := nm.GetClusterMembership()
		require.NoError(t, err)
		assert.Equal(t, 1, len(members))
		assert.Equal(t, "localhost:10000", members[1])
	})

	t.Run("RemoveMissingNode", func(t *testing.T) {
		// Dragonboat 对不存在成员的删除是幂等配置变更。
		require.NoError(t, nm.RemoveNode(99))
		members, err := nm.GetClusterMembership()
		require.NoError(t, err)
		require.Len(t, members, 1)
	})

	t.Run("AsyncOperations", func(t *testing.T) {
		nm.Sync = false
		defer func() { nm.Sync = true }()

		require.NoError(t, nm.Put([]byte("async-key"), []byte("async-value")))
		value, err := nm.Get([]byte("async-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("async-value"), value)
		require.NoError(t, nm.Delete([]byte("async-key")))

		require.NoError(t, nm.BatchWrite([]Command{{
			Op:    OpPut,
			Key:   []byte("async-batch-key"),
			Value: []byte("async-batch-value"),
		}}, 7))
		value, err = nm.Get([]byte("async-batch-key"))
		require.NoError(t, err)
		require.Equal(t, []byte("async-batch-value"), value)

		members, err := nm.GetClusterMembership()
		require.NoError(t, err)
		require.Len(t, members, 1)
	})

	err = nm.Close()
	require.NoError(t, err)
}

func TestMultiNodeCluster(t *testing.T) {
	nodeCount := 3
	nodes := make([]*ClusterDB, nodeCount)
	tempDirs := make([]string, nodeCount)
	cleanups := make([]func(), nodeCount)

	initialMembers := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		nodeID := uint64(i + 1)
		port := 10100 + i
		address := fmt.Sprintf("localhost:%d", port)
		initialMembers[nodeID] = address
	}

	for i := 0; i < nodeCount; i++ {
		tempDir, cleanup := setupTestEnvironment(t)
		tempDirs[i] = tempDir
		cleanups[i] = cleanup

		nodeID := uint64(i + 1)
		port := 10100 + i
		address := fmt.Sprintf("localhost:%d", port)
		config := createTestClusterOptions(nodeID, 300, address, tempDir)
		config.InitialMembers = initialMembers
		option := createTestDBOptions(t, tempDir)

		clusterDB, err := OpenCluster(config, option)
		require.NoError(t, err)
		nodes[i] = clusterDB
	}

	defer func() {
		for i := 0; i < nodeCount; i++ {
			if nodes[i] != nil {
				_ = nodes[i].Close()
			}
			if cleanups[i] != nil {
				cleanups[i]()
			}
		}
	}()

	time.Sleep(5 * time.Second)

	var leaderIndex int = -1
	for i := 0; i < nodeCount; i++ {
		if nodes[i].IsLeader() {
			leaderIndex = i
			break
		}
	}

	require.GreaterOrEqual(t, leaderIndex, 0)
	err := nodes[leaderIndex].Put([]byte("cluster-key"), []byte("cluster-value"))
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	for i := 0; i < nodeCount; i++ {
		t.Run(fmt.Sprintf("Node%d", i+1), func(t *testing.T) {
			value, err := nodes[i].Get([]byte("cluster-key"))
			require.NoError(t, err)
			assert.Equal(t, []byte("cluster-value"), value)
		})
	}
}

func TestClusterLeaderForwarding(t *testing.T) {
	const nodeCount = 3
	nodes := make([]*ClusterDB, nodeCount)
	cleanups := make([]func(), nodeCount)
	initialMembers := make(map[uint64]string)
	peerRPC := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		nodeID := uint64(i + 1)
		initialMembers[nodeID] = fmt.Sprintf("localhost:%d", 10600+i)
		peerRPC[nodeID] = fmt.Sprintf("localhost:%d", 10700+i)
	}

	for i := 0; i < nodeCount; i++ {
		tempDir, cleanup := setupTestEnvironment(t)
		cleanups[i] = cleanup
		nodeID := uint64(i + 1)
		config := createTestClusterOptions(nodeID, 600, fmt.Sprintf("localhost:%d", 10600+i), tempDir)
		config.InitialMembers = initialMembers
		config.RPCAddress = peerRPC[nodeID]
		config.PeerRPC = peerRPC
		config.RPCSecret = "test-secret"
		option := createTestDBOptions(t, tempDir)
		node, err := OpenCluster(config, option)
		require.NoError(t, err)
		nodes[i] = node
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

	time.Sleep(5 * time.Second)
	leaderIndex := -1
	for i, node := range nodes {
		if node.IsLeader() {
			leaderIndex = i
			break
		}
	}
	require.GreaterOrEqual(t, leaderIndex, 0)

	follower := -1
	for i := range nodes {
		if i != leaderIndex {
			follower = i
			break
		}
	}
	require.GreaterOrEqual(t, follower, 0)

	require.NoError(t, nodes[follower].Put([]byte("forward-key"), []byte("forward-value")))
	value, err := nodes[follower].Get([]byte("forward-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("forward-value"), value)
	value, err = nodes[leaderIndex].Get([]byte("forward-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("forward-value"), value)

	_, err = nodes[follower].Get([]byte("missing-forward-key"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	require.NoError(t, nodes[follower].Delete([]byte("forward-key")))
	_, err = nodes[leaderIndex].Get([]byte("forward-key"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	require.NoError(t, nodes[leaderIndex].BatchWrite([]Command{{
		Op:    OpPut,
		Key:   []byte("batch-forward-key"),
		Value: []byte("batch-forward-value"),
	}}, 42))
	value, err = nodes[follower].Get([]byte("batch-forward-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("batch-forward-value"), value)

	require.ErrorIs(t, nodes[follower].AddNode(99, "localhost:19999"), ErrNotLeader)
	require.ErrorIs(t, nodes[follower].RemoveNode(99), ErrNotLeader)

	_, err = nodes[follower].rpcCall(nodes[follower].config.RPCAddress, rpcOpPut, []byte("x"), []byte("y"))
	require.ErrorIs(t, err, ErrNotLeader)
	_, err = nodes[leaderIndex].rpcCall(nodes[leaderIndex].config.RPCAddress, 99, []byte("x"), nil)
	require.Error(t, err)

	conn, err := net.Dial("tcp", nodes[leaderIndex].config.RPCAddress)
	require.NoError(t, err)
	require.NoError(t, writeRPCRequest(conn, []byte("wrong-secret"), rpcOpGet, []byte("x"), nil))
	status, _, err := readRPCResponse(conn)
	require.NoError(t, err)
	require.Equal(t, rpcStatusUnauthorized, status)
	require.NoError(t, conn.Close())

	leaderStats := nodes[leaderIndex].Stats()
	require.Greater(t, leaderStats.RPCRequests, uint64(0))
	require.Greater(t, leaderStats.RPCErrors, uint64(0))
	followerStats := nodes[follower].Stats()
	require.Greater(t, followerStats.ForwardedOps, uint64(0))
	require.Greater(t, followerStats.RPCErrors, uint64(0))
}

type failingRPCWriter struct{}

func (failingRPCWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestClusterRPCCodec(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeRPCRequest(&buf, []byte("token"), rpcOpPut, []byte("key"), []byte("value")))
	token, op, key, value, err := readRPCRequest(&buf)
	require.NoError(t, err)
	require.Equal(t, []byte("token"), token)
	require.Equal(t, rpcOpPut, op)
	require.Equal(t, []byte("key"), key)
	require.Equal(t, []byte("value"), value)

	buf.Reset()
	require.NoError(t, writeRPCResponse(&buf, rpcStatusOK, []byte("payload")))
	status, payload, err := readRPCResponse(&buf)
	require.NoError(t, err)
	require.Equal(t, rpcStatusOK, status)
	require.Equal(t, []byte("payload"), payload)

	require.Equal(t, rpcStatusNotFound, rpcStatusForError(ErrKeyNotFound))
	require.Equal(t, rpcStatusNotLeader, rpcStatusForError(ErrNotLeader))
	require.Equal(t, rpcStatusError, rpcStatusForError(errors.New("boom")))

	require.Error(t, writeRPCRequest(failingRPCWriter{}, nil, rpcOpPut, nil, nil))
	require.Error(t, writeRPCResponse(failingRPCWriter{}, rpcStatusOK, nil))
	require.Error(t, writeRPCRequest(&bytes.Buffer{}, make([]byte, 65536), rpcOpPut, nil, nil))

	badVersion := make([]byte, 12)
	badVersion[0] = 99
	_, _, _, _, err = readRPCRequest(bytes.NewReader(badVersion))
	require.Error(t, err)

	tooLarge := make([]byte, 12)
	tooLarge[0] = clusterRPCVersion
	binary.BigEndian.PutUint32(tooLarge[4:8], uint32(maxRPCValueSize+1))
	_, _, _, _, err = readRPCRequest(bytes.NewReader(tooLarge))
	require.Error(t, err)

	_, _, _, _, err = readRPCRequest(bytes.NewReader([]byte{clusterRPCVersion}))
	require.Error(t, err)

	badResponse := make([]byte, 6)
	badResponse[0] = 99
	_, _, err = readRPCResponse(bytes.NewReader(badResponse))
	require.Error(t, err)

	largeResponse := make([]byte, 6)
	largeResponse[0] = clusterRPCVersion
	binary.BigEndian.PutUint32(largeResponse[2:6], uint32(maxRPCValueSize+1))
	_, _, err = readRPCResponse(bytes.NewReader(largeResponse))
	require.Error(t, err)
}

func TestClusterDBErrorPaths(t *testing.T) {
	empty := &ClusterDB{}
	require.ErrorIs(t, empty.putLocal([]byte("k"), []byte("v"), 0), ErrNodeNotStarted)
	require.ErrorIs(t, empty.deleteLocal([]byte("k"), 0), ErrNodeNotStarted)
	_, err := empty.getLocal([]byte("k"), 0)
	require.ErrorIs(t, err, ErrNodeNotStarted)
	require.ErrorIs(t, empty.BatchWrite(nil, 1), ErrNodeNotStarted)
	_, err = empty.GetClusterMembership()
	require.ErrorIs(t, err, ErrNodeNotStarted)
	require.ErrorIs(t, empty.AddNode(2, "localhost:1"), ErrNodeNotStarted)
	require.ErrorIs(t, empty.AddLearner(2, "localhost:1"), ErrNodeNotStarted)
	require.ErrorIs(t, empty.PromoteLearner(2, "localhost:1"), ErrNodeNotStarted)
	require.ErrorIs(t, empty.RemoveNode(2), ErrNodeNotStarted)
	_, err = empty.GetLeaderID()
	require.ErrorIs(t, err, ErrNodeNotStarted)
	require.False(t, empty.IsLeader())
	require.ErrorIs(t, empty.Close(), ErrClusterClosed)

	badRPC := &ClusterDB{config: ClusterOptions{RPCAddress: "not-an-address"}}
	require.Error(t, badRPC.startRPC())

	opts := ClusterOptions{}
	opts.applyDefaults()
	require.Equal(t, 5*time.Second, opts.RPCTimeout)

	_, err = OpenCluster(ClusterOptions{}, DefaultOptions)
	require.Error(t, err)
}

func TestClusterMembershipErrorPaths(t *testing.T) {
	empty := &ClusterDB{}
	if err := empty.AddNode(1, "127.0.0.1:1"); !errors.Is(err, ErrNodeNotStarted) {
		t.Fatalf("AddNode empty = %v", err)
	}
	if err := empty.RemoveNode(1); !errors.Is(err, ErrNodeNotStarted) {
		t.Fatalf("RemoveNode empty = %v", err)
	}
}

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

func TestShardedClusterForwarding(t *testing.T) {
	const nodes = 3
	raftAddresses := make([]string, nodes)
	rpcAddresses := make([]string, nodes)
	initialMembers := make(map[uint64]string)
	peerRPC := make(map[uint64]string)
	for i := 0; i < nodes; i++ {
		nodeID := uint64(i + 1)
		raftAddresses[i] = freeClusterAddress(t)
		rpcAddresses[i] = freeClusterAddress(t)
		initialMembers[nodeID] = raftAddresses[i]
		peerRPC[nodeID] = rpcAddresses[i]
	}

	cluster := make([]*ClusterDB, nodes)
	for i := 0; i < nodes; i++ {
		dir := t.TempDir()
		nodeID := uint64(i + 1)
		config := createTestClusterOptions(nodeID, 700, raftAddresses[i], dir)
		config.InitialMembers = initialMembers
		config.RPCAddress = peerRPC[nodeID]
		config.PeerRPC = peerRPC
		config.RPCSecret = "test-secret"
		config.ShardCount = 2
		options := createTestDBOptions(t, dir)
		db, err := OpenCluster(config, options)
		require.NoError(t, err)
		cluster[i] = db
	}
	defer func() {
		for _, db := range cluster {
			_ = db.Close()
		}
	}()

	require.Eventually(t, func() bool {
		for _, db := range cluster {
			shards := db.Shards()
			if len(shards) != 2 || shards[0].LeaderID == 0 || shards[1].LeaderID == 0 {
				return false
			}
		}
		return true
	}, 25*time.Second, 100*time.Millisecond)
	time.Sleep(500 * time.Millisecond)

	var key0, key1 []byte
	for i := 0; i < 500 && (key0 == nil || key1 == nil); i++ {
		key := []byte(fmt.Sprintf("forward-shard-%03d", i))
		switch cluster[0].shardFor(key) {
		case 0:
			if key0 == nil {
				key0 = key
			}
		case 1:
			if key1 == nil {
				key1 = key
			}
		}
	}
	require.NotNil(t, key0)
	require.NotNil(t, key1)

	// Pick a node that is not the leader of the target group.
	followerFor := func(key []byte) *ClusterDB {
		leader, err := cluster[0].LeaderOf(key)
		require.NoError(t, err)
		for _, db := range cluster {
			if db.config.NodeID != leader {
				return db
			}
		}
		return cluster[0]
	}
	f0 := followerFor(key0)
	f1 := followerFor(key1)
	require.NoError(t, f0.Put(key0, []byte("v0")))
	require.NoError(t, f1.Put(key1, []byte("v1")))

	for _, db := range cluster {
		got, err := db.Get(key0)
		require.NoError(t, err)
		require.Equal(t, []byte("v0"), got)
		got, err = db.Get(key1)
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), got)
	}

	require.NoError(t, f1.BatchWrite([]Command{{Op: OpPut, Key: key1, Value: []byte("batch-v1")}}, 8))
	batchGot, err := f1.Get(key1)
	require.NoError(t, err)
	require.Equal(t, []byte("batch-v1"), batchGot)

	require.NoError(t, f0.Delete(key0))
	for _, db := range cluster {
		_, err := db.Get(key0)
		require.ErrorIs(t, err, ErrKeyNotFound)
	}

	err = f0.BatchWrite([]Command{
		{Op: OpPut, Key: key0, Value: []byte("x")},
		{Op: OpPut, Key: key1, Value: []byte("y")},
	}, 7)
	require.ErrorIs(t, err, ErrCrossShardBatch)
}

func TestLearnerJoinAndPromote(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	addr1 := freeClusterAddress(t)
	addr2 := freeClusterAddress(t)
	rpc1 := freeClusterAddress(t)
	rpc2 := freeClusterAddress(t)
	peerRPC := map[uint64]string{1: rpc1, 2: rpc2}
	initial := map[uint64]string{1: addr1, 2: addr2}

	makeConfig := func(nodeID uint64, addr, dir string, learner bool) ClusterOptions {
		config := createTestClusterOptions(nodeID, 800, addr, dir)
		config.ShardCount = 2
		if nodeID == 1 {
			config.InitialMembers = map[uint64]string{1: addr1}
		} else {
			config.InitialMembers = initial
		}
		config.RPCAddress = peerRPC[nodeID]
		config.PeerRPC = peerRPC
		config.RPCSecret = "test-secret"
		config.IsLearner = learner
		config.JoinCluster = nodeID != 1
		return config
	}

	node1, err := OpenCluster(makeConfig(1, addr1, dir1, false), createTestDBOptions(t, dir1))
	require.NoError(t, err)
	defer node1.Close()
	require.Eventually(t, func() bool {
		shards := node1.Shards()
		return len(shards) == 2 && shards[0].LeaderID == 1 && shards[1].LeaderID == 1
	}, 25*time.Second, 100*time.Millisecond)

	var key0, key1 []byte
	for i := 0; i < 500 && (key0 == nil || key1 == nil); i++ {
		key := []byte(fmt.Sprintf("learner-key-%03d", i))
		switch node1.shardFor(key) {
		case 0:
			if key0 == nil {
				key0 = key
			}
		case 1:
			if key1 == nil {
				key1 = key
			}
		}
	}
	require.NotNil(t, key0)
	require.NotNil(t, key1)
	require.NoError(t, node1.Put(key0, []byte("v0")))
	require.NoError(t, node1.Put(key1, []byte("v1")))

	node2, err := OpenCluster(makeConfig(2, addr2, dir2, true), createTestDBOptions(t, dir2))
	require.NoError(t, err)
	defer node2.Close()

	require.NoError(t, node1.AddLearner(2, addr2))

	require.Eventually(t, func() bool {
		db0, err := node2.dbForShard(0)
		if err != nil {
			return false
		}
		v, err := db0.Get(key0)
		return err == nil && string(v) == "v0"
	}, 25*time.Second, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	membership, err := node1.nodeHost.SyncGetShardMembership(ctx, 800)
	cancel()
	require.NoError(t, err)
	require.Contains(t, membership.NonVotings, uint64(2))

	require.NoError(t, node1.PromoteLearner(2, addr2))

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m, err := node1.nodeHost.SyncGetShardMembership(ctx, 800)
		return err == nil && m.Nodes[2] == addr2 && len(m.NonVotings) == 0
	}, 25*time.Second, 100*time.Millisecond)
}

func TestShardMapStateMachine(t *testing.T) {
	sm := NewShardMapStateMachine(1, 2)
	require.Equal(t, ShardMapMeta{Epoch: 1, ShardCount: 2}, sm.Meta())

	raw, err := sm.Lookup(nil)
	require.NoError(t, err)
	var meta ShardMapMeta
	require.NoError(t, json.Unmarshal(raw.([]byte), &meta))
	require.Equal(t, ShardMapMeta{Epoch: 1, ShardCount: 2}, meta)

	cmd, err := json.Marshal(ShardMapMeta{Epoch: 2, ShardCount: 3})
	require.NoError(t, err)
	res, err := sm.Update(statemachine.Entry{Cmd: cmd})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res.Value)
	require.Equal(t, ShardMapMeta{Epoch: 2, ShardCount: 3}, sm.Meta())

	// Stale and invalid updates are rejected.
	stale, _ := json.Marshal(ShardMapMeta{Epoch: 1, ShardCount: 3})
	_, err = sm.Update(statemachine.Entry{Cmd: stale})
	require.Error(t, err)
	_, err = sm.Update(statemachine.Entry{Cmd: []byte("{bad")})
	require.Error(t, err)
	badCount, _ := json.Marshal(ShardMapMeta{Epoch: 3, ShardCount: 0})
	_, err = sm.Update(statemachine.Entry{Cmd: badCount})
	require.Error(t, err)

	var buf bytes.Buffer
	require.NoError(t, sm.SaveSnapshot(&buf, nil, make(chan struct{})))
	restored := NewShardMapStateMachine(1, 1)
	require.NoError(t, restored.RecoverFromSnapshot(&buf, nil, make(chan struct{})))
	require.Equal(t, ShardMapMeta{Epoch: 2, ShardCount: 3}, restored.Meta())

	closed := make(chan struct{})
	close(closed)
	require.ErrorIs(t, sm.SaveSnapshot(&buf, nil, closed), statemachine.ErrSnapshotStopped)
	require.ErrorIs(t, restored.RecoverFromSnapshot(&buf, nil, closed), statemachine.ErrSnapshotStopped)

	hash, err := sm.GetHash()
	require.NoError(t, err)
	require.NotZero(t, hash)
	require.NoError(t, sm.Close())
}

func TestMigrateShardToLearner(t *testing.T) {
	const voting = 3
	const clusterID = 900
	addrs := make([]string, 4)
	rpcs := make([]string, 4)
	initial := make(map[uint64]string)
	peerRPC := make(map[uint64]string)
	for i := 0; i < 4; i++ {
		addrs[i] = freeClusterAddress(t)
		rpcs[i] = freeClusterAddress(t)
	}
	for i := 0; i < voting; i++ {
		initial[uint64(i+1)] = addrs[i]
	}
	for i := 0; i < 4; i++ {
		peerRPC[uint64(i+1)] = rpcs[i]
	}

	cluster := make([]*ClusterDB, 4)
	for i := 0; i < voting; i++ {
		dir := t.TempDir()
		config := createTestClusterOptions(uint64(i+1), clusterID, addrs[i], dir)
		config.InitialMembers = initial
		config.ShardCount = 2
		config.RPCAddress = rpcs[i]
		config.PeerRPC = peerRPC
		config.RPCSecret = "test-secret"
		db, err := OpenCluster(config, createTestDBOptions(t, dir))
		require.NoError(t, err)
		cluster[i] = db
	}
	defer func() {
		for _, db := range cluster {
			if db != nil {
				_ = db.Close()
			}
		}
	}()
	var leaderNode *ClusterDB
	require.Eventually(t, func() bool {
		for _, db := range cluster[:voting] {
			if db == nil {
				continue
			}
			leader, err := db.leaderForShard(0)
			if err == nil && leader == db.config.NodeID {
				leaderNode = db
				return true
			}
		}
		return false
	}, 25*time.Second, 100*time.Millisecond)
	require.NotNil(t, leaderNode)

	var key []byte
	for i := 0; i < 500; i++ {
		candidate := []byte(fmt.Sprintf("migrate-key-%03d", i))
		if cluster[0].shardFor(candidate) == 0 {
			key = candidate
			break
		}
	}
	require.NotNil(t, key)
	require.NoError(t, cluster[0].Put(key, []byte("before-migration")))

	// Join a fourth node as a learner in every group.
	dir4 := t.TempDir()
	config4 := createTestClusterOptions(4, clusterID, addrs[3], dir4)
	config4.JoinCluster = true
	config4.IsLearner = true
	config4.InitialMembers = initial
	config4.ShardCount = 2
	config4.RPCAddress = rpcs[3]
	config4.PeerRPC = peerRPC
	config4.RPCSecret = "test-secret"
	node4, err := OpenCluster(config4, createTestDBOptions(t, dir4))
	require.NoError(t, err)
	cluster[3] = node4

	require.NoError(t, leaderNode.AddShardLearner(0, 4, addrs[3]))
	require.Eventually(t, func() bool {
		db, err := node4.dbForShard(0)
		if err != nil {
			return false
		}
		v, err := db.Get(key)
		return err == nil && string(v) == "before-migration"
	}, 25*time.Second, 100*time.Millisecond)

	require.NoError(t, leaderNode.MigrateShard(0, 3, 4, addrs[3]))

	m, err := leaderNode.ShardMembership(0)
	require.NoError(t, err)
	require.Contains(t, m.Voters, uint64(4))
	require.NotContains(t, m.Voters, uint64(3))
	require.Eventually(t, func() bool {
		return leaderNode.ShardMapEpoch() == 2
	}, 10*time.Second, 100*time.Millisecond)

	db4, err := node4.dbForShard(0)
	require.NoError(t, err)
	v, err := db4.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("before-migration"), v)

	moves, err := leaderNode.PlanRebalance()
	require.NoError(t, err)
	require.NotEmpty(t, moves)
	var execNode *ClusterDB
	for _, db := range cluster[:voting] {
		leader, lerr := db.leaderForShard(moves[0].ShardID)
		if lerr == nil && leader == db.config.NodeID {
			execNode = db
			break
		}
	}
	require.NotNil(t, execNode)
	require.NoError(t, execNode.ExecuteShardMove(moves[0]))

	// The migrated shard must still serve reads through the new voter.
	got, err := node4.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("before-migration"), got)
}

func TestMigrateShardErrorPaths(t *testing.T) {
	var empty ClusterDB
	require.ErrorIs(t, empty.MigrateShard(0, 1, 2, "x"), ErrNodeNotStarted)
	require.ErrorIs(t, empty.AddShardLearner(0, 2, "x"), ErrNodeNotStarted)
	require.ErrorIs(t, empty.RemoveShardReplica(0, 2), ErrNodeNotStarted)
	_, err := empty.ShardMembership(0)
	require.ErrorIs(t, err, ErrNodeNotStarted)
	_, err = empty.Rebalance()
	require.Error(t, err)
	require.Error(t, empty.ExecuteShardMove(ShardMove{ShardID: 0, FromNode: 1, ToNode: 2}))
	_, err = empty.RebalanceAll(2)
	require.Error(t, err)
}
