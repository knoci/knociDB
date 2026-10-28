package knocidb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.ErrorIs(t, empty.putLocal([]byte("k"), []byte("v")), ErrNodeNotStarted)
	require.ErrorIs(t, empty.deleteLocal([]byte("k")), ErrNodeNotStarted)
	_, err := empty.getLocal([]byte("k"))
	require.ErrorIs(t, err, ErrNodeNotStarted)
	require.ErrorIs(t, empty.BatchWrite(nil, 1), ErrNodeNotStarted)
	_, err = empty.GetClusterMembership()
	require.ErrorIs(t, err, ErrNodeNotStarted)
	require.ErrorIs(t, empty.AddNode(2, "localhost:1"), ErrNodeNotStarted)
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
