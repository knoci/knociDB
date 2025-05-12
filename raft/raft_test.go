package raft

import (
	"encoding/json"
	"fmt"
	"knocidb"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestEnvironment(t *testing.T) (string, func()) {
	tempDir, err := os.MkdirTemp("", "raft-test-")
	require.NoError(t, err)

	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	return tempDir, cleanup
}

func createTestDB(t *testing.T, dirPath string) *knocidb.DB {
	options := knocidb.DefaultOptions
	options.DirPath = dirPath
	options.RaftPath = filepath.Join(dirPath, "raft")
	options.SnapshotPath = filepath.Join(dirPath, "snapshot")

	err := os.MkdirAll(options.DirPath, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(options.RaftPath, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(options.SnapshotPath, os.ModePerm)
	require.NoError(t, err)

	db, err := knocidb.Open(options)
	require.NoError(t, err)

	return db
}

func createTestRaftConfig(nodeID, clusterID uint64, raftAddress, dataDir string) Config {
	config := DefaultConfig(nodeID, clusterID, raftAddress, dataDir)
	config.TickMs = 50
	config.ElectionRTTMs = 5
	config.HeartbeatRTTMs = 1
	config.SnapshotIntervalSeconds = 10

	config.InitialMembers = map[uint64]string{
		nodeID: raftAddress,
	}

	return config
}

func TestValidateConfig(t *testing.T) {
	t.Run("Valid config", func(t *testing.T) {
		config := DefaultConfig(1, 1, "localhost:10000", "/tmp/raft")
		err := ValidateConfig(&config)
		assert.NoError(t, err)
	})

	t.Run("Invalid NodeID", func(t *testing.T) {
		config := DefaultConfig(0, 1, "localhost:10000", "/tmp/raft")
		err := ValidateConfig(&config)
		assert.Error(t, err)
	})

	t.Run("Invalid ClusterID", func(t *testing.T) {
		config := DefaultConfig(1, 0, "localhost:10000", "/tmp/raft")
		err := ValidateConfig(&config)
		assert.Error(t, err)
	})

	t.Run("Empty RaftAddress", func(t *testing.T) {
		config := DefaultConfig(1, 1, "", "/tmp/raft")
		err := ValidateConfig(&config)
		assert.Error(t, err)
	})

	t.Run("Empty DataDir", func(t *testing.T) {
		config := DefaultConfig(1, 1, "localhost:10000", "")
		err := ValidateConfig(&config)
		assert.Error(t, err)
	})

	t.Run("No InitialMembers", func(t *testing.T) {
		config := DefaultConfig(1, 1, "localhost:10000", "/tmp/raft")
		config.InitialMembers = map[uint64]string{}
		config.JoinCluster = true
		err := ValidateConfig(&config)
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

func TestNodeManager(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)

	db := createTestDB(t, tempDir)
	defer cleanup()

	config := createTestRaftConfig(1, 100, "localhost:10000", tempDir)

	nm, err := NewNodeManager(config, db)
	require.NoError(t, err)

	err = nm.Start()
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

	err = nm.Stop()
	require.NoError(t, err)
}

func TestRaftDB(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	db := createTestDB(t, tempDir)
	defer cleanup()

	config := createTestRaftConfig(1, 200, "localhost:10001", tempDir)

	raftDB, err := OpenRaftDB(db, config)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	t.Run("IsLeader", func(t *testing.T) {
		isLeader := raftDB.IsLeader()
		// 单节点集群应该成为Leader
		assert.True(t, isLeader)
	})

	t.Run("PutAndGet", func(t *testing.T) {
		err := raftDB.Put([]byte("raftdb-key"), []byte("raftdb-value"))
		require.NoError(t, err)

		value, err := raftDB.Get([]byte("raftdb-key"))
		require.NoError(t, err)
		assert.Equal(t, []byte("raftdb-value"), value)
	})

	t.Run("Delete", func(t *testing.T) {
		err := raftDB.Put([]byte("raftdb-delete-key"), []byte("raftdb-delete-value"))
		require.NoError(t, err)

		err = raftDB.Delete([]byte("raftdb-delete-key"))
		require.NoError(t, err)

		_, err = raftDB.Get([]byte("raftdb-delete-key"))
		assert.Error(t, err)
	})

	err = raftDB.Close()
	require.NoError(t, err)
}

func TestMultiNodeRaftCluster(t *testing.T) {
	nodeCount := 3
	nodes := make([]*RaftDB, nodeCount)
	dbs := make([]*knocidb.DB, nodeCount)
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

		db := createTestDB(t, tempDir)
		dbs[i] = db

		nodeID := uint64(i + 1)
		port := 10100 + i
		address := fmt.Sprintf("localhost:%d", port)
		config := createTestRaftConfig(nodeID, 300, address, tempDir)
		config.InitialMembers = initialMembers

		raftDB, err := OpenRaftDB(db, config)
		require.NoError(t, err)
		nodes[i] = raftDB
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
