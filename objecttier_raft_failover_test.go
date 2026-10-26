package knocidb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMultiNodeRaftObjectTierFailover(t *testing.T) {
	const nodeCount = 5
	nodes := make([]*RaftDB, nodeCount)
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
		config := createTestRaftConfig(nodeID, 500, fmt.Sprintf("localhost:%d", 10400+i), tempDir)
		config.InitialMembers = initialMembers
		config.SnapshotIntervalSeconds = 1
		options := createTestDBOptions(t, tempDir)
		options.ObjectTier = ObjectTierOptions{
			Enabled: true,
			Store:   stores[i],
			Prefix:  fmt.Sprintf("raft-node-%d", nodeID),
		}
		rdb, err := OpenRaft(config, options)
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

	// 杀一个 Follower，Leader 仍应可写。
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
	require.NoError(t, nodes[newLeader].Put([]byte("failover-key-3"), []byte("v3")))
	value, err := nodes[newLeader].Get([]byte("failover-key-3"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), value)
}
