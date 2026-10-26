package knocidb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThreeNodeRaftObjectTierLeaderFailover(t *testing.T) {
	const nodeCount = 3
	nodes := make([]*RaftDB, nodeCount)
	cleanups := make([]func(), nodeCount)
	initialMembers := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		initialMembers[uint64(i+1)] = fmt.Sprintf("localhost:%d", 10500+i)
	}

	for i := 0; i < nodeCount; i++ {
		tempDir, cleanup := setupTestEnvironment(t)
		cleanups[i] = cleanup
		config := createTestRaftConfig(uint64(i+1), 501, fmt.Sprintf("localhost:%d", 10500+i), tempDir)
		config.InitialMembers = initialMembers
		options := createTestDBOptions(t, tempDir)
		options.ObjectTier = ObjectTierOptions{
			Enabled: true,
			Store:   newFakeObjectStore(),
			Prefix:  fmt.Sprintf("three-node-%d", i+1),
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
