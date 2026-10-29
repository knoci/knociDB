package knocidb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
