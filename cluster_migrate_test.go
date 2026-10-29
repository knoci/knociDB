package knocidb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
