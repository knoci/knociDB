package knocidb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
