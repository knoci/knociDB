package knocidb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// MigrateShard 把一个固定分片的 voting 副本迁移到另一个节点。
// 目标节点必须已经以 Raft 节点身份可达（通常以 JoinCluster=true 启动）。
// 只有目标 promote 成功后才移除源副本；失败时尝试恢复原成员关系。
func (c *ClusterDB) MigrateShard(shardID uint32, fromNode, toNode uint64, toAddress string) error {
	if fromNode == toNode {
		return fmt.Errorf("source and target replica are identical")
	}
	if _, err := c.requireShardLeader(shardID); err != nil {
		return err
	}
	before, err := c.ShardMembership(shardID)
	if err != nil {
		return err
	}
	if !containsID(before.Voters, fromNode) {
		return fmt.Errorf("source replica %d is not a voter in shard %d", fromNode, shardID)
	}
	if containsID(before.Voters, toNode) {
		return fmt.Errorf("target replica %d is already a voter in shard %d", toNode, shardID)
	}

	addedLearner := !containsID(before.NonVoters, toNode)
	if addedLearner {
		if err := c.AddShardLearner(shardID, toNode, toAddress); err != nil {
			return fmt.Errorf("add learner: %w", err)
		}
	}
	if err := c.waitForNonVoter(shardID, toNode, 10*time.Second); err != nil {
		if addedLearner {
			_ = c.RemoveShardReplica(shardID, toNode)
		}
		return fmt.Errorf("wait learner: %w", err)
	}

	if err := c.PromoteShardLearner(shardID, toNode, toAddress); err != nil {
		if addedLearner {
			_ = c.RemoveShardReplica(shardID, toNode)
		}
		return fmt.Errorf("promote learner: %w", err)
	}
	if err := c.RemoveShardReplica(shardID, fromNode); err != nil {
		// 如果源仍是 voter，移除新目标以回滚。
		// 如果移除已经生效，Dragonboat 会把源放入 Removed 集合，此时不能重新加回。
		if after, mErr := c.ShardMembership(shardID); mErr == nil && containsID(after.Voters, fromNode) {
			_ = c.RemoveShardReplica(shardID, toNode)
		}
		return fmt.Errorf("remove source replica: %w", err)
	}
	if err := c.advanceShardMapEpoch(); err != nil {
		return fmt.Errorf("advance shard map epoch: %w", err)
	}
	return nil
}

func (c *ClusterDB) waitForNonVoter(shardID uint32, nodeID uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m, err := c.ShardMembership(shardID)
		if err == nil && containsID(m.NonVoters, nodeID) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("replica %d did not become a non-voter in shard %d", nodeID, shardID)
}

func (c *ClusterDB) advanceShardMapEpoch() error {
	mid := c.config.MetadataGroupID()
	if mid == 0 {
		return nil
	}
	c.mu.RLock()
	nodeHost := c.nodeHost
	started := c.state && nodeHost != nil
	c.mu.RUnlock()
	if !started {
		return ErrNodeNotStarted
	}
	leader, _, valid, err := nodeHost.GetLeaderID(mid)
	if err != nil {
		return err
	}
	if !valid {
		return ErrNotLeader
	}
	epoch := c.ShardMapEpoch() + 1
	meta := ShardMapMeta{Epoch: epoch, ShardCount: c.config.ShardCount}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if leader == c.config.NodeID {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = nodeHost.SyncPropose(ctx, nodeHost.GetNoOPSession(mid), data)
		return err
	}
	c.mu.RLock()
	addr := c.config.PeerRPC[leader]
	c.mu.RUnlock()
	if addr == "" {
		return fmt.Errorf("metadata leader %d has no rpc address", leader)
	}
	return c.forwardRequest(addr, rpcOpMeta, nil, data)
}

func containsID(ids []uint64, id uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
