package knocidb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lni/dragonboat/v4"
)

// MigrateShard moves a voting replica to a new node. Retry with the same
// arguments on the current shard leader after a timeout or leadership change.
func (c *ClusterDB) MigrateShard(shardID uint32, fromNode, toNode uint64, toAddress string) error {
	if fromNode == toNode {
		return fmt.Errorf("source and target replica are identical")
	}
	if _, err := c.requireShardLeader(shardID); err != nil {
		return err
	}
	move := ShardMove{ShardID: shardID, FromNode: fromNode, ToNode: toNode, ToAddress: toAddress}
	change := migrationChange{Move: move}
	if !change.valid(c.config.ShardCount) {
		return ErrInvalidRaftCommand
	}
	members, err := c.ShardMembership(shardID)
	if err != nil {
		return err
	}
	_, targetRemoved := members.Removed[toNode]
	address := members.Addresses[toNode]
	change.CanStart = containsID(members.Voters, fromNode) &&
		!containsID(members.Voters, toNode) && !targetRemoved &&
		(address == "" || address == toAddress)
	if change.CanStart {
		c.mu.RLock()
		progressAddress := c.config.PeerRPC[toNode]
		c.mu.RUnlock()
		if progressAddress == "" {
			return fmt.Errorf("replica %d has no rpc address for migration progress", toNode)
		}
	}
	state, err := c.submitMigration(change)
	if err != nil {
		return fmt.Errorf("record shard migration: %w", err)
	}
	if state == migrationDone {
		return nil
	}

	members, err = c.ShardMembership(shardID)
	if err != nil {
		return err
	}
	if _, removed := members.Removed[fromNode]; removed {
		if !containsID(members.Voters, toNode) || members.Addresses[toNode] != toAddress {
			return ErrShardMigrationConflict
		}
		return c.finishMigration(move)
	}
	if !containsID(members.Voters, fromNode) {
		return ErrShardMigrationConflict
	}
	if _, removed := members.Removed[toNode]; removed {
		return ErrShardMigrationConflict
	}
	if addr := members.Addresses[toNode]; addr != "" && addr != toAddress {
		return ErrShardMigrationConflict
	}

	if !containsID(members.Voters, toNode) {
		if !containsID(members.NonVoters, toNode) {
			if err := c.changeShardMembershipAt(members, toNode, toAddress, true, true); err != nil {
				return fmt.Errorf("add learner: %w", err)
			}
		}
		timeout := c.config.MigrationTimeout
		if timeout <= 0 {
			timeout = 60 * time.Second
		}
		if err := c.waitForLearnerApplied(shardID, toNode, timeout); err != nil {
			return fmt.Errorf("wait learner applied: %w", err)
		}
		members, err = c.ShardMembership(shardID)
		if err != nil {
			return err
		}
		if !containsID(members.Voters, fromNode) || members.Addresses[toNode] != toAddress {
			return ErrShardMigrationConflict
		}
		if containsID(members.NonVoters, toNode) {
			if err := c.changeShardMembershipAt(members, toNode, toAddress, true, false); err != nil {
				return fmt.Errorf("promote learner: %w", err)
			}
			members, err = c.ShardMembership(shardID)
			if err != nil {
				return err
			}
		}
	}

	if !containsID(members.Voters, toNode) || members.Addresses[toNode] != toAddress {
		return ErrShardMigrationConflict
	}
	if _, removed := members.Removed[fromNode]; removed {
		return c.finishMigration(move)
	}
	if !containsID(members.Voters, fromNode) {
		return ErrShardMigrationConflict
	}
	if c.config.NodeID == fromNode {
		if err := c.transferMigrationLeadership(shardID, toNode); err != nil {
			return err
		}
		return fmt.Errorf("leadership transfer requested for shard %d, retry on its new leader: %w", shardID, ErrNotLeader)
	}
	if err := c.changeShardMembershipAt(members, fromNode, "", false, false); err != nil {
		return fmt.Errorf("remove source replica: %w", err)
	}
	return c.finishMigration(move)
}

func (c *ClusterDB) transferMigrationLeadership(shardID uint32, target uint64) error {
	if _, err := c.requireShardLeader(shardID); err != nil {
		return err
	}
	c.mu.RLock()
	nodeHost := c.nodeHost
	c.mu.RUnlock()
	if nodeHost == nil {
		return ErrNodeNotStarted
	}
	if err := nodeHost.RequestLeaderTransfer(c.config.GroupIDForShard(shardID), target); err != nil {
		return fmt.Errorf("transfer shard %d leadership: %w", shardID, err)
	}
	return nil
}

// AbortShardMigration 中止一条未完成的分片迁移并清理可能遗留的孤儿 Learner，
// 防止目标节点故障导致分片永久卡死在 Pending 状态。
func (c *ClusterDB) AbortShardMigration(shardID uint32, fromNode, toNode uint64, toAddress string) error {
	if _, err := c.requireShardLeader(shardID); err != nil {
		return err
	}
	move := ShardMove{ShardID: shardID, FromNode: fromNode, ToNode: toNode, ToAddress: toAddress}
	change := migrationChange{Move: move, Abort: true}
	if !change.valid(c.config.ShardCount) {
		return ErrInvalidRaftCommand
	}
	members, err := c.ShardMembership(shardID)
	if err != nil {
		return err
	}
	// 如果目标节点已晋升为 Voter，说明已越过不可逆点，应继续重试 MigrateShard 而非回滚。
	if containsID(members.Voters, toNode) {
		return ErrShardMigrationConflict
	}
	if containsID(members.NonVoters, toNode) {
		if err := c.changeShardMembershipAt(members, toNode, "", false, false); err != nil {
			return fmt.Errorf("remove orphan learner %d: %w", toNode, err)
		}
	}
	_, err = c.submitMigration(change)
	if err != nil {
		return fmt.Errorf("abort shard migration: %w", err)
	}
	return nil
}

func (c *ClusterDB) finishMigration(move ShardMove) error {
	_, err := c.submitMigration(migrationChange{Move: move, Complete: true})
	if err != nil {
		return fmt.Errorf("finalize shard migration: %w", err)
	}
	return nil
}

func (c *ClusterDB) submitMigration(change migrationChange) (uint64, error) {
	c.mu.RLock()
	nodeHost := c.nodeHost
	started := c.state.Load() && nodeHost != nil
	c.mu.RUnlock()
	if !started {
		return 0, ErrNodeNotStarted
	}
	groupID := c.config.MetadataGroupID()
	if groupID == 0 {
		data, err := json.Marshal(raftWriteCommand{Command: Command{Op: opShardMigration}, Migration: &change})
		if err != nil {
			return 0, err
		}
		return syncMigrationProposal(nodeHost, c.config.GroupIDForShard(change.Move.ShardID), data)
	}

	data, err := json.Marshal(shardMapUpdate{
		ShardMapMeta: ShardMapMeta{ShardCount: c.config.ShardCount},
		Migration:    &change,
	})
	if err != nil {
		return 0, err
	}
	leader, _, known, _ := nodeHost.GetLeaderID(groupID)
	if known && leader == c.config.NodeID {
		return syncMigrationProposal(nodeHost, groupID, data)
	}

	// A new data replica may not be a member of the metadata group.
	// Try the known leader first, then the remaining peers if it has changed.
	c.mu.RLock()
	peers := make([]uint64, 0, len(c.config.PeerRPC))
	for id := range c.config.PeerRPC {
		if id != c.config.NodeID && id != leader {
			peers = append(peers, id)
		}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	addresses := make([]string, 0, len(peers)+1)
	if addr := c.config.PeerRPC[leader]; addr != "" {
		addresses = append(addresses, addr)
	}
	for _, id := range peers {
		if addr := c.config.PeerRPC[id]; addr != "" {
			addresses = append(addresses, addr)
		}
	}
	c.mu.RUnlock()

	var lastErr error = ErrNotLeader
	for _, addr := range addresses {
		payload, err := c.rpcCall(addr, rpcOpMeta, nil, data)
		if err == nil {
			if len(payload) != 8 {
				return 0, fmt.Errorf("invalid migration result")
			}
			return migrationResult(binary.BigEndian.Uint64(payload))
		}
		if errors.Is(err, ErrShardMigrationConflict) || errors.Is(err, ErrInvalidRaftCommand) || errors.Is(err, ErrClusterUnauthorized) {
			return 0, err
		}
		lastErr = err
	}
	return 0, lastErr
}

func syncMigrationProposal(nodeHost *dragonboat.NodeHost, groupID uint64, data []byte) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := nodeHost.SyncPropose(ctx, nodeHost.GetNoOPSession(groupID), data)
	if err != nil {
		return 0, err
	}
	if err := raftResultError(result); err != nil {
		return 0, err
	}
	return migrationResult(result.Value)
}

func migrationResult(value uint64) (uint64, error) {
	if value != migrationPending && value != migrationDone && value != migrationAborted {
		return 0, fmt.Errorf("invalid migration result")
	}
	return value, nil
}

func containsID(ids []uint64, id uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
