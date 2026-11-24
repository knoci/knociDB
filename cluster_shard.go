package knocidb

import (
	"bytes"
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/lni/dragonboat/v4"
)

type clusterShard struct {
	id       uint32
	groupID  uint64
	sm       *KVStateMachine
	leaderID atomic.Uint64
}

// ShardInfo 描述 ClusterDB 中的一个固定 Raft group。
type ShardInfo struct {
	ID       uint32
	GroupID  uint64
	LeaderID uint64
}

// GroupIDForShard 把固定分片索引映射为 Dragonboat group ID。
// shard 0 保留 ClusterID 以兼容单分片模式，后续分片使用 ClusterID+i。
func (c *ClusterOptions) GroupIDForShard(shard uint32) uint64 {
	return c.ClusterID + uint64(shard)
}

// MetadataGroupID 返回分片元数据 group ID；0 表示没有独立 metadata group（单分片布局）。
func (c *ClusterOptions) MetadataGroupID() uint64 {
	if c.ShardCount <= 1 {
		return 0
	}
	return c.ClusterID + uint64(c.ShardCount)
}

func (c *ClusterDB) MetadataGroupID() uint64 {
	return c.config.MetadataGroupID()
}

func (c *ClusterDB) ShardMapEpoch() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.metadataSM != nil {
		return c.metadataSM.Meta().Epoch
	}
	return c.config.ShardMapEpoch
}

func (c *ClusterDB) shardFor(key []byte) uint32 {
	count := c.config.ShardCount
	if count <= 1 {
		return 0
	}
	return uint32(xxhash.Sum64(key) % uint64(count))
}

func (c *ClusterDB) leaderForShard(shard uint32) (uint64, error) {
	c.mu.RLock()
	nodeHost := c.nodeHost
	started := c.state.Load() && nodeHost != nil
	var sh *clusterShard
	if int(shard) < len(c.shards) {
		sh = c.shards[shard]
	}
	groupID := c.config.GroupIDForShard(shard)
	c.mu.RUnlock()
	if !started {
		return 0, ErrNodeNotStarted
	}

	leader, _, valid, err := nodeHost.GetLeaderID(groupID)
	if err != nil {
		return 0, err
	}
	if !valid {
		return 0, ErrNotLeader
	}
	// 无锁更新每分片 Leader 缓存，消除热路径全局写锁争用。
	if sh != nil {
		sh.leaderID.Store(leader)
	}
	if shard == 0 {
		c.leaderID.Store(leader)
	}
	return leader, nil
}

func (c *ClusterDB) stateMachineForShard(shard uint32) (*KVStateMachine, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.nodeHost == nil || !c.state.Load() || int(shard) >= len(c.shards) {
		return nil, ErrNodeNotStarted
	}
	return c.shards[shard].sm, nil
}

// ReadConsistency 控制分布式读取的一致性级别。
type ReadConsistency uint8

const (
	// ReadLinearizable 默认强一致读：通过 Leader ReadIndex 屏障确保读到最新已提交状态。
	ReadLinearizable ReadConsistency = iota
	// ReadFollowerIndex 在本地节点（含 Follower）通过 ReadIndex 屏障提供线性一致读。
	ReadFollowerIndex
	// ReadStaleLocal 直接读取本地状态机，零网络往返，允许读到略微滞后的状态。
	ReadStaleLocal
)

// Scan 在单分片布局下扫描本地副本，区间包含 start 和 end，空边界表示无界。
// 调用方应在使用完毕后关闭迭代器。
func (c *ClusterDB) Scan(start, end []byte) (*Iterator, error) {
	return c.ScanContext(context.Background(), start, end, ReadStaleLocal)
}

// ScanContext 在单分片布局下按指定一致级别扫描区间 [start, end]。
// 当 consistency 为 ReadLinearizable 或 ReadFollowerIndex 时，先执行 Raft ReadIndex 屏障再打开 MVCC 迭代器。
func (c *ClusterDB) ScanContext(ctx context.Context, start, end []byte, consistency ReadConsistency) (*Iterator, error) {
	if c.config.ShardCount != 1 {
		return nil, ErrCrossShardScan
	}
	if consistency == ReadLinearizable || consistency == ReadFollowerIndex {
		if err := c.readIndexBarrier(ctx, 0); err != nil {
			return nil, err
		}
	}
	sm, err := c.stateMachineForShard(0)
	if err != nil {
		return nil, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	db, ok := sm.db.(*DB)
	if !ok {
		return nil, ErrUnsupportedIndex
	}
	it, err := db.NewIterator(IteratorOptions{})
	if err != nil {
		return nil, err
	}
	it.startKey = bytes.Clone(start)
	it.endKey = bytes.Clone(end)
	it.Rewind()
	return it, nil
}

func (c *ClusterDB) groupIDs() []uint64 {
	groups := make([]uint64, 0, c.config.ShardCount+1)
	for i := 0; i < c.config.ShardCount; i++ {
		groups = append(groups, c.config.GroupIDForShard(uint32(i)))
	}
	if mid := c.config.MetadataGroupID(); mid != 0 {
		groups = append(groups, mid)
	}
	return groups
}

// Member 表示集群成员中的一个节点。
type Member struct {
	NodeID  uint64
	Address string
}

// Members 返回第一个 Raft group 的成员信息。
func (c *ClusterDB) Members() ([]Member, error) {
	membership, err := c.GetClusterMembership()
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(membership))
	for id := range membership {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, Member{NodeID: id, Address: membership[id]})
	}
	return out, nil
}

// ShardMembership 描述一个固定分片 group 的 voter/non-voter 视图。
type ShardMembership struct {
	ShardID        uint32
	GroupID        uint64
	ConfigChangeID uint64
	Voters         []uint64
	NonVoters      []uint64
	Removed        map[uint64]struct{}
	Addresses      map[uint64]string
}

// ShardMembership 返回指定分片 Raft group 的成员信息。
func (c *ClusterDB) ShardMembership(shardID uint32) (*ShardMembership, error) {
	c.mu.RLock()
	if c.nodeHost == nil || !c.state.Load() || int(shardID) >= c.config.ShardCount {
		c.mu.RUnlock()
		return nil, ErrNodeNotStarted
	}
	nodeHost := c.nodeHost
	groupID := c.config.GroupIDForShard(shardID)
	c.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := nodeHost.SyncGetShardMembership(ctx, groupID)
	if err != nil {
		return nil, err
	}
	out := &ShardMembership{ShardID: shardID, GroupID: groupID, ConfigChangeID: m.ConfigChangeID, Addresses: make(map[uint64]string), Removed: make(map[uint64]struct{})}
	for id, addr := range m.Nodes {
		out.Voters = append(out.Voters, id)
		out.Addresses[id] = addr
	}
	for id, addr := range m.NonVotings {
		out.NonVoters = append(out.NonVoters, id)
		out.Addresses[id] = addr
	}
	for id := range m.Removed {
		out.Removed[id] = struct{}{}
	}
	sort.Slice(out.Voters, func(i, j int) bool { return out.Voters[i] < out.Voters[j] })
	sort.Slice(out.NonVoters, func(i, j int) bool { return out.NonVoters[i] < out.NonVoters[j] })
	return out, nil
}

func (c *ClusterDB) requireShardLeader(shardID uint32) (uint64, error) {
	c.mu.RLock()
	if c.nodeHost == nil || !c.state.Load() || int(shardID) >= c.config.ShardCount {
		c.mu.RUnlock()
		return 0, ErrNodeNotStarted
	}
	c.mu.RUnlock()
	leader, err := c.leaderForShard(shardID)
	if err != nil {
		return 0, err
	}
	if leader != c.config.NodeID {
		return leader, ErrNotLeader
	}
	return leader, nil
}

func (c *ClusterDB) changeShardMembership(shardID uint32, nodeID uint64, address string, add, nonVoting bool) error {
	if _, err := c.requireShardLeader(shardID); err != nil {
		return err
	}
	members, err := c.ShardMembership(shardID)
	if err != nil {
		return err
	}
	return c.changeShardMembershipAt(members, nodeID, address, add, nonVoting)
}

func (c *ClusterDB) changeShardMembershipAt(members *ShardMembership, nodeID uint64, address string, add, nonVoting bool) error {
	if _, err := c.requireShardLeader(members.ShardID); err != nil {
		return err
	}
	c.mu.RLock()
	nodeHost := c.nodeHost
	c.mu.RUnlock()
	if nodeHost == nil {
		return ErrNodeNotStarted
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return changeReplica(ctx, nodeHost, members.GroupID, members.ConfigChangeID, nodeID, address, add, nonVoting)
}

func changeReplica(ctx context.Context, nodeHost *dragonboat.NodeHost, groupID, configChangeID, nodeID uint64, address string, add, nonVoting bool) error {
	switch {
	case add && nonVoting:
		return nodeHost.SyncRequestAddNonVoting(ctx, groupID, nodeID, address, configChangeID)
	case add:
		return nodeHost.SyncRequestAddReplica(ctx, groupID, nodeID, address, configChangeID)
	default:
		return nodeHost.SyncRequestDeleteReplica(ctx, groupID, nodeID, configChangeID)
	}
}

func (c *ClusterDB) AddShardLearner(shardID uint32, nodeID uint64, address string) error {
	return c.changeShardMembership(shardID, nodeID, address, true, true)
}

func (c *ClusterDB) PromoteShardLearner(shardID uint32, nodeID uint64, address string) error {
	return c.changeShardMembership(shardID, nodeID, address, true, false)
}

func (c *ClusterDB) RemoveShardReplica(shardID uint32, nodeID uint64) error {
	return c.changeShardMembership(shardID, nodeID, "", false, false)
}

// Shards 返回固定分片布局以及各 group 最近一次已知的 leader。
func (c *ClusterDB) Shards() []ShardInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.config.ShardCount <= 0 {
		return nil
	}
	out := make([]ShardInfo, 0, c.config.ShardCount)
	for i := 0; i < c.config.ShardCount; i++ {
		info := ShardInfo{ID: uint32(i), GroupID: c.config.GroupIDForShard(uint32(i))}
		if i < len(c.shards) && c.shards[i] != nil {
			info.LeaderID = c.shards[i].leaderID.Load()
		}
		out = append(out, info)
	}
	return out
}

// LeaderOf 返回 key 所属分片 group 的 leader 节点 ID。
func (c *ClusterDB) LeaderOf(key []byte) (uint64, error) {
	c.mu.RLock()
	if c.nodeHost == nil || !c.state.Load() {
		c.mu.RUnlock()
		return 0, ErrNodeNotStarted
	}
	shard := c.shardFor(key)
	count := c.config.ShardCount
	c.mu.RUnlock()
	if count <= 0 || int(shard) >= count {
		return 0, ErrNodeNotStarted
	}
	return c.leaderForShard(shard)
}
