package knocidb

import (
	"context"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
)

type clusterShard struct {
	id      uint32
	groupID uint64
	db      *DB
	sm      *KVStateMachine
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
	started := c.state && nodeHost != nil
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
	c.mu.Lock()
	if int(shard) < len(c.leaderIDs) {
		c.leaderIDs[shard] = leader
	}
	if shard == 0 {
		c.leaderID = leader
	}
	c.mu.Unlock()
	return leader, nil
}

func (c *ClusterDB) dbForShard(shard uint32) (*DB, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.nodeHost == nil || !c.state || int(shard) >= len(c.shards) {
		return nil, ErrNodeNotStarted
	}
	return c.shards[shard].db, nil
}

// Scan 在单分片布局下返回 shard 0 的迭代器。
// 跨分片扫描尚未实现合并迭代器，因此直接返回错误。
func (c *ClusterDB) Scan(start, end []byte) (*Iterator, error) {
	if c.config.ShardCount != 1 {
		return nil, ErrCrossShardScan
	}
	db, err := c.dbForShard(0)
	if err != nil {
		return nil, err
	}
	it, err := db.NewIterator(IteratorOptions{})
	if err != nil {
		return nil, err
	}
	if len(start) > 0 {
		it.Seek(start)
	} else {
		it.Rewind()
	}
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
	ShardID   uint32
	GroupID   uint64
	Voters    []uint64
	NonVoters []uint64
	Removed   map[uint64]struct{}
	Addresses map[uint64]string
}

// ShardMembership 返回指定分片 Raft group 的成员信息。
func (c *ClusterDB) ShardMembership(shardID uint32) (*ShardMembership, error) {
	c.mu.RLock()
	if c.nodeHost == nil || !c.state || int(shardID) >= c.config.ShardCount {
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
	out := &ShardMembership{ShardID: shardID, GroupID: groupID, Addresses: make(map[uint64]string), Removed: make(map[uint64]struct{})}
	for id, addr := range m.Nodes {
		out.Voters = append(out.Voters, id)
		out.Addresses[id] = addr
	}
	for id := range m.NonVotings {
		out.NonVoters = append(out.NonVoters, id)
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
	if c.nodeHost == nil || !c.state || int(shardID) >= c.config.ShardCount {
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
	groupID := c.config.GroupIDForShard(shardID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch {
	case add && nonVoting:
		return c.nodeHost.SyncRequestAddNonVoting(ctx, groupID, nodeID, address, 0)
	case add:
		return c.nodeHost.SyncRequestAddReplica(ctx, groupID, nodeID, address, 0)
	default:
		return c.nodeHost.SyncRequestDeleteReplica(ctx, groupID, nodeID, 0)
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
		if i < len(c.leaderIDs) {
			info.LeaderID = c.leaderIDs[i]
		}
		out = append(out, info)
	}
	return out
}

// LeaderOf 返回 key 所属分片 group 的 leader 节点 ID。
func (c *ClusterDB) LeaderOf(key []byte) (uint64, error) {
	c.mu.RLock()
	if c.nodeHost == nil || !c.state {
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
