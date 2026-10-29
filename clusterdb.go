package knocidb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/statemachine"
)

var (
	// ErrNotLeader 表示当前节点不是Leader
	ErrNotLeader = errors.New("current node not Leader")
	// ErrTimeout 表示操作超时
	ErrTimeout = errors.New("operation timeout")
	// ErrCanceled 表示操作被取消
	ErrCanceled = errors.New("operation canceled")
	// ErrNodeNotStarted 表示节点未开启
	ErrNodeNotStarted = errors.New("cluster not started")
	// ErrClusterClosed 表示节点已关闭
	ErrClusterClosed = errors.New("cluster is closed")
	// ErrClusterUnauthorized 表示内部 RPC 鉴权失败。
	ErrClusterUnauthorized = errors.New("cluster rpc unauthorized")
)

// ClusterDB 管理单 Raft Group 集群节点和共识操作。
type ClusterDB struct {
	nodeHost        *dragonboat.NodeHost
	config          ClusterOptions
	dbOptions       Options
	mu              sync.RWMutex
	leaderID        uint64
	stateMachine    *KVStateMachine
	shards          []*clusterShard
	leaderIDs       []uint64
	metadataGroupID uint64
	metadataSM      *ShardMapStateMachine
	state           bool
	Sync            bool // 是否使用同步模式进行操作

	rpcListener net.Listener
	rpcWG       sync.WaitGroup

	rpcRequests  atomic.Uint64
	rpcErrors    atomic.Uint64
	forwardedOps atomic.Uint64
}

// OpenCluster 打开一个单 Raft Group 的集群节点。
func OpenCluster(config ClusterOptions, dboption Options) (*ClusterDB, error) {
	config.applyDefaults()
	if err := ValidateClusterOptions(&config); err != nil {
		return nil, err
	}
	// 创建NodeHost配置
	nhc := config.GetNodeHostOptions()

	// 创建NodeHost
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, fmt.Errorf("create nodehost failed: %w", err)
	}

	// 创建节点管理器
	rdb := &ClusterDB{
		nodeHost:  nodeHost,
		config:    config,
		dbOptions: dboption,
		leaderID:  0,
		state:     false,
		Sync:      config.Sync,
	}

	if err = rdb.start(); err != nil {
		nodeHost.Close()
		return nil, err
	}
	if config.RPCAddress != "" {
		if err = rdb.startRPC(); err != nil {
			_ = rdb.stop()
			return nil, err
		}
	}
	return rdb, nil
}

// start 启动本地存储和 Raft 副本。
func (rdb *ClusterDB) start() error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	shardCount := rdb.config.ShardCount
	if shardCount <= 0 {
		shardCount = 1
	}
	rdb.leaderIDs = make([]uint64, shardCount)
	rdb.shards = make([]*clusterShard, 0, shardCount)

	closeShardDBs := func() {
		for _, sh := range rdb.shards {
			if sh.db != nil {
				_ = sh.db.Close()
			}
		}
		rdb.shards = nil
	}

	for i := 0; i < shardCount; i++ {
		dbOptions := rdb.dbOptions
		if shardCount > 1 {
			dbOptions.DirPath = filepath.Join(rdb.dbOptions.DirPath, fmt.Sprintf("shard-%d", i))
			if rdb.dbOptions.SnapshotPath != "" {
				dbOptions.SnapshotPath = filepath.Join(rdb.dbOptions.SnapshotPath, fmt.Sprintf("shard-%d", i))
			}
			if dbOptions.ObjectTier.Enabled {
				prefix := strings.TrimRight(dbOptions.ObjectTier.Prefix, "/")
				if prefix != "" {
					prefix += "/"
				}
				dbOptions.ObjectTier.Prefix = prefix + fmt.Sprintf("shard-%d", i)
			}
		}
		db, err := Open(dbOptions)
		if err != nil {
			closeShardDBs()
			return fmt.Errorf("open database for shard %d failed: %w", i, err)
		}
		groupID := rdb.config.GroupIDForShard(uint32(i))
		shard := &clusterShard{
			id:      uint32(i),
			groupID: groupID,
			db:      db,
		}
		shard.sm = NewKVStateMachine(groupID, rdb.config.NodeID, db)
		rdb.shards = append(rdb.shards, shard)
		if i == 0 {
			rdb.stateMachine = shard.sm
		}

		rc := rdb.config.GetRaftConfig()
		rc.ShardID = groupID
		factory := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
			return shard.sm
		}
		join := rdb.config.JoinCluster
		initialMembers := rdb.config.InitialMembers
		if join {
			initialMembers = nil
		}
		if err := rdb.nodeHost.StartReplica(initialMembers, join, factory, rc); err != nil {
			closeShardDBs()
			return fmt.Errorf("start raft group %d (shard %d) failed: %w", groupID, i, err)
		}
	}

	if metadataID := rdb.config.MetadataGroupID(); metadataID != 0 {
		sm := NewShardMapStateMachine(rdb.config.ShardMapEpoch, shardCount)
		rc := rdb.config.GetRaftConfig()
		rc.ShardID = metadataID
		factory := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine { return sm }
		initialMembers := rdb.config.InitialMembers
		if rdb.config.JoinCluster {
			initialMembers = nil
		}
		if err := rdb.nodeHost.StartReplica(initialMembers, rdb.config.JoinCluster, factory, rc); err != nil {
			closeShardDBs()
			return fmt.Errorf("start metadata group %d failed: %w", metadataID, err)
		}
		rdb.metadataGroupID = metadataID
		rdb.metadataSM = sm
	}

	go rdb.leaderMonitor()
	rdb.state = true
	return nil
}

func (rdb *ClusterDB) Close() error {
	if !rdb.state {
		return ErrClusterClosed
	}
	return rdb.stop()
}

// stop 停止本地 RPC、Raft 副本和 NodeHost。
func (rdb *ClusterDB) stop() error {
	rdb.mu.Lock()
	if rdb.rpcListener != nil {
		_ = rdb.rpcListener.Close()
		rdb.rpcListener = nil
	}
	rdb.mu.Unlock()
	rdb.rpcWG.Wait()

	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	if rdb.nodeHost != nil {
		rdb.nodeHost.Close()
		rdb.nodeHost = nil
	}
	rdb.state = false
	return nil
}

// leaderMonitor 监控各 shard 的 Leader 变化。
func (rdb *ClusterDB) leaderMonitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rdb.mu.RLock()
			nodeHost := rdb.nodeHost
			shards := append([]*clusterShard(nil), rdb.shards...)
			rdb.mu.RUnlock()
			if nodeHost == nil {
				return
			}

			changed := false
			leaders := make([]uint64, len(shards))
			for i, sh := range shards {
				leaderID, _, valid, err := nodeHost.GetLeaderID(sh.groupID)
				if err != nil || !valid {
					continue
				}
				leaders[i] = leaderID
				if sh.id == 0 {
					rdb.mu.Lock()
					oldLeaderID := rdb.leaderID
					rdb.leaderID = leaderID
					rdb.mu.Unlock()
					if oldLeaderID != leaderID {
						changed = true
					}
				}
			}

			rdb.mu.Lock()
			if len(rdb.leaderIDs) == len(leaders) {
				copy(rdb.leaderIDs, leaders)
			}
			rdb.mu.Unlock()

			if changed {
				log.Printf("Leader changed on shard 0")
			}
		}
	}
}

// ClusterStats 是 ClusterDB 的运行状态快照。
type ClusterStats struct {
	NodeID       uint64
	LeaderID     uint64
	IsLeader     bool
	RPCRequests  uint64
	RPCErrors    uint64
	ForwardedOps uint64
}

// Stats 返回当前节点的轻量运行状态，可用于监控和健康检查。
func (rdb *ClusterDB) Stats() ClusterStats {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()
	return ClusterStats{
		NodeID:       rdb.config.NodeID,
		LeaderID:     rdb.leaderID,
		IsLeader:     rdb.state && rdb.nodeHost != nil && rdb.leaderID != 0 && rdb.leaderID == rdb.config.NodeID,
		RPCRequests:  rdb.rpcRequests.Load(),
		RPCErrors:    rdb.rpcErrors.Load(),
		ForwardedOps: rdb.forwardedOps.Load(),
	}
}

// IsLeader 检查当前节点是否是Leader
func (rdb *ClusterDB) IsLeader() bool {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	return rdb.state && rdb.nodeHost != nil && rdb.leaderID != 0 && rdb.leaderID == rdb.config.NodeID
}

// GetLeaderID 获取 shard 0 的 Leader NodeID。
func (rdb *ClusterDB) GetLeaderID() (uint64, error) {
	return rdb.leaderForShard(0)
}

// putLocal 在当前节点通过 Raft 共识写入键值对。
func (rdb *ClusterDB) putLocal(key, value []byte, shard uint32) error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
	groupID := rdb.config.GroupIDForShard(shard)
	// 创建命令
	cmd := Command{
		Op:    OpPut,
		Key:   key,
		Value: value,
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// 根据Sync字段决定使用同步还是异步模式
	if rdb.Sync {
		// 使用同步Propose方法
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := rdb.nodeHost.SyncPropose(ctx, rdb.nodeHost.GetNoOPSession(groupID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := rdb.nodeHost.Propose(rdb.nodeHost.GetNoOPSession(groupID), data, 5*time.Second)
		if err != nil {
			return err
		}

		// 等待操作完成
		res := <-rs.ResultC()
		if res.Completed() {
			rs.Release()
			return nil
		}

		// 处理错误
		if res.Timeout() {
			return ErrTimeout
		}
		if res.Terminated() {
			return ErrCanceled
		}
		rs.Release()
		return errors.New("propose failed")
	}
}

// deleteLocal 在当前节点通过 Raft 共识删除键。
func (rdb *ClusterDB) deleteLocal(key []byte, shard uint32) error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
	groupID := rdb.config.GroupIDForShard(shard)
	// 创建命令
	cmd := Command{
		Op:  OpDelete,
		Key: key,
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// 根据Sync字段决定使用同步还是异步模式
	if rdb.Sync {
		// 使用同步Propose方法
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := rdb.nodeHost.SyncPropose(ctx, rdb.nodeHost.GetNoOPSession(groupID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := rdb.nodeHost.Propose(rdb.nodeHost.GetNoOPSession(groupID), data, 5*time.Second)
		if err != nil {
			return err
		}

		// 等待操作完成
		res := <-rs.ResultC()
		if res.Completed() {
			rs.Release()
			return nil
		}

		// 处理错误
		if res.Timeout() {
			return ErrTimeout
		}
		if res.Terminated() {
			return ErrCanceled
		}
		rs.Release()
		return errors.New("delete failed")
	}
}

// getLocal 在当前节点通过 Raft ReadIndex 读取键值。
func (rdb *ClusterDB) getLocal(key []byte, shard uint32) ([]byte, error) {
	if rdb.nodeHost == nil || !rdb.state {
		return nil, ErrNodeNotStarted
	}
	groupID := rdb.config.GroupIDForShard(shard)
	// 创建查询命令
	cmd := Command{
		Key: key,
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	// 根据Sync字段决定使用同步还是异步模式
	if rdb.Sync {
		// 使用同步ReadIndex方法
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := rdb.nodeHost.SyncRead(ctx, groupID, data)
		if err != nil {
			return nil, err
		}

		// 处理结果
		value, ok := result.([]byte)
		if !ok {
			return nil, errors.New("invalid type")
		}

		return value, nil
	} else {
		// 使用异步ReadIndex方法
		rs, err := rdb.nodeHost.ReadIndex(groupID, 5*time.Second)
		if err != nil {
			return nil, err
		}

		// 等待ReadIndex操作完成
		res := <-rs.ResultC()
		if !res.Completed() {
			if res.Timeout() {
				return nil, ErrTimeout
			}
			if res.Terminated() {
				return nil, ErrCanceled
			}
			return nil, errors.New("ReadIndex failed")
		}

		// 执行本地读取
		result, err := rdb.nodeHost.ReadLocalNode(rs, data)
		if err != nil {
			return nil, err
		}

		// 处理结果
		value, ok := result.([]byte)
		if !ok {
			return nil, errors.New("invalid type")
		}
		rs.Release()
		return value, nil
	}
}

// BatchWrite 通过 Raft 共识批量写入。所有命令必须落在同一个 shard。
func (rdb *ClusterDB) BatchWrite(commands []Command, batchID uint64) error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
	if len(commands) == 0 {
		return nil
	}
	shard := rdb.shardFor(commands[0].Key)
	for _, cmd := range commands[1:] {
		if rdb.shardFor(cmd.Key) != shard {
			return ErrCrossShardBatch
		}
	}
	batchCmd := BatchCommand{Op: OpBatch, Commands: commands, BatchID: batchID}
	data, err := json.Marshal(batchCmd)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if _, addr, ok := rdb.forwardTarget(shard); ok {
			err := rdb.forwardRequest(addr, rpcOpBatch, nil, data)
			if !errors.Is(err, ErrNotLeader) {
				return err
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := rdb.batchWriteLocal(batchCmd, shard)
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

func (rdb *ClusterDB) batchWriteLocal(batchCmd BatchCommand, shard uint32) error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
	groupID := rdb.config.GroupIDForShard(shard)
	data, err := json.Marshal(batchCmd)
	if err != nil {
		return err
	}
	if rdb.Sync {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := rdb.nodeHost.SyncPropose(ctx, rdb.nodeHost.GetNoOPSession(groupID), data)
		return err
	}
	rs, err := rdb.nodeHost.Propose(rdb.nodeHost.GetNoOPSession(groupID), data, 10*time.Second)
	if err != nil {
		return err
	}
	res := <-rs.ResultC()
	if res.Completed() {
		rs.Release()
		return nil
	}
	if res.Timeout() {
		return ErrTimeout
	}
	if res.Terminated() {
		return ErrCanceled
	}
	rs.Release()
	return errors.New("batch write failed")
}

// GetClusterMembership 获取集群成员信息
func (rdb *ClusterDB) GetClusterMembership() (map[uint64]string, error) {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	if rdb.nodeHost == nil || !rdb.state {
		return nil, ErrNodeNotStarted
	}

	// 根据Sync字段决定使用同步还是异步模式
	if rdb.Sync {
		// 直接使用同步方法获取成员信息
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		membership, err := rdb.nodeHost.SyncGetShardMembership(ctx, rdb.config.ClusterID)
		if err != nil {
			return nil, err
		}

		return membership.Nodes, nil
	} else {
		// 使用异步ReadIndex方法确保线性一致性，然后获取成员信息
		rs, err := rdb.nodeHost.ReadIndex(rdb.config.ClusterID, 5*time.Second)
		if err != nil {
			return nil, err
		}

		// 等待ReadIndex操作完成
		res := <-rs.ResultC()
		if !res.Completed() {
			if res.Timeout() {
				return nil, ErrTimeout
			}
			if res.Terminated() {
				return nil, ErrCanceled
			}
			return nil, errors.New("get cluster membership failed")
		}
		rs.Release()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		membership, err := rdb.nodeHost.SyncGetShardMembership(ctx, rdb.config.ClusterID)
		if err != nil {
			return nil, err
		}

		return membership.Nodes, nil
	}
}

func (rdb *ClusterDB) requireLeaderAll() error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
	for i := 0; i < rdb.config.ShardCount; i++ {
		leader, err := rdb.leaderForShard(uint32(i))
		if err != nil {
			return err
		}
		if leader != rdb.config.NodeID {
			return ErrNotLeader
		}
	}
	if mid := rdb.config.MetadataGroupID(); mid != 0 {
		leader, _, valid, err := rdb.nodeHost.GetLeaderID(mid)
		if err != nil {
			return err
		}
		if !valid || leader != rdb.config.NodeID {
			return ErrNotLeader
		}
	}
	return nil
}

func (rdb *ClusterDB) changeMembership(add bool, nonVoting bool, nodeID uint64, address string) error {
	if err := rdb.requireLeaderAll(); err != nil {
		return err
	}
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
	for _, groupID := range rdb.groupIDs() {
		if rdb.Sync {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			var err error
			switch {
			case add && nonVoting:
				err = rdb.nodeHost.SyncRequestAddNonVoting(ctx, groupID, nodeID, address, 0)
			case add:
				err = rdb.nodeHost.SyncRequestAddReplica(ctx, groupID, nodeID, address, 0)
			default:
				err = rdb.nodeHost.SyncRequestDeleteReplica(ctx, groupID, nodeID, 0)
			}
			cancel()
			if err != nil {
				return err
			}
			continue
		}
		var (
			rs  *dragonboat.RequestState
			err error
		)
		switch {
		case add && nonVoting:
			rs, err = rdb.nodeHost.RequestAddNonVoting(groupID, nodeID, address, 0, 30*time.Second)
		case add:
			rs, err = rdb.nodeHost.RequestAddReplica(groupID, nodeID, address, 0, 30*time.Second)
		default:
			rs, err = rdb.nodeHost.RequestDeleteReplica(groupID, nodeID, 0, 30*time.Second)
		}
		if err != nil {
			return err
		}
		res := <-rs.ResultC()
		if res.Completed() {
			rs.Release()
			continue
		}
		if res.Timeout() {
			return ErrTimeout
		}
		if res.Terminated() {
			return ErrCanceled
		}
		rs.Release()
		return errors.New("membership change failed")
	}
	return nil
}

// AddNode adds (or promotes) a voting replica in every fixed group.
func (rdb *ClusterDB) AddNode(nodeID uint64, address string) error {
	return rdb.changeMembership(true, false, nodeID, address)
}

// AddLearner adds a non-voting replica in every fixed group.
func (rdb *ClusterDB) AddLearner(nodeID uint64, address string) error {
	return rdb.changeMembership(true, true, nodeID, address)
}

// PromoteLearner promotes an existing non-voting replica to voting.
func (rdb *ClusterDB) PromoteLearner(nodeID uint64, address string) error {
	return rdb.AddNode(nodeID, address)
}

// RemoveNode removes a replica from every fixed group.
func (rdb *ClusterDB) RemoveNode(nodeID uint64) error {
	return rdb.changeMembership(false, false, nodeID, "")
}
