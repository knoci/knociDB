package knocidb

import (
	"context"
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

// ClusterDB 管理多 Raft Group 集群节点和共识操作。
type ClusterDB struct {
	nodeHost        *dragonboat.NodeHost
	config          ClusterOptions
	dbOptions       Options
	mu              sync.RWMutex
	leaderID        atomic.Uint64
	stateMachine    *KVStateMachine
	shards          []*clusterShard
	leaderIDs       []uint64
	metadataGroupID uint64
	metadataSM      *ShardMapStateMachine
	state           atomic.Bool
	Sync            bool // 是否使用同步模式进行操作

	stopCh    chan struct{}
	stopOnce  sync.Once
	monitorWG sync.WaitGroup

	rpcListener net.Listener
	rpcWG       sync.WaitGroup
	rpcPoolMu   sync.Mutex
	rpcPool     map[string][]net.Conn

	rpcRequests  atomic.Uint64
	rpcErrors    atomic.Uint64
	forwardedOps atomic.Uint64
}

// OpenCluster 打开一个 Raft 集群节点。
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
		Sync:      config.Sync,
		stopCh:    make(chan struct{}),
	}

	if err = rdb.start(); err != nil {
		rdb.mu.Lock()
		if rdb.nodeHost != nil {
			rdb.nodeHost.Close()
			rdb.nodeHost = nil
		}
		rdb.mu.Unlock()
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

	// 失败时必须先关闭 nodeHost 停止活跃 Raft 副本，再关闭尚未被 Raft 接管的本地 shard DB。
	cleanupOnFailure := func(startedReplicas int) {
		if startedReplicas > 0 && rdb.nodeHost != nil {
			rdb.nodeHost.Close()
			rdb.nodeHost = nil
		}
		for i := startedReplicas; i < len(rdb.shards); i++ {
			_ = rdb.shards[i].sm.Close()
		}
		rdb.shards = nil
	}

	startedReplicas := 0
	for i := 0; i < shardCount; i++ {
		dbOptions := rdb.dbOptions
		// Multi-Raft 分片禁用本地墙钟 TTL 后台清扫，改为由 Raft 日志中的绝对时间戳确定性判定过期，
		// 彻底杜绝副本间因时钟漂移导致状态分叉。
		dbOptions.TTLScanInterval = 0
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
			cleanupOnFailure(startedReplicas)
			return fmt.Errorf("open database for shard %d failed: %w", i, err)
		}
		groupID := rdb.config.GroupIDForShard(uint32(i))
		shard := &clusterShard{
			id:      uint32(i),
			groupID: groupID,
		}
		shard.sm = NewKVStateMachine(groupID, rdb.config.NodeID, db)
		rdb.shards = append(rdb.shards, shard)
		if i == 0 {
			rdb.stateMachine = shard.sm
		}

		rc := rdb.config.GetRaftConfig()
		rc.ShardID = groupID
		onDiskFactory := func(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
			return shard.sm.asOnDisk()
		}
		join := rdb.config.JoinCluster
		initialMembers := rdb.config.InitialMembers
		if join {
			initialMembers = nil
		}
		if err := rdb.nodeHost.StartOnDiskReplica(initialMembers, join, onDiskFactory, rc); err != nil {
			cleanupOnFailure(startedReplicas)
			return fmt.Errorf("start raft group %d (shard %d) failed: %w", groupID, i, err)
		}
		startedReplicas++
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
			cleanupOnFailure(startedReplicas)
			return fmt.Errorf("start metadata group %d failed: %w", metadataID, err)
		}
		rdb.metadataGroupID = metadataID
		rdb.metadataSM = sm
	}

	rdb.state.Store(true)
	rdb.monitorWG.Add(1)
	go func() {
		defer rdb.monitorWG.Done()
		rdb.leaderMonitor()
	}()
	return nil
}

func (rdb *ClusterDB) Close() error {
	if !rdb.state.Load() {
		return ErrClusterClosed
	}
	return rdb.stop()
}

// stop 停止后台监控、本地 RPC、Raft 副本和 NodeHost。
func (rdb *ClusterDB) stop() error {
	rdb.state.Store(false)
	rdb.stopOnce.Do(func() {
		if rdb.stopCh != nil {
			close(rdb.stopCh)
		}
	})
	rdb.monitorWG.Wait()

	rdb.mu.Lock()
	if rdb.rpcListener != nil {
		_ = rdb.rpcListener.Close()
		rdb.rpcListener = nil
	}
	rdb.mu.Unlock()
	rdb.rpcWG.Wait()
	rdb.closeRPCPool()

	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	if rdb.nodeHost != nil {
		rdb.nodeHost.Close()
		rdb.nodeHost = nil
	}
	return nil
}

// leaderMonitor 监控各 shard 的 Leader 变化。
func (rdb *ClusterDB) leaderMonitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rdb.stopCh:
			return
		case <-ticker.C:
			rdb.mu.RLock()
			nodeHost := rdb.nodeHost
			shards := append([]*clusterShard(nil), rdb.shards...)
			rdb.mu.RUnlock()
			if nodeHost == nil || !rdb.state.Load() {
				return
			}

			changed := false
			for _, sh := range shards {
				select {
				case <-rdb.stopCh:
					return
				default:
				}
				leaderID, _, valid, err := nodeHost.GetLeaderID(sh.groupID)
				if err != nil || !valid {
					continue
				}
				sh.leaderID.Store(leaderID)
				if sh.id == 0 {
					oldLeaderID := rdb.leaderID.Swap(leaderID)
					if oldLeaderID != leaderID {
						changed = true
					}
				}
			}

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
	hasNodeHost := rdb.nodeHost != nil
	rdb.mu.RUnlock()
	leaderID := rdb.leaderID.Load()
	return ClusterStats{
		NodeID:       rdb.config.NodeID,
		LeaderID:     leaderID,
		IsLeader:     rdb.state.Load() && hasNodeHost && leaderID != 0 && leaderID == rdb.config.NodeID,
		RPCRequests:  rdb.rpcRequests.Load(),
		RPCErrors:    rdb.rpcErrors.Load(),
		ForwardedOps: rdb.forwardedOps.Load(),
	}
}

// IsLeader 检查当前节点是否是Leader
func (rdb *ClusterDB) IsLeader() bool {
	rdb.mu.RLock()
	hasNodeHost := rdb.nodeHost != nil
	rdb.mu.RUnlock()
	leaderID := rdb.leaderID.Load()
	return rdb.state.Load() && hasNodeHost && leaderID != 0 && leaderID == rdb.config.NodeID
}

// GetLeaderID 获取 shard 0 的 Leader NodeID。
func (rdb *ClusterDB) GetLeaderID() (uint64, error) {
	return rdb.leaderForShard(0)
}

// RegisterPeerRPC 动态注册或更新指定节点的内部转发 RPC 地址，支持运行期扩缩容与分片迁移。
func (rdb *ClusterDB) RegisterPeerRPC(nodeID uint64, rpcAddr string) {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	if rdb.config.PeerRPC == nil {
		rdb.config.PeerRPC = make(map[uint64]string)
	}
	if rpcAddr == "" {
		delete(rdb.config.PeerRPC, nodeID)
	} else {
		rdb.config.PeerRPC[nodeID] = rpcAddr
	}
}

// PeerRPCAddress 返回已知的对端内部 RPC 地址。
func (rdb *ClusterDB) PeerRPCAddress(nodeID uint64) (string, bool) {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()
	addr, ok := rdb.config.PeerRPC[nodeID]
	return addr, ok && addr != ""
}

// putLocal 在当前节点通过 Raft 共识写入键值对。
func (rdb *ClusterDB) putLocal(key, value []byte, shard uint32) error {
	return rdb.proposeWriteContext(context.Background(), Command{Op: OpPut, Key: key, Value: value}, shard)
}

func (rdb *ClusterDB) putLocalWithExpireAt(ctx context.Context, key, value []byte, expireAt int64, shard uint32) error {
	return rdb.proposeWriteContext(ctx, Command{Op: OpPut, Key: key, Value: value, ExpireAt: expireAt}, shard)
}

// deleteLocal 在当前节点通过 Raft 共识删除键。
func (rdb *ClusterDB) deleteLocal(key []byte, shard uint32) error {
	return rdb.proposeWriteContext(context.Background(), Command{Op: OpDelete, Key: key}, shard)
}

func (rdb *ClusterDB) proposeWrite(cmd Command, shard uint32) error {
	return rdb.proposeWriteContext(context.Background(), cmd, shard)
}

func (rdb *ClusterDB) proposeWriteContext(ctx context.Context, cmd Command, shard uint32) error {
	if err := validateRaftWrite(cmd); err != nil {
		return err
	}
	data := encodeRaftCommand(cmd)
	return rdb.proposeDataContext(ctx, shard, data, 5*time.Second)
}

func (rdb *ClusterDB) proposeData(shard uint32, data []byte, timeout time.Duration) error {
	return rdb.proposeDataContext(context.Background(), shard, data, timeout)
}

func (rdb *ClusterDB) proposeDataContext(ctx context.Context, shard uint32, data []byte, defaultTimeout time.Duration) error {
	rdb.mu.RLock()
	nodeHost := rdb.nodeHost
	started := rdb.state.Load() && nodeHost != nil
	rdb.mu.RUnlock()
	if !started {
		return ErrNodeNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session := nodeHost.GetNoOPSession(rdb.config.GroupIDForShard(shard))
	if rdb.Sync {
		callCtx := ctx
		var cancel context.CancelFunc
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			callCtx, cancel = context.WithTimeout(ctx, defaultTimeout)
			defer cancel()
		}
		result, err := nodeHost.SyncPropose(callCtx, session, data)
		if err != nil {
			return err
		}
		return raftResultError(result)
	}

	timeout := defaultTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem > 0 {
			timeout = rem
		} else {
			return ErrTimeout
		}
	}
	rs, err := nodeHost.Propose(session, data, timeout)
	if err != nil {
		return err
	}
	defer rs.Release()
	select {
	case <-ctx.Done():
		return ErrCanceled
	case res := <-rs.ResultC():
		switch {
		case res.Completed():
			return raftResultError(res.GetResult())
		case res.Timeout():
			return ErrTimeout
		case res.Terminated():
			return ErrCanceled
		default:
			return errors.New("raft proposal did not complete")
		}
	}
}

func (rdb *ClusterDB) readIndexBarrier(ctx context.Context, shard uint32) error {
	rdb.mu.RLock()
	nodeHost := rdb.nodeHost
	started := rdb.state.Load() && nodeHost != nil
	rdb.mu.RUnlock()
	if !started {
		return ErrNodeNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem > 0 {
			timeout = rem
		} else {
			return ErrTimeout
		}
	}
	rs, err := nodeHost.ReadIndex(rdb.config.GroupIDForShard(shard), timeout)
	if err != nil {
		return err
	}
	defer rs.Release()
	select {
	case <-ctx.Done():
		return ErrCanceled
	case res := <-rs.ResultC():
		if res.Completed() {
			return nil
		}
		if res.Timeout() {
			return ErrTimeout
		}
		if res.Terminated() {
			return ErrCanceled
		}
		return errors.New("ReadIndex failed")
	}
}

// getLocal 在当前节点通过 Raft ReadIndex（零 JSON 直传 raftLookupKey）读取键值。
func (rdb *ClusterDB) getLocal(key []byte, shard uint32) ([]byte, error) {
	return rdb.getLocalContext(context.Background(), key, shard, ReadLinearizable)
}

func (rdb *ClusterDB) getLocalContext(ctx context.Context, key []byte, shard uint32, consistency ReadConsistency) ([]byte, error) {
	if consistency == ReadStaleLocal {
		sm, err := rdb.stateMachineForShard(shard)
		if err != nil {
			return nil, err
		}
		res, err := sm.Lookup(raftLookupKey(key))
		if err != nil {
			return nil, err
		}
		val, ok := res.([]byte)
		if !ok {
			return nil, errors.New("invalid type")
		}
		return val, nil
	}

	rdb.mu.RLock()
	nodeHost := rdb.nodeHost
	started := rdb.state.Load() && nodeHost != nil
	rdb.mu.RUnlock()
	if !started {
		return nil, ErrNodeNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	groupID := rdb.config.GroupIDForShard(shard)
	query := raftLookupKey(key)

	if rdb.Sync {
		callCtx := ctx
		var cancel context.CancelFunc
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			callCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
		}
		result, err := nodeHost.SyncRead(callCtx, groupID, query)
		if err != nil {
			return nil, err
		}
		value, ok := result.([]byte)
		if !ok {
			return nil, errors.New("invalid type")
		}
		return value, nil
	}

	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem > 0 {
			timeout = rem
		} else {
			return nil, ErrTimeout
		}
	}
	rs, err := nodeHost.ReadIndex(groupID, timeout)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

	select {
	case <-ctx.Done():
		return nil, ErrCanceled
	case res := <-rs.ResultC():
		if !res.Completed() {
			if res.Timeout() {
				return nil, ErrTimeout
			}
			if res.Terminated() {
				return nil, ErrCanceled
			}
			return nil, errors.New("ReadIndex failed")
		}
	}

	result, err := nodeHost.ReadLocalNode(rs, query)
	if err != nil {
		return nil, err
	}
	value, ok := result.([]byte)
	if !ok {
		return nil, errors.New("invalid type")
	}
	return value, nil
}

// BatchWrite 通过 Raft 共识批量写入。所有命令必须落在同一个 shard。
func (rdb *ClusterDB) BatchWrite(commands []Command, batchID uint64) error {
	rdb.mu.RLock()
	started := rdb.state.Load() && rdb.nodeHost != nil
	rdb.mu.RUnlock()
	if !started {
		return ErrNodeNotStarted
	}
	if len(commands) == 0 {
		return nil
	}
	shard, err := rdb.shardForBatch(commands)
	if err != nil {
		return err
	}
	batchCmd := BatchCommand{Op: OpBatch, Commands: commands, BatchID: batchID}
	data := encodeRaftBatchCommand(batchCmd)
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
		err := rdb.proposeData(shard, data, 10*time.Second)
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

// shardForBatch validates every command before routing the batch.
func (rdb *ClusterDB) shardForBatch(commands []Command) (uint32, error) {
	if len(commands) == 0 {
		return 0, ErrInvalidRaftCommand
	}
	shard := rdb.shardFor(commands[0].Key)
	for i, cmd := range commands {
		if err := validateRaftWrite(cmd); err != nil {
			return 0, fmt.Errorf("command %d: %w", i+1, err)
		}
		if rdb.shardFor(cmd.Key) != shard {
			return 0, ErrCrossShardBatch
		}
	}
	return shard, nil
}

// GetClusterMembership 获取集群成员信息
func (rdb *ClusterDB) GetClusterMembership() (map[uint64]string, error) {
	rdb.mu.RLock()
	nodeHost := rdb.nodeHost
	started := rdb.state.Load() && nodeHost != nil
	rdb.mu.RUnlock()

	if !started {
		return nil, ErrNodeNotStarted
	}

	if rdb.Sync {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		membership, err := nodeHost.SyncGetShardMembership(ctx, rdb.config.ClusterID)
		if err != nil {
			return nil, err
		}
		return membership.Nodes, nil
	}

	rs, err := nodeHost.ReadIndex(rdb.config.ClusterID, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer rs.Release()

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	membership, err := nodeHost.SyncGetShardMembership(ctx, rdb.config.ClusterID)
	if err != nil {
		return nil, err
	}
	return membership.Nodes, nil
}

func (rdb *ClusterDB) requireLeaderAll() error {
	rdb.mu.RLock()
	nodeHost := rdb.nodeHost
	started := rdb.state.Load() && nodeHost != nil
	rdb.mu.RUnlock()
	if !started {
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
		leader, _, valid, err := nodeHost.GetLeaderID(mid)
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
	rdb.mu.RLock()
	nodeHost := rdb.nodeHost
	started := rdb.state.Load() && nodeHost != nil
	rdb.mu.RUnlock()
	if !started {
		return ErrNodeNotStarted
	}
	for _, groupID := range rdb.groupIDs() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		members, err := nodeHost.SyncGetShardMembership(ctx, groupID)
		cancel()
		if err != nil {
			return err
		}
		if rdb.Sync {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := changeReplica(ctx, nodeHost, groupID, members.ConfigChangeID, nodeID, address, add, nonVoting)
			cancel()
			if err != nil {
				return err
			}
			continue
		}
		if err := func() error {
			var rs *dragonboat.RequestState
			var reqErr error
			switch {
			case add && nonVoting:
				rs, reqErr = nodeHost.RequestAddNonVoting(groupID, nodeID, address, members.ConfigChangeID, 30*time.Second)
			case add:
				rs, reqErr = nodeHost.RequestAddReplica(groupID, nodeID, address, members.ConfigChangeID, 30*time.Second)
			default:
				rs, reqErr = nodeHost.RequestDeleteReplica(groupID, nodeID, members.ConfigChangeID, 30*time.Second)
			}
			if reqErr != nil {
				return reqErr
			}
			defer rs.Release()
			res := <-rs.ResultC()
			if res.Completed() {
				return nil
			}
			if res.Timeout() {
				return ErrTimeout
			}
			if res.Terminated() {
				return ErrCanceled
			}
			return errors.New("membership change failed")
		}(); err != nil {
			return err
		}
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
