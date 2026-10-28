package knocidb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
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
	nodeHost     *dragonboat.NodeHost
	config       ClusterOptions
	dbOptions    Options
	mu           sync.RWMutex
	leaderID     uint64
	stateMachine *KVStateMachine
	state        bool
	Sync         bool // 是否使用同步模式进行操作

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
	// 创建状态机工厂函数
	db, err := Open(rdb.dbOptions)
	if err != nil {
		return fmt.Errorf("open database failed: %w", err)
	}
	factory := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		sm := NewKVStateMachine(clusterID, nodeID, db)
		rdb.stateMachine = sm
		return sm
	}

	// 获取Raft配置
	rc := rdb.config.GetRaftConfig()

	// 检查是否需要加入现有集群
	if rdb.config.JoinCluster {
		// 加入现有集群
		log.Printf("joining raft group, NodeID: %d, ClusterID: %d\n", rdb.config.NodeID, rdb.config.ClusterID)
		if err := rdb.nodeHost.StartReplica(
			rdb.config.InitialMembers,
			true,
			factory,
			rc,
		); err != nil {
			return fmt.Errorf("join goup failed: %w", err)
		}
	} else {
		// 启动新集群
		log.Printf("create new raft group, NodeID: %d, ClusterID: %d\n", rdb.config.NodeID, rdb.config.ClusterID)
		if err := rdb.nodeHost.StartReplica(
			rdb.config.InitialMembers,
			false,
			factory,
			rc,
		); err != nil {
			return fmt.Errorf("create new raft group failed: %w", err)
		}
	}

	// 启动Leader检测
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

// leaderMonitor 监控Leader变化
func (rdb *ClusterDB) leaderMonitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rdb.mu.RLock()
			nodeHost := rdb.nodeHost
			rdb.mu.RUnlock()
			if nodeHost == nil {
				return
			}

			// 获取Leader信息
			leaderID, _, valid, err := nodeHost.GetLeaderID(rdb.config.ClusterID)
			if err != nil || !valid {
				continue
			}

			rdb.mu.Lock()
			oldLeaderID := rdb.leaderID
			rdb.leaderID = leaderID
			rdb.mu.Unlock()

			// 如果Leader发生变化，记录日志
			if oldLeaderID != leaderID {
				log.Printf("Leader changed: %d -> %d\n", oldLeaderID, leaderID)
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

// GetLeaderID 获取当前Leader的NodeID
func (rdb *ClusterDB) GetLeaderID() (uint64, error) {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	if rdb.nodeHost == nil || !rdb.state {
		return 0, ErrNodeNotStarted
	}

	leaderID, _, valid, err := rdb.nodeHost.GetLeaderID(rdb.config.ClusterID)
	if err != nil {
		return 0, err
	}
	if !valid {
		return 0, ErrNotLeader
	}

	return leaderID, nil
}

// putLocal 在当前节点通过 Raft 共识写入键值对。
func (rdb *ClusterDB) putLocal(key, value []byte) error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
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
		_, err := rdb.nodeHost.SyncPropose(ctx, rdb.nodeHost.GetNoOPSession(rdb.config.ClusterID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := rdb.nodeHost.Propose(rdb.nodeHost.GetNoOPSession(rdb.config.ClusterID), data, 5*time.Second)
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
func (rdb *ClusterDB) deleteLocal(key []byte) error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
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
		_, err := rdb.nodeHost.SyncPropose(ctx, rdb.nodeHost.GetNoOPSession(rdb.config.ClusterID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := rdb.nodeHost.Propose(rdb.nodeHost.GetNoOPSession(rdb.config.ClusterID), data, 5*time.Second)
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
func (rdb *ClusterDB) getLocal(key []byte) ([]byte, error) {
	if rdb.nodeHost == nil || !rdb.state {
		return nil, ErrNodeNotStarted
	}
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
		result, err := rdb.nodeHost.SyncRead(ctx, rdb.config.ClusterID, data)
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

// BatchWrite 通过Raft共识批量写入
func (rdb *ClusterDB) BatchWrite(commands []Command, batchID uint64) error {
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}
	// 创建批处理命令
	batchCmd := BatchCommand{
		Op:       OpBatch,
		Commands: commands,
		BatchID:  batchID,
	}

	// 序列化命令
	data, err := json.Marshal(batchCmd)
	if err != nil {
		return err
	}

	// 根据Sync字段决定使用同步还是异步模式
	if rdb.Sync {
		// 使用同步Propose方法
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := rdb.nodeHost.SyncPropose(ctx, rdb.nodeHost.GetNoOPSession(rdb.config.ClusterID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := rdb.nodeHost.Propose(rdb.nodeHost.GetNoOPSession(rdb.config.ClusterID), data, 10*time.Second)
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
		return errors.New("batch write failed")
	}
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

// AddNode 向集群添加新节点
func (rdb *ClusterDB) AddNode(nodeID uint64, address string) error {
	// 先在不持有写锁的情况下判断状态和 Leader，避免 Lock 后调用 IsLeader 造成重入死锁
	rdb.mu.RLock()
	started := rdb.nodeHost != nil && rdb.state
	leader := rdb.leaderID == rdb.config.NodeID
	rdb.mu.RUnlock()
	if !started {
		return ErrNodeNotStarted
	}
	if !leader {
		return ErrNotLeader
	}

	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}

	// 根据Sync字段决定使用同步还是异步模式
	if rdb.Sync {
		// 使用同步方法添加节点
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return rdb.nodeHost.SyncRequestAddReplica(ctx, rdb.config.ClusterID, nodeID, address, 0)
	} else {
		// 使用异步方法添加节点
		rs, err := rdb.nodeHost.RequestAddReplica(rdb.config.ClusterID, nodeID, address, 0, 30*time.Second)
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
		return errors.New("add node failed")
	}
}

// RemoveNode 从集群移除节点
func (rdb *ClusterDB) RemoveNode(nodeID uint64) error {
	rdb.mu.RLock()
	started := rdb.nodeHost != nil && rdb.state
	leader := rdb.leaderID == rdb.config.NodeID
	rdb.mu.RUnlock()
	if !started {
		return ErrNodeNotStarted
	}
	if !leader {
		return ErrNotLeader
	}

	rdb.mu.Lock()
	defer rdb.mu.Unlock()
	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}

	// 根据Sync字段决定使用同步还是异步模式
	if rdb.Sync {
		// 使用同步方法移除节点
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return rdb.nodeHost.SyncRequestDeleteReplica(ctx, rdb.config.ClusterID, nodeID, 0)
	} else {
		// 使用异步方法移除节点
		rs, err := rdb.nodeHost.RequestDeleteReplica(rdb.config.ClusterID, nodeID, 0, 30*time.Second)
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
		return errors.New("remove node failed")
	}
}
