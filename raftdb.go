package knocidb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
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
	// ErrRaftNotStartted 表示节点未开启
	ErrNodeNotStarted = errors.New("raft not started")
	// ErrRaftIsClosed 表示节点已关闭
	ErrRaftIsClosed = errors.New("raft is closed")
)

// RaftDB 管理Raft节点和处理共识操作
type RaftDB struct {
	nodeHost     *dragonboat.NodeHost
	config       RaftOptions
	dbOptions    Options
	mu           sync.RWMutex
	leaderID     uint64
	stateMachine *KVStateMachine
	state        bool
	Sync         bool // 是否使用同步模式进行操作
}

// NewRaftDB 创建一个新的节点管理器
func OpenRaft(config RaftOptions, dboption Options) (*RaftDB, error) {
	// 创建NodeHost配置
	nhc := config.GetNodeHostOptions()

	// 创建NodeHost
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, fmt.Errorf("create nodehost failed: %w", err)
	}

	// 创建节点管理器
	rdb := &RaftDB{
		nodeHost:  nodeHost,
		config:    config,
		dbOptions: dboption,
		leaderID:  0,
		state:     false,
		Sync:      config.Sync,
	}

	err = rdb.start()
	if err != nil {
		return nil, err
	}
	return rdb, nil
}

// Start 启动Raft节点
func (rdb *RaftDB) start() error {
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
	rc := rdb.config.GetRaftOptions()

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

func (rdb *RaftDB) Close() error {
	if !rdb.state {
		return ErrRaftIsClosed
	}
	return rdb.stop()
}

// Stop 停止Raft节点
func (rdb *RaftDB) stop() error {
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
func (rdb *RaftDB) leaderMonitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if rdb.nodeHost == nil {
				return
			}

			// 获取Leader信息
			leaderID, _, valid, err := rdb.nodeHost.GetLeaderID(rdb.config.ClusterID)
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

// IsLeader 检查当前节点是否是Leader
func (rdb *RaftDB) IsLeader() bool {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	return rdb.leaderID == rdb.config.NodeID
}

// GetLeaderID 获取当前Leader的NodeID
func (rdb *RaftDB) GetLeaderID() (uint64, error) {
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

// Put 通过Raft共识写入键值对
func (rdb *RaftDB) Put(key, value []byte) error {
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

// Delete 通过Raft共识删除键
func (rdb *RaftDB) Delete(key []byte) error {
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

// Get 通过Raft共识读取键值
func (rdb *RaftDB) Get(key []byte) ([]byte, error) {
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
func (rdb *RaftDB) BatchWrite(commands []Command, batchID uint64) error {
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
func (rdb *RaftDB) GetClusterMembership() (map[uint64]string, error) {
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
func (rdb *RaftDB) AddNode(nodeID uint64, address string) error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}

	// 检查是否是Leader
	if !rdb.IsLeader() {
		return ErrNotLeader
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
func (rdb *RaftDB) RemoveNode(nodeID uint64) error {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	if rdb.nodeHost == nil || !rdb.state {
		return ErrNodeNotStarted
	}

	// 检查是否是Leader
	if !rdb.IsLeader() {
		return ErrNotLeader
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
