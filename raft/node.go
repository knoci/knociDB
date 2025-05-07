package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"knocidb"
	"log"
	"os"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/statemachine"
)

var (
	// ErrNotLeader 表示当前节点不是Leader
	ErrNotLeader = errors.New("当前节点不是Leader")
	// ErrTimeout 表示操作超时
	ErrTimeout = errors.New("操作超时")
	// ErrCanceled 表示操作被取消
	ErrCanceled = errors.New("操作被取消")
	// ErrInvalidCommand 表示无效的命令
	ErrInvalidCommand = errors.New("无效的命令")
	// ErrDBNotOpen 表示数据库为空
	ErrDBEmpty = errors.New("Database not open")
	// ErrRaftNotStartted 表示节点未开启
	ErrNodeNotStarted = errors.New("Raft not started")
)

// NodeManager 管理Raft节点和处理共识操作
type NodeManager struct {
	nodeHost     *dragonboat.NodeHost
	config       Config
	db           *knocidb.DB
	mu           sync.RWMutex
	leaderID     uint64
	stateMachine *KVStateMachine
	state        bool
	Sync         bool // 是否使用同步模式进行操作
}

// NewNodeManager 创建一个新的节点管理器
func NewNodeManager(config Config, db *knocidb.DB) (*NodeManager, error) {
	if db == nil {
		return nil, ErrDBEmpty
	}
	if err := ValidateConfig(&config); err != nil {
		return nil, err
	}

	// 确保Raft数据目录存在
	raftDir := knocidb.DefaultOptions.RaftPath
	if err := os.MkdirAll(raftDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("创建Raft数据目录失败: %w", err)
	}

	// 创建NodeHost配置
	nhc := config.GetNodeHostConfig()

	// 创建NodeHost
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, fmt.Errorf("创建NodeHost失败: %w", err)
	}

	// 创建节点管理器
	nm := &NodeManager{
		nodeHost: nodeHost,
		config:   config,
		db:       db,
		leaderID: 0,
		state:    false,
		Sync:     false, // 默认使用异步模式
	}

	return nm, nil
}

// Start 启动Raft节点
func (nm *NodeManager) Start() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	// 创建状态机工厂函数
	factory := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		sm := NewKVStateMachine(clusterID, nodeID, nm.db)
		nm.stateMachine = sm
		return nil
	}

	// 获取Raft配置
	rc := nm.config.GetRaftConfig()

	// 检查是否需要加入现有集群
	if nm.config.JoinCluster {
		// 加入现有集群
		log.Printf("正在加入现有集群，NodeID: %d, ClusterID: %d\n", nm.config.NodeID, nm.config.ClusterID)
		// 在Dragonboat v4中，StartCluster方法的参数顺序有变化
		if err := nm.nodeHost.StartReplica(
			nm.config.InitialMembers,
			true,
			factory,
			rc,
		); err != nil {
			return fmt.Errorf("加入集群失败: %w", err)
		}
	} else {
		// 启动新集群
		log.Printf("正在启动新集群，NodeID: %d, ClusterID: %d\n", nm.config.NodeID, nm.config.ClusterID)
		if err := nm.nodeHost.StartReplica(
			nm.config.InitialMembers,
			false,
			factory,
			rc,
		); err != nil {
			return fmt.Errorf("启动集群失败: %w", err)
		}
	}

	// 启动Leader检测
	go nm.leaderMonitor()
	nm.state = true
	return nil
}

// Stop 停止Raft节点
func (nm *NodeManager) Stop() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.nodeHost != nil {
		nm.nodeHost.Close()
		nm.nodeHost = nil
	}
	nm.state = false
	return nil
}

// leaderMonitor 监控Leader变化
func (nm *NodeManager) leaderMonitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if nm.nodeHost == nil {
				return
			}

			// 获取Leader信息
			leaderID, _, valid, err := nm.nodeHost.GetLeaderID(nm.config.ClusterID)
			if err != nil || !valid {
				continue
			}

			nm.mu.Lock()
			oldLeaderID := nm.leaderID
			nm.leaderID = leaderID
			nm.mu.Unlock()

			// 如果Leader发生变化，记录日志
			if oldLeaderID != leaderID {
				log.Printf("Leader变更: %d -> %d\n", oldLeaderID, leaderID)
			}
		}
	}
}

// IsLeader 检查当前节点是否是Leader
func (nm *NodeManager) IsLeader() bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	return nm.leaderID == nm.config.NodeID
}

// GetLeaderID 获取当前Leader的NodeID
func (nm *NodeManager) GetLeaderID() (uint64, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.nodeHost == nil || !nm.state {
		return 0, ErrNodeNotStarted
	}

	// 在Dragonboat v4中，GetLeaderID返回三个值：leaderID, valid, error
	leaderID, _, valid, err := nm.nodeHost.GetLeaderID(nm.config.ClusterID)
	if err != nil {
		return 0, err
	}
	if !valid {
		return 0, ErrNotLeader
	}

	return leaderID, nil
}

// Put 通过Raft共识写入键值对
func (nm *NodeManager) Put(key, value []byte) error {
	if nm.nodeHost == nil || !nm.state {
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
	if nm.Sync {
		// 使用同步Propose方法
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := nm.nodeHost.SyncPropose(ctx, nm.nodeHost.GetNoOPSession(nm.config.ClusterID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := nm.nodeHost.Propose(nm.nodeHost.GetNoOPSession(nm.config.ClusterID), data, 5*time.Second)
		if err != nil {
			return err
		}

		// 等待操作完成
		<-rs.ResultC()
		if rs.Completed() {
			return nil
		}

		// 处理错误
		if rs.Timeout() {
			return ErrTimeout
		}
		if rs.Terminated() {
			return ErrCanceled
		}
		return errors.New("提议失败")
	}
}

// Delete 通过Raft共识删除键
func (nm *NodeManager) Delete(key []byte) error {
	if nm.nodeHost == nil || !nm.state {
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
	if nm.Sync {
		// 使用同步Propose方法
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := nm.nodeHost.SyncPropose(ctx, nm.nodeHost.GetNoOPSession(nm.config.ClusterID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := nm.nodeHost.Propose(nm.nodeHost.GetNoOPSession(nm.config.ClusterID), data, 5*time.Second)
		if err != nil {
			return err
		}

		// 等待操作完成
		<-rs.ResultC()
		if rs.Completed() {
			return nil
		}

		// 处理错误
		if rs.Timeout() {
			return ErrTimeout
		}
		if rs.Terminated() {
			return ErrCanceled
		}
		return errors.New("删除操作失败")
	}
}

// Get 通过Raft共识读取键值
func (nm *NodeManager) Get(key []byte) ([]byte, error) {
	if nm.nodeHost == nil || !nm.state {
		return nil, ErrNodeNotStarted
	}
	// 创建查询命令
	cmd := Command{
		Op:  OpGet,
		Key: key,
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	// 根据Sync字段决定使用同步还是异步模式
	if nm.Sync {
		// 使用同步ReadIndex方法
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := nm.nodeHost.SyncRead(ctx, nm.config.ClusterID, data)
		if err != nil {
			return nil, err
		}

		// 处理结果
		value, ok := result.([]byte)
		if !ok {
			return nil, errors.New("无效的查询结果类型")
		}

		return value, nil
	} else {
		// 使用异步ReadIndex方法
		rs, err := nm.nodeHost.ReadIndex(nm.config.ClusterID, 5*time.Second)
		if err != nil {
			return nil, err
		}

		// 等待ReadIndex操作完成
		<-rs.ResultC()
		if !rs.Completed() {
			if rs.Timeout() {
				return nil, ErrTimeout
			}
			if rs.Terminated() {
				return nil, ErrCanceled
			}
			return nil, errors.New("ReadIndex操作失败")
		}

		// 执行本地读取
		result, err := nm.nodeHost.ReadLocalNode(rs, data)
		if err != nil {
			return nil, err
		}

		// 处理结果
		value, ok := result.([]byte)
		if !ok {
			return nil, errors.New("无效的查询结果类型")
		}

		return value, nil
	}
}

// BatchWrite 通过Raft共识批量写入
func (nm *NodeManager) BatchWrite(commands []Command, batchID uint64) error {
	if nm.nodeHost == nil || !nm.state {
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
	if nm.Sync {
		// 使用同步Propose方法
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := nm.nodeHost.SyncPropose(ctx, nm.nodeHost.GetNoOPSession(nm.config.ClusterID), data)
		return err
	} else {
		// 使用异步Propose方法
		rs, err := nm.nodeHost.Propose(nm.nodeHost.GetNoOPSession(nm.config.ClusterID), data, 10*time.Second)
		if err != nil {
			return err
		}

		// 等待操作完成
		<-rs.ResultC()
		if rs.Completed() {
			return nil
		}

		// 处理错误
		if rs.Timeout() {
			return ErrTimeout
		}
		if rs.Terminated() {
			return ErrCanceled
		}
		return errors.New("批量写入操作失败")
	}
}

// GetClusterMembership 获取集群成员信息
func (nm *NodeManager) GetClusterMembership() (map[uint64]string, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.nodeHost == nil || !nm.state {
		return nil, ErrNodeNotStarted
	}

	// 根据Sync字段决定使用同步还是异步模式
	if nm.Sync {
		// 直接使用同步方法获取成员信息
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		membership, err := nm.nodeHost.SyncGetShardMembership(ctx, nm.config.ClusterID)
		if err != nil {
			return nil, err
		}

		return membership.Nodes, nil
	} else {
		// 使用异步ReadIndex方法确保线性一致性，然后获取成员信息
		rs, err := nm.nodeHost.ReadIndex(nm.config.ClusterID, 5*time.Second)
		if err != nil {
			return nil, err
		}

		// 等待ReadIndex操作完成
		<-rs.ResultC()
		if !rs.Completed() {
			if rs.Timeout() {
				return nil, ErrTimeout
			}
			if rs.Terminated() {
				return nil, ErrCanceled
			}
			return nil, errors.New("获取集群成员信息操作失败")
		}

		// 由于Dragonboat v4中没有直接的异步获取成员信息的方法
		// 我们仍然使用同步方法，但已经通过ReadIndex确保了线性一致性
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		membership, err := nm.nodeHost.SyncGetShardMembership(ctx, nm.config.ClusterID)
		if err != nil {
			return nil, err
		}

		return membership.Nodes, nil
	}
}

// AddNode 向集群添加新节点
func (nm *NodeManager) AddNode(nodeID uint64, address string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.nodeHost == nil || !nm.state {
		return ErrNodeNotStarted
	}

	// 检查是否是Leader
	if !nm.IsLeader() {
		return ErrNotLeader
	}

	// 根据Sync字段决定使用同步还是异步模式
	if nm.Sync {
		// 使用同步方法添加节点
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return nm.nodeHost.SyncRequestAddReplica(ctx, nm.config.ClusterID, nodeID, address, 0)
	} else {
		// 使用异步方法添加节点
		rs, err := nm.nodeHost.RequestAddReplica(nm.config.ClusterID, nodeID, address, 0, 30*time.Second)
		if err != nil {
			return err
		}

		// 等待操作完成
		<-rs.ResultC()
		if rs.Completed() {
			return nil
		}

		// 处理错误
		if rs.Timeout() {
			return ErrTimeout
		}
		if rs.Terminated() {
			return ErrCanceled
		}
		return errors.New("添加节点操作失败")
	}
}

// RemoveNode 从集群移除节点
func (nm *NodeManager) RemoveNode(nodeID uint64) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.nodeHost == nil || !nm.state {
		return ErrNodeNotStarted
	}

	// 检查是否是Leader
	if !nm.IsLeader() {
		return ErrNotLeader
	}

	// 根据Sync字段决定使用同步还是异步模式
	if nm.Sync {
		// 使用同步方法移除节点
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return nm.nodeHost.SyncRequestDeleteReplica(ctx, nm.config.ClusterID, nodeID, 0)
	} else {
		// 使用异步方法移除节点
		rs, err := nm.nodeHost.RequestDeleteReplica(nm.config.ClusterID, nodeID, 0, 30*time.Second)
		if err != nil {
			return err
		}

		// 等待操作完成
		<-rs.ResultC()
		if rs.Completed() {
			return nil
		}

		// 处理错误
		if rs.Timeout() {
			return ErrTimeout
		}
		if rs.Terminated() {
			return ErrCanceled
		}
		return errors.New("移除节点操作失败")
	}
}
