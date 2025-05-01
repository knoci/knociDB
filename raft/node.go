package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/statemachine"
)

var (
	ErrNotLeader        = errors.New("节点不是Leader")
	ErrProposalRejected = errors.New("提案被拒绝")
	ErrTimeout          = errors.New("操作超时")
	ErrNodeStopped      = errors.New("节点已停止")
)

// NodeManager 管理Raft节点
type NodeManager struct {
	nh        *dragonboat.NodeHost
	config    Config
	db        DBOperator
	stopped   bool
	mu        sync.RWMutex
	sessions  map[uint64]*client.Session
	sessionMu sync.Mutex
}

// NewNodeManager 创建一个新的NodeManager实例
func NewNodeManager(config Config, db DBOperator) (*NodeManager, error) {
	// 验证配置
	if err := ValidateConfig(&config); err != nil {
		return nil, err
	}

	// 创建NodeManager
	nm := &NodeManager{
		config:   config,
		db:       db,
		stopped:  false,
		sessions: make(map[uint64]*client.Session),
	}

	// 创建NodeHost
	nh, err := dragonboat.NewNodeHost(config.GetNodeHostConfig())
	if err != nil {
		return nil, fmt.Errorf("创建NodeHost失败: %w", err)
	}
	nm.nh = nh

	return nm, nil
}

// Start 启动Raft节点
func (nm *NodeManager) Start() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.stopped {
		return ErrNodeStopped
	}

	// 创建状态机工厂
	factory := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		return NewKVStateMachine(clusterID, nodeID, nm.db)
	}

	// 启动或加入Raft集群
	if nm.config.JoinCluster {
		// 作为新节点加入现有集群
		return nm.nh.StartCluster(
			nm.config.InitialMembers,
			nm.config.JoinCluster,
			factory,
			nm.config.GetRaftConfig(),
		)
	} else {
		// 启动新集群
		return nm.nh.StartCluster(
			nm.config.InitialMembers,
			false,
			factory,
			nm.config.GetRaftConfig(),
		)
	}
}

// Stop 停止Raft节点
func (nm *NodeManager) Stop() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.stopped {
		return nil
	}

	// 关闭所有会话
	nm.sessionMu.Lock()
	for _, session := range nm.sessions {
		if err := nm.nh.CloseSession(context.Background(), session); err != nil {
			// 只记录错误，继续关闭其他会话
			fmt.Printf("关闭会话失败: %v\n", err)
		}
	}
	nm.sessions = make(map[uint64]*client.Session)
	nm.sessionMu.Unlock()

	// 停止NodeHost
	nm.nh.Stop()
	nm.stopped = true
	return nil
}

// getSession 获取或创建一个客户端会话
func (nm *NodeManager) getSession() (*client.Session, error) {
	nm.sessionMu.Lock()
	defer nm.sessionMu.Unlock()

	// 检查是否已有会话
	if session, ok := nm.sessions[nm.config.ClusterID]; ok {
		return session, nil
	}

	// 创建新会话
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nm.nh.GetNewSession(ctx, nm.config.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("创建会话失败: %w", err)
	}

	// 保存会话
	nm.sessions[nm.config.ClusterID] = session
	return session, nil
}

// Put 通过Raft共识写入键值对
func (nm *NodeManager) Put(key, value []byte) error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return ErrNodeStopped
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

	// 获取会话
	session, err := nm.getSession()
	if err != nil {
		return err
	}

	// 提交提案
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = nm.nh.SyncPropose(ctx, session, data)
	if err != nil {
		return fmt.Errorf("提交提案失败: %w", err)
	}

	return nil
}

// Delete 通过Raft共识删除键
func (nm *NodeManager) Delete(key []byte) error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return ErrNodeStopped
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

	// 获取会话
	session, err := nm.getSession()
	if err != nil {
		return err
	}

	// 提交提案
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = nm.nh.SyncPropose(ctx, session, data)
	if err != nil {
		return fmt.Errorf("提交提案失败: %w", err)
	}

	return nil
}

// BatchWrite 通过Raft共识批量写入
func (nm *NodeManager) BatchWrite(commands []Command, batchID uint64) error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return ErrNodeStopped
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

	// 获取会话
	session, err := nm.getSession()
	if err != nil {
		return err
	}

	// 提交提案
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = nm.nh.SyncPropose(ctx, session, data)
	if err != nil {
		return fmt.Errorf("提交批处理提案失败: %w", err)
	}

	return nil
}

// Get 通过Raft共识读取键值
func (nm *NodeManager) Get(key []byte) ([]byte, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return nil, ErrNodeStopped
	}

	// 创建查询命令
	cmd := Command{
		Op:  OpPut, // 查询操作使用OpPut，但在状态机中只会读取
		Key: key,
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	// 提交只读查询
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := nm.nh.SyncRead(ctx, nm.config.ClusterID, data)
	if err != nil {
		return nil, fmt.Errorf("读取数据失败: %w", err)
	}

	// 处理结果
	if result == nil {
		return nil, nil // 键不存在
	}

	value, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("无效的结果类型: %T", result)
	}

	return value, nil
}

// IsLeader 检查当前节点是否是Leader
func (nm *NodeManager) IsLeader() bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return false
	}

	leaderID, valid, err := nm.nh.GetLeaderID(nm.config.ClusterID)
	if err != nil || !valid {
		return false
	}

	return leaderID == nm.config.NodeID
}

// GetLeaderID 获取当前Leader的NodeID
func (nm *NodeManager) GetLeaderID() (uint64, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return 0, ErrNodeStopped
	}

	leaderID, valid, err := nm.nh.GetLeaderID(nm.config.ClusterID)
	if err != nil {
		return 0, err
	}

	if !valid {
		return 0, errors.New("当前没有Leader")
	}

	return leaderID, nil
}

// GetClusterMembership 获取集群成员信息
func (nm *NodeManager) GetClusterMembership() (map[uint64]string, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return nil, ErrNodeStopped
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	membership, err := nm.nh.GetClusterMembership(ctx, nm.config.ClusterID)
	if err != nil {
		return nil, err
	}

	return membership.Nodes, nil
}

// AddNode 向集群添加新节点
func (nm *NodeManager) AddNode(nodeID uint64, address string) error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return ErrNodeStopped
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return nm.nh.RequestAddNode(ctx, nm.config.ClusterID, nodeID, address, 0)
}

// RemoveNode 从集群移除节点
func (nm *NodeManager) RemoveNode(nodeID uint64) error {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if nm.stopped {
		return ErrNodeStopped
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return nm.nh.RequestDeleteNode(ctx, nm.config.ClusterID, nodeID, 0)
}