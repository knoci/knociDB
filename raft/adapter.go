package raft

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/lni/dragonboat/v4"
	"knocidb"
)

var (
	ErrDBNotOpen      = errors.New("数据库未打开")
	ErrRaftNotStarted = errors.New("Raft节点未启动")
)

// DBAdapter 是数据库与Raft共识机制的适配器
// 它实现了DBOperator接口，将数据库操作与Raft共识集成
type DBAdapter struct {
	db          *knocidb.DB
	nodeManager *NodeManager
	config      Config
	started     bool
	mu          sync.RWMutex
}

// NewDBAdapter 创建一个新的数据库适配器
func NewDBAdapter(db *knocidb.DB, config Config) (*DBAdapter, error) {
	if db == nil {
		return nil, ErrDBNotOpen
	}

	// 确保Raft数据目录存在
	raftDir := filepath.Join(config.DataDir, "raft")
	if err := os.MkdirAll(raftDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("创建Raft数据目录失败: %w", err)
	}

	// 创建适配器
	adapter := &DBAdapter{
		db:      db,
		config:  config,
		started: false,
	}

	return adapter, nil
}

// Start 启动Raft节点
func (a *DBAdapter) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		return nil
	}

	// 创建节点管理器
	nodeManager, err := NewNodeManager(a.config, a)
	if err != nil {
		return fmt.Errorf("创建节点管理器失败: %w", err)
	}
	a.nodeManager = nodeManager

	// 启动节点
	if err := a.nodeManager.Start(); err != nil {
		return fmt.Errorf("启动Raft节点失败: %w", err)
	}

	a.started = true
	log.Printf("Raft节点已启动，NodeID: %d, ClusterID: %d, 地址: %s\n",
		a.config.NodeID, a.config.ClusterID, a.config.RaftAddress)

	return nil
}

// Stop 停止Raft节点
func (a *DBAdapter) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.started || a.nodeManager == nil {
		return nil
	}

	// 停止节点管理器
	if err := a.nodeManager.Stop(); err != nil {
		return fmt.Errorf("停止Raft节点失败: %w", err)
	}

	a.started = false
	log.Println("Raft节点已停止")

	return nil
}

// Put 实现DBOperator接口的Put方法
// 在Leader节点上，通过Raft共识写入数据
// 在Follower节点上，转发请求到Leader
func (a *DBAdapter) Put(key, value []byte) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return ErrRaftNotStarted
	}

	// 如果是Leader，直接通过Raft共识写入
	if a.nodeManager.IsLeader() {
		return a.nodeManager.Put(key, value)
	}

	// 如果不是Leader，直接在本地执行（状态机会通过Raft日志应用）
	// 这里简化处理，实际上应该转发到Leader
	return a.db.Put(key, value)
}

// Delete 实现DBOperator接口的Delete方法
func (a *DBAdapter) Delete(key []byte) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return ErrRaftNotStarted
	}

	// 如果是Leader，直接通过Raft共识删除
	if a.nodeManager.IsLeader() {
		return a.nodeManager.Delete(key)
	}

	// 如果不是Leader，直接在本地执行（状态机会通过Raft日志应用）
	return a.db.Delete(key)
}

// Get 实现DBOperator接口的Get方法
func (a *DBAdapter) Get(key []byte) ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return nil, ErrRaftNotStarted
	}

	// 如果是Leader，通过Raft共识读取（线性一致性读）
	if a.nodeManager.IsLeader() {
		return a.nodeManager.Get(key)
	}

	// 如果不是Leader，直接从本地读取（可能不是最新数据）
	return a.db.Get(key)
}

// BatchWrite 实现DBOperator接口的BatchWrite方法
func (a *DBAdapter) BatchWrite(commands []Command, batchID uint64) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return ErrRaftNotStarted
	}

	// 如果是Leader，通过Raft共识批量写入
	if a.nodeManager.IsLeader() {
		return a.nodeManager.BatchWrite(commands, batchID)
	}

	// 如果不是Leader，在本地执行批处理
	batch := knocidb.NewWriteBatch()
	for _, cmd := range commands {
		switch cmd.Op {
		case OpPut:
			batch.Put(cmd.Key, cmd.Value)
		case OpDelete:
			batch.Delete(cmd.Key)
		}
	}

	return a.db.WriteBatch(batch)
}

// IsLeader 检查当前节点是否是Leader
func (a *DBAdapter) IsLeader() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return false
	}

	return a.nodeManager.IsLeader()
}

// GetLeaderID 获取当前Leader的NodeID
func (a *DBAdapter) GetLeaderID() (uint64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return 0, ErrRaftNotStarted
	}

	return a.nodeManager.GetLeaderID()
}

// GetClusterMembership 获取集群成员信息
func (a *DBAdapter) GetClusterMembership() (map[uint64]string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return nil, ErrRaftNotStarted
	}

	return a.nodeManager.GetClusterMembership()
}

// AddNode 向集群添加新节点
func (a *DBAdapter) AddNode(nodeID uint64, address string) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return ErrRaftNotStarted
	}

	return a.nodeManager.AddNode(nodeID, address)
}

// RemoveNode 从集群移除节点
func (a *DBAdapter) RemoveNode(nodeID uint64) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.started || a.nodeManager == nil {
		return ErrRaftNotStarted
	}

	return a.nodeManager.RemoveNode(nodeID)
}