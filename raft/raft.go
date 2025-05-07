package raft

import (
	"fmt"
	"knocidb"
	"log"
	"os"
)

// RaftDB 是支持Raft共识的分布式数据库
type RaftDB struct {
	node *NodeManager
}

// OpenRaftDB 打开一个支持Raft共识的分布式数据库
func OpenRaftDB(db *knocidb.DB, config Config) (*RaftDB, error) {
	if db == nil {
		return nil, fmt.Errorf("database is necessary")
	}

	// 验证配置
	if err := ValidateConfig(&config); err != nil {
		return nil, err
	}

	// 确保Raft数据目录存在
	raftDir := db.GetOptions().RaftPath
	if err := os.MkdirAll(raftDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("创建Raft数据目录失败: %w", err)
	}

	// 创建适配器
	node, err := NewNodeManager(config, db)
	if err != nil {
		return nil, err
	}

	// 创建RaftDB
	raftDB := &RaftDB{
		node: node,
	}

	// 启动Raft节点
	if err := node.Start(); err != nil {
		return nil, err
	}

	log.Printf("RaftDB set up, NodeID: %d, ClusterID: %d, Address: %s\n",
		config.NodeID, config.ClusterID, config.RaftAddress)

	return raftDB, nil
}

// Close 关闭RaftDB
func (r *RaftDB) Close() error {
	if r.node != nil {
		if err := r.node.Stop(); err != nil {
			return err
		}
	}
	return nil
}

// Put 写入键值对
func (r *RaftDB) Put(key, value []byte) error {
	return r.node.Put(key, value)
}

// Delete 删除键
func (r *RaftDB) Delete(key []byte) error {
	return r.node.Delete(key)
}

// Get 读取键值
func (r *RaftDB) Get(key []byte) ([]byte, error) {
	return r.node.Get(key)
}

// IsLeader 检查当前节点是否是Leader
func (r *RaftDB) IsLeader() bool {
	return r.node.IsLeader()
}

// GetLeaderID 获取当前Leader的NodeID
func (r *RaftDB) GetLeaderID() (uint64, error) {
	return r.node.GetLeaderID()
}

// GetClusterMembership 获取集群成员信息
func (r *RaftDB) GetClusterMembership() (map[uint64]string, error) {
	return r.node.GetClusterMembership()
}

// AddNode 向集群添加新节点
func (r *RaftDB) AddNode(nodeID uint64, address string) error {
	return r.node.AddNode(nodeID, address)
}

// RemoveNode 从集群移除节点
func (r *RaftDB) RemoveNode(nodeID uint64) error {
	return r.node.RemoveNode(nodeID)
}
