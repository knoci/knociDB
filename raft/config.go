package raft

import (
	"fmt"
	"path/filepath"

	"github.com/lni/dragonboat/v4/config"
)

// Config 表示Raft节点的配置信息
type Config struct {
	// NodeID 是当前节点的唯一标识符
	NodeID uint64
	// ClusterID 是Raft集群的唯一标识符
	ClusterID uint64
	// RaftAddress 是当前节点的网络地址，格式为 "host:port"
	RaftAddress string
	// DataDir 是存储Raft日志和快照的目录
	DataDir string
	// JoinCluster 表示是否作为新节点加入现有集群
	JoinCluster bool
	// InitialMembers 是初始集群成员的映射，键是NodeID，值是RaftAddress
	InitialMembers map[uint64]string
	// TickMs 是心跳间隔，单位为毫秒
	TickMs uint64
	// ElectionRTTMs 是选举超时，单位为心跳间隔的倍数
	ElectionRTTMs uint64
	// HeartbeatRTTMs 是心跳超时，单位为心跳间隔的倍数
	HeartbeatRTTMs uint64
	// SnapshotIntervalSeconds 是自动创建快照的间隔，单位为秒
	SnapshotIntervalSeconds uint64
	// Sync 指定节点是同步还是异步更新状态
	Sync bool
}

// GetNodeHostConfig 返回Dragonboat节点主机配置
func (c *Config) GetNodeHostConfig() config.NodeHostConfig {
	return config.NodeHostConfig{
		RaftAddress:    c.RaftAddress,
		NodeHostDir:    filepath.Join(c.DataDir, "nodehost"),
		RTTMillisecond: c.TickMs,
		WALDir:         filepath.Join(c.DataDir, "wal"),
		DeploymentID:   1,
	}
}

// GetRaftConfig 返回Raft节点配置
func (c *Config) GetRaftConfig() config.Config {
	return config.Config{
		ReplicaID:               c.NodeID,
		ShardID:                 c.ClusterID,
		ElectionRTT:             c.ElectionRTTMs,
		HeartbeatRTT:            c.HeartbeatRTTMs,
		CheckQuorum:             true,
		SnapshotEntries:         c.SnapshotIntervalSeconds,
		CompactionOverhead:      5,
		SnapshotCompressionType: config.Snappy,
		EntryCompressionType:    config.Snappy,
		DisableAutoCompactions:  false,
		PreVote:                 true,
	}
}

// DefaultConfig 返回默认的Raft配置
func DefaultConfig(nodeID, clusterID uint64, raftAddress, dataDir string) Config {
	return Config{
		NodeID:                  nodeID,
		ClusterID:               clusterID,
		RaftAddress:             raftAddress,
		DataDir:                 dataDir,
		JoinCluster:             false,
		InitialMembers:          make(map[uint64]string),
		TickMs:                  100,
		ElectionRTTMs:           10,
		HeartbeatRTTMs:          1,
		SnapshotIntervalSeconds: 3600, // 默认每小时创建一次快照
		Sync:                    true,
	}
}

// ValidateConfig 验证配置是否有效
func ValidateConfig(cfg *Config) error {
	if cfg.NodeID == 0 {
		return fmt.Errorf("NodeID can not be0")
	}
	if cfg.ClusterID == 0 {
		return fmt.Errorf("ClusterID can not be 0")
	}
	if cfg.RaftAddress == "" {
		return fmt.Errorf("RaftAddress can not be nil")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("DataDir can not be nil")
	}
	if !cfg.JoinCluster && len(cfg.InitialMembers) == 0 {
		return fmt.Errorf("InitialMembers should have at least one member")
	}
	if cfg.ElectionRTTMs == 0 {
		return fmt.Errorf("ElectionRTTMs can not be 0")
	}
	if cfg.ElectionRTTMs <= 2*cfg.HeartbeatRTTMs {
		return fmt.Errorf("ElectionRTTMs must greater than 2*HeartbeatRTTMs")
	}
	if cfg.ElectionRTTMs < 10*cfg.HeartbeatRTTMs {
		fmt.Printf("warnning: ElectionRTTMs support to greater than 10*HeartbeatRTTMs\n")
	}
	return nil
}
