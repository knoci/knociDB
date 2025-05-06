package raft

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"knocidb"
)

// 这个示例展示了如何创建一个三节点的Raft集群
// 每个节点都有自己的数据库实例，但它们通过Raft共识保持数据一致性

// StartRaftNode 启动一个Raft节点
func StartRaftNode(nodeID, clusterID uint64, raftAddress, dataDir string, initialMembers map[uint64]string, joinCluster bool) (*RaftDB, error) {
	// 创建数据库目录
	dbPath := filepath.Join(dataDir, fmt.Sprintf("node-%d", nodeID))
	if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("创建数据库目录失败: %w", err)
	}

	// 打开数据库
	db, err := knocidb.Open(knocidb.DefaultOptions)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}

	// 创建Raft配置
	config := Config{
		NodeID:                  nodeID,
		ClusterID:               clusterID,
		RaftAddress:             raftAddress,
		DataDir:                 dataDir,
		JoinCluster:             joinCluster,
		InitialMembers:          initialMembers,
		TickMs:                  100,
		ElectionRTTMs:           10,
		HeartbeatRTTMs:          1,
		SnapshotIntervalSeconds: 3600,
	}

	// 打开RaftDB
	raftDB, err := OpenRaftDB(db, config)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("打开RaftDB失败: %w", err)
	}

	return raftDB, nil
}

// ExampleThreeNodeCluster 展示如何创建和使用一个三节点的Raft集群
func ExampleThreeNodeCluster() {
	// 集群配置
	clusterID := uint64(1)
	initialMembers := map[uint64]string{
		1: "localhost:10001",
		2: "localhost:10002",
		3: "localhost:10003",
	}

	// 创建临时数据目录
	tmpDir := filepath.Join(os.TempDir(), "raftdb-example")
	_ = os.RemoveAll(tmpDir) // 清理旧数据
	_ = os.MkdirAll(tmpDir, os.ModePerm)

	// 启动三个节点
	node1, err := StartRaftNode(1, clusterID, initialMembers[1], filepath.Join(tmpDir, "node1"), initialMembers, false)
	if err != nil {
		log.Fatalf("启动节点1失败: %v", err)
	}
	defer node1.Close()

	node2, err := StartRaftNode(2, clusterID, initialMembers[2], filepath.Join(tmpDir, "node2"), initialMembers, false)
	if err != nil {
		log.Fatalf("启动节点2失败: %v", err)
	}
	defer node2.Close()

	node3, err := StartRaftNode(3, clusterID, initialMembers[3], filepath.Join(tmpDir, "node3"), initialMembers, false)
	if err != nil {
		log.Fatalf("启动节点3失败: %v", err)
	}
	defer node3.Close()

	// 等待集群选举完成
	time.Sleep(5 * time.Second)

	// 找到Leader节点
	var leaderNode *RaftDB
	if node1.IsLeader() {
		leaderNode = node1
		fmt.Println("节点1是Leader")
	} else if node2.IsLeader() {
		leaderNode = node2
		fmt.Println("节点2是Leader")
	} else if node3.IsLeader() {
		leaderNode = node3
		fmt.Println("节点3是Leader")
	} else {
		log.Fatal("没有找到Leader节点")
	}

	// 通过Leader写入数据
	key := []byte("test-key")
	value := []byte("test-value")
	if err := leaderNode.Put(key, value); err != nil {
		log.Fatalf("写入数据失败: %v", err)
	}
	fmt.Println("通过Leader写入数据成功")

	// 等待数据同步
	time.Sleep(1 * time.Second)

	// 从所有节点读取数据
	readFromNode := func(node *RaftDB, nodeID int) {
		val, err := node.Get(key)
		if err != nil {
			log.Fatalf("从节点%d读取数据失败: %v", nodeID, err)
		}
		fmt.Printf("从节点%d读取数据: %s\n", nodeID, string(val))
	}

	readFromNode(node1, 1)
	readFromNode(node2, 2)
	readFromNode(node3, 3)

	// 删除数据
	if err := leaderNode.Delete(key); err != nil {
		log.Fatalf("删除数据失败: %v", err)
	}
	fmt.Println("通过Leader删除数据成功")

	// 等待数据同步
	time.Sleep(1 * time.Second)

	// 再次从所有节点读取数据
	for i, node := range []*RaftDB{node1, node2, node3} {
		val, err := node.Get(key)
		if err != nil {
			fmt.Printf("从节点%d读取数据失败: %v\n", i+1, err)
		} else if val == nil {
			fmt.Printf("从节点%d读取数据: 键不存在\n", i+1)
		} else {
			fmt.Printf("从节点%d读取数据: %s\n", i+1, string(val))
		}
	}
}
