package main

import (
	"flag"
	"fmt"
	"knocidb"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const (
	defaultClusterID uint64 = 1
)

// 命令行参数
var (
	nodeID      = flag.Uint64("id", 1, "Node ID")
	clusterID   = flag.Uint64("cluster", defaultClusterID, "Cluster ID")
	raftAddress = flag.String("addr", "localhost:8001", "Raft node address")
	dataDir     = flag.String("dir", "./node", "Data directory")
	join        = flag.Bool("join", false, "Join existing cluster")
	peersStr    = flag.String("peers", "", "Cluster members list, format: id1=host1:port1,id2=host2:port2")
	sync        = flag.Bool("sync", true, "Use synchronous mode")
	interactive = flag.Bool("interactive", true, "Enable interactive mode")
)

// 解析peers字符串为map
func parsePeers(peersStr string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	if peersStr == "" {
		return peers, nil
	}

	parts := strings.Split(peersStr, ",")
	for _, part := range parts {
		kv := strings.Split(part, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("Invalid peers format: %s", part)
		}

		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Invalid node ID: %s", kv[0])
		}

		peers[id] = kv[1]
	}

	return peers, nil
}

// 处理用户输入命令
func handleCommand(raftDB *knocidb.RaftDB) {
	for {
		fmt.Print("KnociDB> ")
		var cmd, key, value string
		fmt.Scanf("%s %s %s", &cmd, &key, &value)

		switch strings.ToLower(cmd) {
		case "put":
			if key == "" {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			err := raftDB.Put([]byte(key), []byte(value))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Success")
			}

		case "get":
			if key == "" {
				fmt.Println("Usage: get <key>")
				continue
			}
			val, err := raftDB.Get([]byte(key))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("%s\n", string(val))
			}

		case "delete":
			if key == "" {
				fmt.Println("Usage: delete <key>")
				continue
			}
			err := raftDB.Delete([]byte(key))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Success")
			}

		case "leader":
			if raftDB.IsLeader() {
				fmt.Println("Current node is Leader")
			} else {
				leaderID, err := raftDB.GetLeaderID()
				if err != nil {
					fmt.Printf("Failed to get Leader: %v\n", err)
				} else {
					fmt.Printf("Current Leader node ID: %d\n", leaderID)
				}
			}

		case "members":
			members, err := raftDB.GetClusterMembership()
			if err != nil {
				fmt.Printf("Failed to get cluster members: %v\n", err)
			} else {
				fmt.Println("Cluster members:")
				for id, addr := range members {
					fmt.Printf("  Node %d: %s\n", id, addr)
				}
			}

		case "add":
			if key == "" || value == "" {
				fmt.Println("Usage: add <nodeID> <address>")
				continue
			}
			nodeID, err := strconv.ParseUint(key, 10, 64)
			if err != nil {
				fmt.Printf("Invalid node ID: %v\n", err)
				continue
			}
			err = raftDB.AddNode(nodeID, value)
			if err != nil {
				fmt.Printf("Failed to add node: %v\n", err)
			} else {
				fmt.Println("Node added successfully")
			}

		case "remove":
			if key == "" {
				fmt.Println("Usage: remove <nodeID>")
				continue
			}
			nodeID, err := strconv.ParseUint(key, 10, 64)
			if err != nil {
				fmt.Printf("Invalid node ID: %v\n", err)
				continue
			}
			err = raftDB.RemoveNode(nodeID)
			if err != nil {
				fmt.Printf("Failed to remove node: %v\n", err)
			} else {
				fmt.Println("Node removed successfully")
			}

		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  put <key> <value>  - Write key-value pair")
			fmt.Println("  get <key>         - Read key value")
			fmt.Println("  delete <key>      - Delete key-value pair")
			fmt.Println("  leader            - Show current Leader info")
			fmt.Println("  members           - Show cluster members")
			fmt.Println("  add <id> <addr>   - Add new node")
			fmt.Println("  remove <id>       - Remove node")
			fmt.Println("  exit              - Exit program")

		case "exit":
			return

		default:
			fmt.Println("Unknown command, type 'help' for assistance")
		}
	}
}

func main() {
	// 解析命令行参数
	flag.Parse()

	// 创建数据目录
	nodeDir := filepath.Join(*dataDir, fmt.Sprintf("%d", *nodeID))
	if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// 解析peers
	peers, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("Failed to parse peers: %v", err)
	}

	// 如果peers为空，添加自身
	if len(peers) == 0 {
		peers[*nodeID] = *raftAddress
	}

	// 创建DB配置
	options := knocidb.DefaultOptions
	options.DirPath = nodeDir
	options.RaftPath = filepath.Join(nodeDir, "raft")
	options.SnapshotPath = filepath.Join(nodeDir, "snapshot")

	// 创建Raft配置
	config := knocidb.DefaultRaftOptions(*nodeID, *clusterID, *raftAddress, nodeDir)
	config.JoinCluster = *join
	config.InitialMembers = peers
	config.Sync = *sync

	// 打开RaftDB
	raftDB, err := knocidb.OpenRaft(config, options)
	if err != nil {
		log.Fatalf("Failed to start Raft node: %v", err)
	}
	defer raftDB.Close()

	// 打印节点信息
	fmt.Printf("KnociDB Raft node started:\n")
	fmt.Printf("  Node ID: %d\n", *nodeID)
	fmt.Printf("  Cluster ID: %d\n", *clusterID)
	fmt.Printf("  Address: %s\n", *raftAddress)
	fmt.Printf("  Data directory: %s\n", nodeDir)
	fmt.Printf("  Sync mode: %v\n", *sync)
	fmt.Printf("  Cluster members: %v\n", peers)

	// 设置信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 如果启用交互模式，启动命令处理
	if *interactive {
		go handleCommand(raftDB)
	}

	// 等待信号
	<-sigCh
	fmt.Println("\nShutting down node...")
}
