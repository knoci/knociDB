package main

import (
	"flag"
	"fmt"
	"github.com/knoci/knocidb"
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
	nodeID         = flag.Uint64("id", 1, "Node ID")
	clusterID      = flag.Uint64("cluster", defaultClusterID, "Cluster ID")
	clusterAddress = flag.String("addr", "localhost:8001", "Cluster Raft address")
	rpcAddress     = flag.String("rpc", "", "Internal forwarding RPC address")
	rpcSecret      = flag.String("rpc-secret", "", "Shared secret for forwarding RPC")
	dataDir        = flag.String("dir", "./node", "Data directory")
	join           = flag.Bool("join", false, "Join existing cluster")
	peersStr       = flag.String("peers", "", "Cluster members, format: id1=host1:port1,id2=host2:port2")
	rpcPeersStr    = flag.String("rpc-peers", "", "Forwarding RPC peers, format: id1=host1:port1,id2=host2:port2")
	sync           = flag.Bool("sync", true, "Use synchronous mode")
	interactive    = flag.Bool("interactive", true, "Enable interactive mode")
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
func handleCommand(clusterDB *knocidb.ClusterDB) {
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
			err := clusterDB.Put([]byte(key), []byte(value))
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
			val, err := clusterDB.Get([]byte(key))
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
			err := clusterDB.Delete([]byte(key))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Success")
			}

		case "leader":
			if clusterDB.IsLeader() {
				fmt.Println("Current node is Leader")
			} else {
				leaderID, err := clusterDB.GetLeaderID()
				if err != nil {
					fmt.Printf("Failed to get Leader: %v\n", err)
				} else {
					fmt.Printf("Current Leader node ID: %d\n", leaderID)
				}
			}

		case "members":
			members, err := clusterDB.GetClusterMembership()
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
			err = clusterDB.AddNode(nodeID, value)
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
			err = clusterDB.RemoveNode(nodeID)
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
	rpcPeers, err := parsePeers(*rpcPeersStr)
	if err != nil {
		log.Fatalf("Failed to parse rpc peers: %v", err)
	}

	// 如果peers为空，添加自身
	if len(peers) == 0 {
		peers[*nodeID] = *clusterAddress
	}

	// 创建DB配置
	options := knocidb.DefaultOptions
	options.DirPath = nodeDir
	options.ClusterPath = filepath.Join(nodeDir, "cluster")
	options.SnapshotPath = filepath.Join(nodeDir, "snapshot")

	// 创建Cluster配置
	config := knocidb.DefaultClusterOptions(*nodeID, *clusterID, *clusterAddress, nodeDir)
	config.JoinCluster = *join
	config.InitialMembers = peers
	config.RPCAddress = *rpcAddress
	config.PeerRPC = rpcPeers
	config.RPCSecret = *rpcSecret
	config.Sync = *sync

	// 打开ClusterDB
	clusterDB, err := knocidb.OpenCluster(config, options)
	if err != nil {
		log.Fatalf("Failed to start Cluster node: %v", err)
	}
	defer clusterDB.Close()

	// 打印节点信息
	fmt.Printf("KnociDB Cluster node started:\n")
	fmt.Printf("  Node ID: %d\n", *nodeID)
	fmt.Printf("  Cluster ID: %d\n", *clusterID)
	fmt.Printf("  Raft address: %s\n", *clusterAddress)
	fmt.Printf("  RPC address: %s\n", *rpcAddress)
	fmt.Printf("  Data directory: %s\n", nodeDir)
	fmt.Printf("  Sync mode: %v\n", *sync)
	fmt.Printf("  Cluster members: %v\n", peers)

	// 设置信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 如果启用交互模式，启动命令处理
	if *interactive {
		go handleCommand(clusterDB)
	}

	// 等待信号
	<-sigCh
	fmt.Println("\nShutting down node...")
}
