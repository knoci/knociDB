package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"knocidb"
)

var (
	dirPath      = flag.String("dir", "./examples/cmd_client/data", "database dir path")
	partitionNum = flag.Int("partitions", 3, "partition nums")
	distributed  = flag.Bool("distributed", false, "use distributed")
	nodeID       = flag.Uint64("id", 1, "node id")
	listenAddr   = flag.String("listen", "127.0.0.1:8001", "http listen")
	peers        = flag.String("peers", "127.0.0.1:8001", "peers list")
	join         = flag.Bool("join", false, "join peers")
)

type Client struct {
	db           *knocidb.DB
	distributeDB *knocidb.RaftDB
	isDistribute bool
	batch        *BatchClient // 批处理客户端
}

func NewClient(isDistribute bool) (*Client, error) {
	if isDistribute {
		// 创建分布式数据库目录
		dbDir := filepath.Join(*dirPath, fmt.Sprintf("%d", *nodeID), "db")
		raftDir := filepath.Join(*dirPath, fmt.Sprintf("%d", *nodeID), "raft")

		if err := os.MkdirAll(dbDir, 0750); err != nil {
			return nil, fmt.Errorf("create dir failed: %v", err)
		}
		if err := os.MkdirAll(raftDir, 0750); err != nil {
			return nil, fmt.Errorf("create raft dir failed: %v", err)
		}

		// 解析peers列表
		peerList := strings.Split(*peers, ",")
		initialMembers := make(map[uint64]string)

		// 确保当前节点ID在initialMembers中有对应的地址
		// 如果peers列表只有一个元素，直接将其映射到当前节点ID
		if !*join && len(peerList) == 1 && *nodeID > 0 {
			initialMembers[*nodeID] = peerList[0]
		} else if !*join {
			// 否则按照索引顺序映射
			for i, peer := range peerList {
				if peer != "" {
					// 节点ID从1开始递增
					nodeID := uint64(i + 1)
					initialMembers[nodeID] = peer
				}
			}
		}

		// 创建Raft配置
		cfg := knocidb.RaftOptions{
			NodeID:                  *nodeID,
			ClusterID:               1, // 使用固定的集群ID
			RaftAddress:             *listenAddr,
			DataDir:                 raftDir,
			JoinCluster:             *join,
			InitialMembers:          initialMembers,
			TickMs:                  100,
			ElectionRTTMs:           10,
			HeartbeatRTTMs:          1,
			SnapshotIntervalSeconds: 3600, // 默认每小时创建一次快照
			Sync:                    true,
		}

		// 创建KV存储选项
		dbOpts := knocidb.DefaultOptions
		dbOpts.DirPath = dbDir
		dbOpts.PartitionNum = *partitionNum
		dbOpts.RaftPath = raftDir

		// 创建分布式数据库
		distributeDB, err := knocidb.OpenRaft(cfg, dbOpts)
		if err != nil {
			return nil, fmt.Errorf("open raft database failed: %v", err)
		}

		client := &Client{distributeDB: distributeDB, isDistribute: true}
		client.batch = NewBatchClient(client)
		return client, nil
	} else {
		// 创建数据目录
		if err := os.MkdirAll(*dirPath, 0750); err != nil {
			return nil, fmt.Errorf("create dir failed: %v", err)
		}

		// 创建KV存储选项
		dbOpts := knocidb.DefaultOptions
		dbOpts.DirPath = *dirPath
		dbOpts.PartitionNum = *partitionNum

		// 打开数据库
		db, err := knocidb.Open(dbOpts)
		if err != nil {
			return nil, fmt.Errorf("open database failed: %v", err)
		}

		client := &Client{db: db, isDistribute: false}
		client.batch = NewBatchClient(client)
		return client, nil
	}
}

func (c *Client) Close() error {
	if c.isDistribute {
		// 先关闭分布式数据库
		if err := c.distributeDB.Close(); err != nil {
			return fmt.Errorf("close raft database failed: %v", err)
		}
		// 再关闭底层数据库
		return c.db.Close()
	} else {
		return c.db.Close()
	}
}

func (c *Client) ExecuteCommand(cmd string, args []string) (string, error) {
	// 检查是否在批处理模式中
	if c.batch.IsInBatch() && cmd != "COMMIT" && cmd != "DISCARD" {
		// 在批处理模式中，将命令加入队列
		return c.batch.Queue(cmd, args)
	}

	switch strings.ToUpper(cmd) {
	case "GET":
		if len(args) != 1 {
			return "", fmt.Errorf("GET require one arg: GET key")
		}
		key := []byte(args[0])
		var value []byte
		var err error

		if c.isDistribute {
			value, err = c.distributeDB.Get(key)
		} else {
			value, err = c.db.Get(key)
		}

		if err != nil {
			if err == knocidb.ErrKeyNotFound {
				return "(nil)", nil
			}
			return "", err
		}
		return fmt.Sprintf("\"%s\"", string(value)), nil

	case "PUT":
		if len(args) != 2 {
			return "", fmt.Errorf("PUT require two args: PUT key value")
		}
		key := []byte(args[0])
		value := []byte(args[1])
		var err error

		if c.isDistribute {
			err = c.distributeDB.Put(key, value)
		} else {
			err = c.db.Put(key, value)
		}

		if err != nil {
			return "", err
		}
		return "OK", nil

	case "DEL":
		if len(args) < 1 {
			return "", fmt.Errorf("DEL require at least one arg: DEL key [key ...]")
		}
		deleted := 0
		for _, arg := range args {
			key := []byte(arg)
			var err error

			if c.isDistribute {
				err = c.distributeDB.Delete(key)
			} else {
				err = c.db.Delete(key)
			}

			if err == nil {
				deleted++
			}
		}
		return fmt.Sprintf("(integer) %d", deleted), nil

	case "EXISTS":
		if len(args) < 1 {
			return "", fmt.Errorf("EXISTS require at least one arg: EXISTS key [key ...]")
		}
		exists := 0
		for _, arg := range args {
			key := []byte(arg)
			var exist bool
			var err error

			if c.isDistribute {
				// 分布式模式下通过Get操作检查键是否存在
				_, err = c.distributeDB.Get(key)
				exist = (err == nil)
			} else {
				exist, err = c.db.Exist(key)
			}

			if err == nil && exist {
				exists++
			}
		}
		return fmt.Sprintf("(integer) %d", exists), nil

	case "BEGIN":
		return c.batch.Begin()

	case "COMMIT":
		return c.batch.Commit()

	case "DISCARD":
		return c.batch.Discard()

	case "PING":
		if len(args) == 0 {
			return "PONG", nil
		}
		return fmt.Sprintf("\"%s\"", args[0]), nil

	case "ECHO":
		if len(args) != 1 {
			return "", fmt.Errorf("ECHO need at least one arg: ECHO message")
		}
		return fmt.Sprintf("\"%s\"", args[0]), nil

	case "INFO":
		info := "# knocidb \nmode " + func() string {
			if c.isDistribute {
				return "distributed"
			}
			return "standalone"
		}() + "\n\n# Data\ndb_path " + *dirPath + "\npartitions " + fmt.Sprintf("%d", *partitionNum)

		// 添加分布式模式下的额外信息
		if c.isDistribute {
			// 获取节点信息
			isLeader := c.distributeDB.IsLeader()
			leaderStatus := "no"
			if isLeader {
				leaderStatus = "yes"
			}

			// 获取Leader ID
			leaderID, err := c.distributeDB.GetLeaderID()
			leaderInfo := "unknown"
			if err == nil {
				leaderInfo = fmt.Sprintf("%d", leaderID)
			}

			// 获取集群成员信息
			members, err := c.distributeDB.GetClusterMembership()
			memberInfo := ""
			if err == nil {
				for id, addr := range members {
					memberInfo += fmt.Sprintf("\n  %d: %s", id, addr)
				}
			}

			info += fmt.Sprintf("\n\n# Cluster\nnode_id %d\nis_leader %s\nleader_id %s\nraft_address %s\nmembers%s",
				*nodeID, leaderStatus, leaderInfo, *listenAddr, memberInfo)
		}

		return info, nil

	case "CLUSTER":
		if !c.isDistribute {
			return "", fmt.Errorf("CLUSTER command only available in distributed mode")
		}

		if len(args) < 1 {
			return "", fmt.Errorf("CLUSTER requires subcommand: CLUSTER [INFO|ADD|REMOVE|STATUS]")
		}

		subCmd := strings.ToUpper(args[0])
		switch subCmd {
		case "INFO":
			// 获取集群成员信息
			members, err := c.distributeDB.GetClusterMembership()
			if err != nil {
				return "", fmt.Errorf("failed to get cluster membership: %v", err)
			}

			result := fmt.Sprintf("Cluster has %d nodes:\n", len(members))
			for id, addr := range members {
				result += fmt.Sprintf("Node %d: %s\n", id, addr)
			}

			// 获取Leader信息
			leaderID, err := c.distributeDB.GetLeaderID()
			if err == nil {
				result += fmt.Sprintf("Current leader: Node %d\n", leaderID)
			}

			return result, nil

		case "STATUS":
			// 获取当前节点状态
			isLeader := c.distributeDB.IsLeader()
			leaderStatus := "Follower"
			if isLeader {
				leaderStatus = "Leader"
			}

			// 获取Leader ID
			leaderID, err := c.distributeDB.GetLeaderID()
			leaderInfo := "unknown"
			if err == nil {
				leaderInfo = fmt.Sprintf("%d", leaderID)
			}

			result := fmt.Sprintf("Node Status:\n")
			result += fmt.Sprintf("Current Node ID: %d\n", *nodeID)
			result += fmt.Sprintf("Role: %s\n", leaderStatus)
			result += fmt.Sprintf("Current Leader: Node %s\n", leaderInfo)
			result += fmt.Sprintf("Listen Address: %s\n", *listenAddr)

			return result, nil

		case "ADD":
			if len(args) != 3 {
				return "", fmt.Errorf("CLUSTER ADD requires node ID and address: CLUSTER ADD <node_id> <address>")
			}

			// 解析节点ID
			nodeID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return "", fmt.Errorf("invalid node ID: %v", err)
			}

			// 添加节点
			if err := c.distributeDB.AddNode(nodeID, args[2]); err != nil {
				return "", fmt.Errorf("failed to add node: %v", err)
			}

			return fmt.Sprintf("Node %d added to cluster", nodeID), nil

		case "REMOVE":
			if len(args) != 2 {
				return "", fmt.Errorf("CLUSTER REMOVE requires node ID: CLUSTER REMOVE <node_id>")
			}

			// 解析节点ID
			nodeID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return "", fmt.Errorf("invalid node ID: %v", err)
			}

			// 移除节点
			if err := c.distributeDB.RemoveNode(nodeID); err != nil {
				return "", fmt.Errorf("failed to remove node: %v", err)
			}

			return fmt.Sprintf("Node %d removed from cluster", nodeID), nil

		default:
			return "", fmt.Errorf("unknown CLUSTER subcommand: %s", subCmd)
		}

	case "HELP":
		baseHelp := `cmd:
GET key 				to retrieve the value of the specified key
PUT key value 			Set the value of the key
DEL key [key...] 		Delete one or more keys
EXISTS key [key...] 	Check if one or more keys exist
BEGIN 					initiates a transaction
COMMIT 					executes all commands in the transaction
DISCARD 				discards all commands in the transaction
PING 		 			test connection, return PONG or specified message
ECHO message 			returns the same message
INFO 					displays information
HELP 					displays help information
EXIT					exits the cmd_client (Ctrl+C also works)`

		if c.isDistribute {
			baseHelp += `

Distributed mode commands:
CLUSTER INFO 			display cluster membership information
CLUSTER STATUS 			display current node status
CLUSTER ADD <id> <addr> 	add a new node to the cluster
CLUSTER REMOVE <id> 	remove a node from the cluster`
		}

		return baseHelp, nil

	default:
		return "", fmt.Errorf("unknow cmd: %s", cmd)
	}
}

func parseCommand(line string) (string, []string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", nil
	}

	cmd := parts[0]
	args := parts[1:]

	return cmd, args
}

func main() {
	flag.Parse()

	// 创建客户端
	client, err := NewClient(*distributed)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	// 创建一个上下文，用于处理信号
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 在后台监听信号
	go func() {
		<-sigCh
		ctx.Done()
		cancel() // 取消上下文
		close(sigCh)
	}()

	fmt.Println("KnociDB")
	fmt.Println("use 'HELP' to get help，'EXIT' to exit the cmd_client")

	// 创建命令行读取器
	reader := bufio.NewReader(os.Stdin)

	// 命令行循环
	running := true
	for running {
		select {
		case <-ctx.Done():
			// 收到退出信号
			running = false
			break
		default:
			fmt.Print("knocidb> ")
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("error scan %v\n", err)
				continue
			}

			// 去除行尾的换行符
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			// 检查是否退出
			if strings.ToUpper(line) == "EXIT" {
				running = false
				break
			}

			// 解析命令和参数
			cmd, args := parseCommand(line)
			if cmd == "" {
				continue
			}

			// 执行命令
			result, err := client.ExecuteCommand(cmd, args)
			if err != nil {
				fmt.Printf("(error) %v\n", err)
			} else {
				fmt.Println(result)
			}
		}
	}
	// 关闭客户端
	fmt.Println("Closing database connections...")
	err = client.Close()
	if err != nil {
		fmt.Printf("Error during shutdown: %v\n", err)
	}

	fmt.Println("see you next time~")
}
