# KnociDB Raft 分布式实现

本模块基于 Dragonboat v4 实现了 KnociDB 的分布式共识机制，使用 Raft 协议确保数据在多个节点间的一致性。

## 功能特性

- 基于 Dragonboat v4 的 Raft 共识实现
- 支持多节点集群配置
- 线性一致性读写操作
- 自动故障恢复和领导者选举
- 集群成员动态变更
- 与现有 KnociDB 无缝集成

## 架构设计

该实现包含以下核心组件：

1. **配置管理 (config.go)**：定义 Raft 节点和集群的配置参数
2. **状态机 (statemachine.go)**：实现 Dragonboat 的状态机接口，处理数据操作
3. **节点管理 (node.go)**：管理 Raft 节点的生命周期和操作
4. **数据库适配器 (adapter.go)**：将 KnociDB 与 Raft 共识机制集成
5. **主接口 (raft.go)**：提供简单的 API 来使用 Raft 功能

## 使用方法

### 1. 创建配置

```go
// 创建默认配置
config := raft.DefaultConfig(
    1,                // 节点ID
    100,              // 集群ID
    "localhost:10001", // Raft地址
    "/path/to/data"    // 数据目录
)

// 添加初始集群成员
config.InitialMembers = map[uint64]string{
    1: "localhost:10001",
    2: "localhost:10002",
    3: "localhost:10003",
}
```

### 2. 打开数据库并启用 Raft

```go
// 打开常规数据库
db, err := knocidb.Open(knocidb.DefaultOptions("/path/to/db"))
if err != nil {
    log.Fatalf("打开数据库失败: %v", err)
}

// 启用 Raft 分布式功能
raftDB, err := raft.OpenRaftDB(db, config)
if err != nil {
    log.Fatalf("启用Raft失败: %v", err)
}
defer raftDB.Close()
```

### 3. 使用分布式数据库

```go
// 写入数据（通过 Raft 共识）
if err := raftDB.Put([]byte("key"), []byte("value")); err != nil {
    log.Printf("写入失败: %v", err)
}

// 读取数据（线性一致性读）
value, err := raftDB.Get([]byte("key"))
if err != nil {
    log.Printf("读取失败: %v", err)
}

// 删除数据
if err := raftDB.Delete([]byte("key")); err != nil {
    log.Printf("删除失败: %v", err)
}
```

### 4. 集群管理

```go
// 检查节点是否是 Leader
isLeader := raftDB.IsLeader()

// 获取当前 Leader 的 NodeID
leaderID, err := raftDB.GetLeaderID()

// 获取集群成员信息
members, err := raftDB.GetClusterMembership()

// 添加新节点
err := raftDB.AddNode(4, "localhost:10004")

// 移除节点
err := raftDB.RemoveNode(3)
```

## 示例

查看 `example.go` 文件了解如何创建和使用一个三节点的 Raft 集群。

## 注意事项

1. 确保集群中至少有大多数节点可用，以维持 Raft 共识
2. Leader 节点提供线性一致性读写，Follower 节点可能返回稍旧的数据
3. 首次启动集群时，需要指定初始成员配置
4. 新节点加入现有集群时，需要设置 `JoinCluster = true`