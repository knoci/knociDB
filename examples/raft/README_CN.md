# KnociDB Raft 示例

这个示例展示了如何使用 KnociDB 的 Raft 共识模块构建一个分布式键值存储系统。

## 使用方法

### 编译

```bash
go build -o raft-example main.go
```

### 启动集群

#### 启动第一个节点（作为集群的初始节点）

```bash
# Windows
.\raft-example --id=1 --addr=localhost:8001 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003

# Linux/macOS
./raft-example --id=1 --addr=localhost:8001 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003
```

#### 启动第二个节点并加入集群

```bash
# Windows
.\raft-example --id=2 --addr=localhost:8002 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join

# Linux/macOS
./raft-example --id=2 --addr=localhost:8002 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join
```

#### 启动第三个节点并加入集群

```bash
# Windows
.\raft-example --id=3 --addr=localhost:8003 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join

# Linux/macOS
./raft-example --id=3 --addr=localhost:8003 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join
```

### 命令行参数

- `--id`: 节点ID（必须是唯一的正整数）
- `--cluster`: 集群ID（默认为1）
- `--addr`: Raft节点地址（格式：host:port）
- `--dir`: 数据目录
- `--join`: 是否作为新节点加入现有集群
- `--peers`: 集群成员列表（格式：id1=host1:port1,id2=host2:port2,...）
- `--sync`: 是否使用同步模式（默认为true）
- `--interactive`: 是否启用交互模式（默认为true）

### 交互命令

启动节点后，可以使用以下命令与系统交互：

- `put <key> <value>`: 写入键值对
- `get <key>`: 读取键值
- `delete <key>`: 删除键值对
- `leader`: 显示当前Leader信息
- `members`: 显示集群成员
- `add <id> <addr>`: 添加新节点
- `remove <id>`: 移除节点
- `help`: 显示帮助信息
- `exit`: 退出程序

## 示例操作流程

1. 启动三个节点组成集群
2. 在任意节点上执行 `put name KnociDB` 写入数据
3. 在其他节点上执行 `get name` 验证数据已同步
4. 执行 `leader` 查看当前Leader节点
5. 执行 `members` 查看集群成员

## 注意事项

- 确保所有节点的时钟同步
- 集群需要多数节点在线才能正常工作（如三节点集群至少需要两个节点在线）
- 在生产环境中，应该为每个节点配置不同的数据目录