# KnociDB 命令行客户端

这是一个交互式命令行客户端，允许用户直接连接和操作KnociDB数据库。

## 功能特点

- 支持基本的CRUD操作（GET、PUT、DEL等）
- 提供交互式命令行界面
- 支持单机模式和分布式模式

## 使用方法

### 编译

```bash
cd examples/cmd_client
go build -o knocidb-cli
```

### 运行

#### 单机模式

```bash
# 使用默认配置
./knocidb-cli

# 指定数据目录
./knocidb-cli --dir /path/to/data

# 指定分区数量
./knocidb-cli --partitions 3
```

#### 分布式模式

```bash
# 启动节点1（作为集群的第一个节点）
./knocidb-cli --distributed --dir ./node --id 1 --listen 127.0.0.1:8001 --peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003

# 启动节点2并加入集群
./knocidb-cli --distributed --dir ./node --id 2 --listen 127.0.0.1:8002  --join

# 启动节点3并加入集群
./knocidb-cli --distributed --dir ./node --id 3 --listen 127.0.0.1:8003  --join
```

#### 分布式模式特有命令

```
# 查看集群信息
CLUSTER INFO

# 添加新节点到集群
CLUSTER ADD <node_id> <address>

# 从集群中移除节点
CLUSTER REMOVE <node_id>

# 查看详细信息（包括集群状态）
CLUSTER STATUS
```

## 支持的命令

- `GET key` - 获取指定key的值
- `PUT key value` - 设置key的值
- `DEL key [key ...]` - 删除一个或多个key
- `EXISTS key [key ...]` - 检查一个或多个key是否存在
- `PING [message]` - 测试连接，返回PONG或指定消息
- `BEGIN` - 开启批处理
- `COMMIT` - 提交批处理
- `ECHO message` - 返回相同的消息
- `INFO` - 显示服务器信息
- `HELP` - 显示帮助信息
- `EXIT` - 退出客户端

## 示例

```
KnociDB
use 'HELP' to get help，'EXIT' to exit the client
knocidb> SET mykey hello
OK
knocidb> GET mykey
"hello"
knocidb> EXISTS mykey
(integer) 1
knocidb> DEL mykey
(integer) 1
knocidb> GET mykey
(nil)
knocidb> PING
PONG
knocidb> INFO
# knocidb
mode standalone

# Data
db_path ./data
partitions 3
knocidb> EXIT
see you next time~
```