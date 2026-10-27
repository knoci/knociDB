# knociDB Redis 兼容示例

在 knociDB 上实现的一个小型 Redis 兼容子集，使用 RESP2，支持 string、hash、list、set、zset、TTL、SCAN/KEYS、Pub/Sub 和集群协议端点。

## 运行

~~~bash
go run ./examples/redis --dir ./redis_data --addr 127.0.0.1:6379
~~~

带密码：

~~~bash
go run ./examples/redis --dir ./redis_data --addr 127.0.0.1:6379 --password secret
~~~

单节点集群协议：

~~~bash
go run ./examples/redis --dir ./redis_data --addr 127.0.0.1:6379 --cluster --node-id node-1
~~~

然后用任意 RESP2 Redis 客户端连接：

~~~bash
redis-cli -p 6379
# 集群客户端
redis-cli -c -p 6379
~~~

## 示例

~~~text
SET user:1:name Alice
GET user:1:name

HSET user:1 name Alice age 20
HGETALL user:1

RPUSH queue a b c
LRANGE queue 0 -1

SADD tags go db
SMEMBERS tags

ZADD ranking 10 Alice 20 Bob
ZRANGE ranking 0 -1 WITHSCORES

KEYS user:*
SCAN 0 MATCH user:* COUNT 10

EXPIRE user:1:name 60
TTL user:1:name
~~~

两个客户端测试发布订阅：

~~~text
# 客户端 A
SUBSCRIBE news

# 客户端 B
PUBLISH news hello
~~~

## 已支持命令

- 连接：PING、ECHO、SELECT 0、AUTH、HELLO 2、CLIENT SETINFO/SETNAME/GETNAME、COMMAND、QUIT
- String：SET（EX/PX/NX/XX）、GET、SETNX、DEL/UNLINK、EXISTS、MSET、MGET、INCR/DECR/INCRBY/DECRBY、APPEND、STRLEN、TYPE
- TTL：EXPIRE、PEXPIRE、TTL、PTTL、PERSIST
- Hash：HSET、HMSET、HGET、HDEL、HEXISTS、HLEN、HGETALL、HKEYS、HVALS
- List：LPUSH、RPUSH、LPOP、RPOP、LLEN、LRANGE
- Set：SADD、SREM、SMEMBERS、SISMEMBER、SCARD
- ZSet：ZADD、ZSCORE、ZREM、ZCARD、ZRANGE、ZREVRANGE、ZRANK、ZREVRANK、ZINCRBY、ZCOUNT、ZRANGEBYSCORE
- Key 空间：KEYS、SCAN、DBSIZE
- Pub/Sub：SUBSCRIBE、UNSUBSCRIBE、PSUBSCRIBE、PUNSUBSCRIBE、PUBLISH、PUBSUB
- Cluster：CLUSTER INFO/MYID/SLOTS/NODES/KEYSLOT、READONLY、ASKING

## 不支持

MULTI/EXEC/WATCH、Streams、Lua、RESP3、Redis Cluster 的 gossip/failover/复制。
不支持的命令会返回错误，不会假装支持。

## 说明

- knociDB 必须使用 BTree 索引（Options.IndexType = knocidb.BTree），因为 hash/set/zset/list 的遍历依赖有序扫描。
- Redis key 统一放在 r1: 前缀下；hash field、set member、list element 都是独立的 knociDB key；list 元素用 8 字节逻辑序号定位。
- TTL 存在每个 key 的元数据里，后台 sweeper 默认每秒清理一次过期 key。
- 这是库级示例，不是生产级 Redis 替代品。

## TTL 后台清理

~~~go
srv, lis, err := redis.Start(addr, db, redis.WithExpiryInterval(5*time.Second))
~~~

## Redis Cluster

这是集群协议端点，不是完整的 Redis Cluster 控制面：

- 单节点：16384 个 slot 全部属于本节点，使用 --cluster 和 redis-cli -c 即可。
- 静态多节点：每个节点配置相同的 ClusterOptions.Nodes；访问远端 slot 返回 MOVED，跨 slot 多 key 命令返回 CROSSSLOT。
- 没有 gossip、自动 failover、复制和 slot 迁移。
- Pub/Sub 只在当前节点内广播，未实现跨节点 PUBLISH 广播。

~~~go
opts := redis.ClusterOptions{
    Enabled: true,
    NodeID:  "node-1",
    Nodes: []redis.ClusterNode{
        {ID: "node-1", Host: "10.0.0.1", Port: 6379, StartSlot: 0, EndSlot: 8191},
        {ID: "node-2", Host: "10.0.0.2", Port: 6379, StartSlot: 8192, EndSlot: 16383},
    },
}
srv, lis, err := redis.Start(addr, db, redis.WithCluster(opts))
~~~

## Go 客户端

~~~go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", Protocol: 2})
defer rdb.Close()
~~~
