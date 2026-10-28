# knociDB Redis-compatible Example

A small Redis-compatible subset on top of knociDB. It uses RESP2 and supports strings, hashes, lists, sets, sorted sets, TTL, SCAN/KEYS, Pub/Sub and a cluster protocol endpoint.

## Run

~~~bash
go run ./examples/redis --dir ./redis_data --addr 127.0.0.1:6379
~~~

With a password:

~~~bash
go run ./examples/redis --dir ./redis_data --addr 127.0.0.1:6379 --password secret
~~~

Cluster-enabled single node:

~~~bash
go run ./examples/redis --dir ./redis_data --addr 127.0.0.1:6379 --cluster --node-id node-1
~~~

Then use any RESP2 Redis client:

~~~bash
redis-cli -p 6379
# cluster client
redis-cli -c -p 6379
~~~

## Examples

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

Pub/Sub in two clients:

~~~text
# client A
SUBSCRIBE news

# client B
PUBLISH news hello
~~~

## Supported commands

- Connection: PING, ECHO, SELECT 0, AUTH, HELLO 2, CLIENT SETINFO/SETNAME/GETNAME, COMMAND, QUIT
- String: SET (EX/PX/NX/XX), GET, SETNX, DEL/UNLINK, EXISTS, MSET, MGET, INCR/DECR/INCRBY/DECRBY, APPEND, STRLEN, TYPE
- TTL: EXPIRE, PEXPIRE, TTL, PTTL, PERSIST
- Hash: HSET, HMSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HKEYS, HVALS
- List: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE
- Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- ZSet: ZADD, ZSCORE, ZREM, ZCARD, ZRANGE, ZREVRANGE, ZRANK, ZREVRANK, ZINCRBY, ZCOUNT, ZRANGEBYSCORE
- Keyspace: KEYS, SCAN, DBSIZE
- Pub/Sub: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB
- Cluster: CLUSTER INFO/MYID/SLOTS/NODES/KEYSLOT, READONLY, ASKING

## Not supported

MULTI/EXEC/WATCH, Streams, Lua, RESP3, Redis Cluster gossip/failover/replication.
The server returns an error for unsupported commands instead of pretending to support them.

## Notes

- knociDB now only provides the B+Tree index; hash/set/zset/list scans use its ordered iteration.
- Redis keys are stored under the r1: prefix. Hash fields, set members and list positions are separate knociDB keys; list elements use an 8-byte sequence number.
- TTL is stored in per-key metadata and a background sweeper removes expired keys (default every 1s).
- This is a library-level example, not a production Redis replacement.

## TTL background sweeper

~~~go
srv, lis, err := redis.Start(addr, db, redis.WithExpiryInterval(5*time.Second))
~~~

## Redis Cluster

This is a cluster protocol endpoint, not a full Redis Cluster control plane:

- Single node: all 16384 slots belong to this node; use --cluster with redis-cli -c.
- Static multi-node: configure the same ClusterOptions.Nodes on every node. Commands for remote slots return MOVED; cross-slot multi-key commands return CROSSSLOT.
- No gossip, automatic failover, replication or slot migration.
- Pub/Sub is local to the node; cross-node PUBLISH broadcast is not implemented.

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

## Go client

~~~go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", Protocol: 2})
defer rdb.Close()
~~~
