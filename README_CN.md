# knociDB
[![Release](https://img.shields.io/github/v/release/knoci/knociDB)](https://github.com/knoci/knociDB/releases)
[![License](https://img.shields.io/github/license/knoci/knociDB)](https://github.com/knoci/knociDB/main/LICENSE)
[![codecov](https://codecov.io/gh/knoci/knociDB/graph/badge.svg?token=56I4EZVBTW)](https://codecov.io/gh/knoci/knociDB)
[![golangci-lint](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml)
[![Go](https://github.com/knoci/knociDB/actions/workflows/go.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/go.yml)
[![OpenIssue](https://img.shields.io/github/issues/knoci/knociDB)](https://github.com/knoci/knociDB/issues)
![Stars](https://img.shields.io/github/stars/knoci/knociDB)
![KnociDB Logo](docs/logo.png)
### [English](README.md) | 中文 | [日本語](README_JP.md)

## 特性
- ⚡ **高性能**键值存储
- 🔄 **批量操作**具有ACD保证
- 🌲 **B+树索引**支持
- 🌐 **分片 ClusterDB** 多 Raft Group、key 哈希路由、Leader-aware 转发
- 💾 **对象冷层** 快照 + VLOG 冷段（S3/R2/COS/OSS）
- 📋 **原子 DB manifest** 文件清单与 generation 跟踪
- 🔌 **gRPC 支持** 客户端流式批量、服务端流式扫描、可选鉴权与 TLS
- 🧩 **Redis 兼容子集** string/hash/list/set/zset、TTL、SCAN、Pub/Sub、集群协议

## 快速开始
```go
package main

import (
	"time"
	"github.com/knoci/knocidb"
)

// 此文件展示了如何使用knociDB的基本操作
func main() {
	options := knocidb.DefaultOptions
	options.DirPath = "/tmp/knocidb_basic"

	// 打开数据库
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// 存入一个键
	err = db.Put([]byte("name"), []byte("knocidb"))
	if err != nil {
		panic(err)
	}

	// 获取一个键
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// 存入一个带 TTL 的键
	err = db.PutWithTTL([]byte("session"), []byte("alive"), 30*time.Second)
	if err != nil {
		panic(err)
	}

	// 读取剩余 TTL
	ttl, ok, err := db.GetTTL([]byte("session"))
	if err != nil {
		panic(err)
	}
	if ok {
		println(ttl.String())
	}

	// 删除一个键
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```

## 示例
💡 关于 Cluster 的使用，请查看 `examples/cluster` 目录中的使用示例，您可以通过该文件中的 README 文档获取使用 Cluster 的具体方法。
```bash
$ cd examples/cluster
$ go build -o cluster-example main.go
```

在 `OpenCluster` 前设置 `config.ShardCount` 即可创建多个固定 Raft Group。
🔆 如果您想体验knociDB的更多功能，在`examples/cmd_client`中有一个命令行实现客户端，您可以通过该文件中的README文档获取使用cmd_client的具体方法。
```bash
$ cd examples/cmd_client
$ go build -o knocidb-cli
```

🔌 关于 gRPC 的使用，请查看 `examples/gRPC`，其中包含交互式客户端与可配置服务端。
```bash
$ cd examples/gRPC/server
$ go run main.go --dir ./grpc_data --addr 127.0.0.1:50051

$ cd ../client
$ go run main.go --addr 127.0.0.1:50051
```

🧩 关于 Redis 兼容子集，请查看 `examples/redis`。支持 string/hash/list/set/zset、TTL、SCAN 和 Pub/Sub。
```bash
$ cd examples/redis
$ go run main.go --dir ./redis_data --addr 127.0.0.1:6379

$ redis-cli -p 6379
```

## 性能表现

knociDB 读性能突出，大 value 批量导入效率高：测试中随机读约为 LevelDB 的 **2.1 倍**，顺序读约为 **9.8 倍**。单条写不是最快，但批量写很快，并且支持 WAL 崩溃恢复、原子 Batch、B+Tree 索引、对象冷层和分片多 Raft。

**📖 吞吐**

| 引擎          | 顺序写 | 随机写 |      随机读 |      顺序读 |
| ------------- | -----: | -----: | ----------: | ----------: |
| 🟢 **knociDB** | 228.6k | 172.2k | 🥇 **1.27M** | 🥇 **3.07M** |
| LevelDB       | 294.0k | 205.4k |        610k |        313k |
| LotusDB       | 289.8k | 213.3k |        543k |        145k |
| BadgerDB      | 141.5k | 116.8k |        205k |        304k |
| BoltDB        |  35.6k |  35.9k |        763k |       1.51M |

**📦 大 value 批量导入**

| 引擎          |    批量写 |
| ------------- | --------: |
| 🟢 **knociDB** | **96.2k** |
| BadgerDB      |     89.1k |
| BoltDB        |    103.7k |
| LevelDB       |     62.7k |

<sub><i><span style="color:#777">Windows、Go 1.24.4、单线程、不 fsync。吞吐：10 万 key × 1 KB，5 万随机读/写（三次中位数）。批量导入：2 万 key × 16 KB，每批 100 条（三次最优）。结果与具体负载相关；BoltDB 吞吐使用单条 Update，批量导入使用事务批量提交。</span></i></sub>

**💡 功能对比**

| 功能         | knociDB           | BadgerDB       | RocksDB | BoltDB | LevelDB | LotusDB        |
| ------------ | ----------------- | -------------- | ------- | ------ | ------- | -------------- |
| 存储设计     | B+Tree + ValueLog | LSM + ValueLog | LSM     | B+Tree | LSM     | LSM + ValueLog |
| 纯 Go        | ✅                 | ✅              | ❌       | ✅      | ✅       | ✅              |
| 可嵌入       | ✅                 | ✅              | ✅       | ✅      | ✅       | ✅              |
| 有序遍历     | ✅                 | ✅              | ✅       | ✅      | ✅       | ✅              |
| 原子批量     | ✅                 | ✅              | ✅       | ✅      | ✅       | ➖              |
| ACID 事务    | ❌                 | ✅              | ✅       | ✅      | ❌       | ❌              |
| TTL          | ✅                 | ✅              | ➖       | ❌      | ❌       | ➖              |
| 快照         | ✅                 | ✅              | ✅       | ✅      | ✅       | ➖              |
| 键值版本     | ❌                 | ✅              | ❌       | ❌      | ❌       | ❌              |
| 分片多 Raft  | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |
| 对象冷层     | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |
| Redis + gRPC | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |

<sub><i><span style="color:#777">✅ 支持 · ❌ 不支持 · ➖ 有限支持或非原生能力</span></i></sub>

## 信息

📚 详细文档请查看`docs/`目录。

## 致谢
👍️ 本项目的设计和实现离不开以下优秀项目的启发:
- https://github.com/lni/dragonboat Go语言的多组Raft库。
- https://github.com/skyzh/mini-lsm 一周内构建LSM-Tree存储引擎（数据库）的课程。
- https://github.com/hypermodeinc/badger Go语言的快速键值数据库。
- https://github.com/rosedblabs/rosedb 基于Bitcask的轻量级、快速且可靠的键/值存储引擎。
- https://github.com/lotusdblabs/lotusdb 用Go编写的最先进的键值数据库，极快，兼容LSM树和B+树。
- https://github.com/etcd-io/bbolt Go语言的嵌入式键/值数据库。
