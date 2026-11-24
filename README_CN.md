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
- ⚡ **极致单机引擎**：自研 MVCC Arena 跳表（`arenaskl`）、WAL Group Commit 与 32 KB 块缓存、零冻结快照迭代器与 O(log N) 前缀寻址
- 🔄 **原子批量与内嵌 TTL**：跨分片 ACD 原子提交，内嵌 `ExpireAt` 元数据彻底消除独立 `TTL.DB` 双写放大
- 🌲 **B+Tree 索引与在线 CAS 压缩**：单事务升序刷盘（`ApplyBatch`）与无锁并发 `CompareAndSwapBatch` 增量压缩
- 🌐 **生产级分片 Multi-Raft（`ClusterDB`）**：Dragonboat `IOnDiskStateMachine`、零停服 `KRS2` 流式快照、紧凑二进制编码、双向 mTLS、一致读分级（`ReadConsistency`）、`BatchID` 幂等与 Raft 确定性 TTL
- 💾 **对象冷层与原子清单**：支持 S3/R2/COS/OSS 快照及 VLOG 冷段下沉与原子 MANIFEST 版本追踪
- 🔌 **gRPC v2 与可观测性**：`PutWithTTL`/`GetTTL`/`Persist`、分页 `ScanLimit`、`grpc.health.v1`、Server Reflection、`DB.Stats()`、`/metrics`（Prometheus）、`/healthz`、`/readyz`、`/debug/pprof/`、统一入口 `cmd/knocidb` 与 `Dockerfile`

## 快速开始
```go
package main

import (
	"fmt"
	"time"

	"github.com/knoci/knocidb"
)

// 此文件展示了如何使用knociDB的基本操作
func main() {
	options := knocidb.NewDefaultOptions("/tmp/knocidb_basic")

	// 打开数据库
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// 存入一个键
	if err := db.Put([]byte("name"), []byte("knocidb")); err != nil {
		panic(err)
	}

	// 获取一个键
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	// 存入一个带内嵌元数据 TTL 的键
	if err := db.PutWithTTL([]byte("session"), []byte("alive"), 30*time.Second); err != nil {
		panic(err)
	}

	// 读取剩余 TTL
	if ttl, ok, err := db.GetTTL([]byte("session")); err != nil {
		panic(err)
	} else if ok {
		fmt.Println(ttl.String())
	}

	// 读取引擎实时指标
	stats := db.Stats()
	fmt.Printf("shards=%d seq=%d\n", stats.Shards, stats.Sequence)

	// 删除一个键
	if err := db.Delete([]byte("name")); err != nil {
		panic(err)
	}
}
```

## 示例
🚀 生产环境部署单机 gRPC 服务或 Multi-Raft 集群守护进程（带 `/metrics`、`/healthz`、`/readyz`、`/debug/pprof/`），请使用统一入口 `cmd/knocidb` 或根目录 `Dockerfile`：
```bash
$ go run ./cmd/knocidb --mode=standalone --data-dir=./data --grpc-addr=:9090 --admin-addr=:9091
```

💡 关于 Cluster 的使用，请查看 `examples/cluster` 目录中的使用示例，您可以通过该文件中的 README 文档获取使用 Cluster 的具体方法。
```bash
$ cd examples/cluster
$ go build -o cluster-example main.go
```

在 `OpenCluster` 前设置 `config.ShardCount` 即可创建多个固定 Raft Group。

🔆 如果您想体验 knociDB 的更多功能，在 `examples/cmd_client` 中有一个命令行实现客户端，您可以通过该文件中的 README 文档获取使用 cmd_client 的具体方法。

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

## 性能表现

升级后的 knociDB v2 存储引擎（零 GC 指针扁平偏移 MVCC Arena 跳表、WAL 零分配编码、32 KB 块缓存、内嵌元数据 TTL 与零冻结迭代器）在读吞吐与综合读写性能上均实现突破：实测随机读约为 LevelDB 的 **2.32 倍**，顺序读约为 **9.78 倍**（两项读指标均居所测 Go 引擎第一），同时完整具备 WAL 崩溃恢复、原子 Batch、在线 CAS 压缩、对象冷层与生产级分片 Multi-Raft 能力。

**📖 吞吐**

| 引擎          | 顺序写 | 随机写 |        随机读 |        顺序读 |
| ------------- | -----: | -----: | ------------: | ------------: |
| 🟢 **knociDB** | 240.9k | 198.2k | 🥇 **1.42M** | 🥇 **3.06M** |
| LevelDB       | 294.0k | 205.4k |          610k |          313k |
| LotusDB       | 289.8k | 213.3k |          543k |          145k |
| BadgerDB      | 141.5k | 116.8k |          205k |          304k |
| BoltDB        |  35.6k |  35.9k |          763k |         1.51M |

**📦 大 value 批量导入**

| 引擎          |    批量写 |
| ------------- | --------: |
| 🟢 **knociDB** | **89.7k** |
| BadgerDB      |     89.1k |
| BoltDB        |    103.7k |
| LevelDB       |     62.7k |

<sub><i><span style="color:#777">Windows、Go 1.24.4、单线程、不 fsync。吞吐：10 万 key × 1 KB，5 万随机读/写（新版 knociDB 为预热后三次中位数）。批量导入：2 万 key × 16 KB，每批 100 条（三次最优）。结果与具体负载相关；BoltDB 吞吐使用单条 Update，批量导入使用事务批量提交。另可运行 `go test -bench . -benchmem ./benchmark` 查看 Go 微基准（i9-13980HX 上 `BenchmarkPut`: 2,900 ns/op、0 allocs/op，`BenchmarkGet`: 202.9 ns/op、0 allocs/op，`BenchmarkParallelPutGet`: 612.3 ns/op、0 allocs/op，`BenchmarkBatch` 100 条: 116.7 µs/op）。</span></i></sub>

**💡 功能对比**

| 功能         | knociDB                  | BadgerDB       | RocksDB | BoltDB | LevelDB | LotusDB        |
| ------------ | ------------------------ | -------------- | ------- | ------ | ------- | -------------- |
| 存储设计     | B+Tree + ValueLog (MVCC) | LSM + ValueLog | LSM     | B+Tree | LSM     | LSM + ValueLog |
| 纯 Go        | ✅                        | ✅              | ❌       | ✅      | ✅       | ✅              |
| 可嵌入       | ✅                        | ✅              | ✅       | ✅      | ✅       | ✅              |
| 有序遍历     | ✅（零冻结 MVCC）        | ✅              | ✅       | ✅      | ✅       | ✅              |
| 原子批量     | ✅                        | ✅              | ✅       | ✅      | ✅       | ➖              |
| ACID 事务    | ❌                        | ✅              | ✅       | ✅      | ❌       | ❌              |
| TTL          | ✅（内嵌元数据）         | ✅              | ➖       | ❌      | ❌       | ➖              |
| 快照         | ✅（在线流式快照）       | ✅              | ✅       | ✅      | ✅       | ➖              |
| 键值版本     | ➖（内部 MVCC 快照）     | ✅              | ❌       | ❌      | ❌       | ❌              |
| 分片多 Raft  | ✅（`IOnDiskStateMachine`）| ❌             | ❌       | ❌      | ❌       | ❌              |
| 对象冷层     | ✅                        | ❌              | ❌       | ❌      | ❌       | ❌              |
| gRPC         | ✅（v2 + 健康检查/反射） | ❌              | ❌       | ❌      | ❌       | ❌              |

<sub><i><span style="color:#777">✅ 支持 · ❌ 不支持 · ➖ 有限支持、内部能力或非原生能力</span></i></sub>

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
