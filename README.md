# knociDB
[![Release](https://img.shields.io/github/v/release/knoci/knociDB)](https://github.com/knoci/knociDB/releases)
[![License](https://img.shields.io/github/license/knoci/knociDB)](https://github.com/knoci/knociDB/main/LICENSE)
[![codecov](https://codecov.io/gh/knoci/knociDB/graph/badge.svg?token=56I4EZVBTW)](https://codecov.io/gh/knoci/knociDB)
[![golangci-lint](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml)
[![Go](https://github.com/knoci/knociDB/actions/workflows/go.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/go.yml)
[![OpenIssue](https://img.shields.io/github/issues/knoci/knociDB)](https://github.com/knoci/knociDB/issues)
![Stars](https://img.shields.io/github/stars/knoci/knociDB)
![KnociDB Logo](docs/logo.png)
### English | [中文](README_CN.md) | [日本語](README_JP.md)

## Features
- ⚡ **High-performance** key-value storage
- 🔄 **Batch operations** with ACD guarantees
- 🌲 **B+Tree index** support
- 🌐 **Sharded ClusterDB** multiple Raft groups with hash routing and leader-aware forwarding
- 💾 **Object tier** snapshots + cold VLOG segments (S3/R2/COS/OSS)
- 📋 **Atomic DB manifest** file inventory and generation tracking
- 🔌 **gRPC support** client-stream Batch, server-stream Scan, optional auth/TLS
- 🧩 **Redis-compatible subset** strings/hash/list/set/zset, TTL, SCAN, Pub/Sub, cluster protocol

## Quick Start
```go
package main

import (
	"time"
	"github.com/knoci/knocidb"
)

// this file shows how to use the basic operations of knociDB
func main() {
	options := knocidb.DefaultOptions
	options.DirPath = "/tmp/knocidb_basic"

	// open database
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// put a key
	err = db.Put([]byte("name"), []byte("knocidb"))
	if err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// put a key with TTL
	err = db.PutWithTTL([]byte("session"), []byte("alive"), 30*time.Second)
	if err != nil {
		panic(err)
	}

	// read remaining TTL
	ttl, ok, err := db.GetTTL([]byte("session"))
	if err != nil {
		panic(err)
	}
	if ok {
		println(ttl.String())
	}

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```

## Examples
💡 For Cluster usage, please check out usage examples in the `examples/cluster` directory, and you can get the specific method of using Cluster through the README document.
```bash
$ cd examples/cluster
$ go build -o cluster-example main.go
```

Set `config.ShardCount` before `OpenCluster` to create multiple fixed Raft groups.

🔆 If you want to  experience more features of knociDB, there is a cmd implementation client in the  `examples/cmd_client`, you can obtain the specific method of using cmd_client through the README document in the file.

```bash
$ cd examples/cmd_client
$ go build -o knocidb-cli
```

🔌 For gRPC usage, see `examples/gRPC` for an interactive client and a configurable server.
```bash
$ cd examples/gRPC/server
$ go run main.go --dir ./grpc_data --addr 127.0.0.1:50051

$ cd ../client
$ go run main.go --addr 127.0.0.1:50051
```

🧩 For the Redis-compatible subset, see `examples/redis`. It supports strings, hashes, lists, sets, sorted sets, TTL, SCAN and Pub/Sub.
```bash
$ cd examples/redis
$ go run main.go --dir ./redis_data --addr 127.0.0.1:6379

$ redis-cli -p 6379
```

## Performance

knociDB delivers strong read throughput and efficient large-value bulk loads: tests show about **2.1× LevelDB random reads** and **9.8× sequential reads**. Single-key writes are not the fastest in every case, but batch writes are fast, and the engine supports WAL crash recovery, atomic batches, a B+Tree index, an object tier, and sharded multi-Raft.

**📖 Throughput**

| Engine        | Sequential write | Random write | Random read | Sequential read |
| ------------- | ---------------: | -----------: | ----------: | --------------: |
| 🟢 **knociDB** |           228.6k |       172.2k | 🥇 **1.27M** |     🥇 **3.07M** |
| LevelDB       |           294.0k |       205.4k |        610k |            313k |
| LotusDB       |           289.8k |       213.3k |        543k |            145k |
| BadgerDB      |           141.5k |       116.8k |        205k |            304k |
| BoltDB        |            35.6k |        35.9k |        763k |           1.51M |

**📦 Large-value bulk load**

| Engine        | Batch write |
| ------------- | ----------: |
| 🟢 **knociDB** |   **96.2k** |
| BadgerDB      |       89.1k |
| BoltDB        |      103.7k |
| LevelDB       |       62.7k |

<sub><i><span style="color:#777">Windows, Go 1.24.4, single thread, no fsync. Throughput: 100k keys × 1 KB, 50k random reads/writes (median of three runs). Bulk load: 20k keys × 16 KB, batches of 100 (best of three runs). Results are workload-specific; BoltDB uses per-key Update for throughput and batch transactions for bulk load.</span></i></sub>

**💡 Feature comparison**

| Feature             | knociDB           | BadgerDB       | RocksDB | BoltDB | LevelDB | LotusDB        |
| ------------------- | ----------------- | -------------- | ------- | ------ | ------- | -------------- |
| Storage design      | B+Tree + ValueLog | LSM + ValueLog | LSM     | B+Tree | LSM     | LSM + ValueLog |
| Pure Go             | ✅                 | ✅              | ❌       | ✅      | ✅       | ✅              |
| Embeddable          | ✅                 | ✅              | ✅       | ✅      | ✅       | ✅              |
| Sorted KV iteration | ✅                 | ✅              | ✅       | ✅      | ✅       | ✅              |
| Atomic batch        | ✅                 | ✅              | ✅       | ✅      | ✅       | ➖              |
| ACID transactions   | ❌                 | ✅              | ✅       | ✅      | ❌       | ❌              |
| TTL                 | ✅                 | ✅              | ➖       | ❌      | ❌       | ➖              |
| Snapshots           | ✅                 | ✅              | ✅       | ✅      | ✅       | ➖              |
| Key-value-version   | ❌                 | ✅              | ❌       | ❌      | ❌       | ❌              |
| Sharded multi-Raft  | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |
| Object/cold tier    | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |
| Redis + gRPC        | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |

<sub><i><span style="color:#777">✅ supported · ❌ not supported · ➖ limited or not first-class</span></i></sub>

## Info

📚 See the `docs/` directory for detailed documentation.

## Acknowledgments
👍️ The design and implementation of this project cannot be separated from the inspiration of the following excellent projects
- https://github.com/lni/dragonboat A Multi-Group Raft library in Go.
- https://github.com/skyzh/mini-lsm A course of building an LSM-Tree storage engine (database) in a week.
- https://github.com/hypermodeinc/badger Fast key-value DB in Go.
- https://github.com/rosedblabs/rosedb Lightweight, fast and reliable key/value storage engine based on Bitcask.
- https://github.com/lotusdblabs/lotusdb Most advanced key-value database written in Go, extremely fast, compatible with LSM tree and B+ tree.
- https://github.com/etcd-io/bbolt An embedded key/value database for Go.
