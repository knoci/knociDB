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
- 🌲 **B+Tree index** support (legacy diskhash index removed)
- 🌐 **ClusterDB** single-Raft group with leader-aware forwarding and optional RPC auth
- 💾 **S3 Object Storage** read and load wal
- 📸 **Async snapshots** B+Tree-only background snapshot with atomic archive + manifest
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
