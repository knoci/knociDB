# knociDB
[![Release](https://img.shields.io/github/v/release/knoci/knociDB)](https://github.com/knoci/knociDB/releases)
[![License](https://img.shields.io/github/license/knoci/knociDB)](https://github.com/knoci/knociDB/main/LICENSE)
[![codecov](https://codecov.io/gh/knoci/knociDB/graph/badge.svg?token=56I4EZVBTW)](https://codecov.io/gh/knoci/knociDB)
[![Go Report Card](https://goreportcard.com/badge/github.com/knoci/knociDB)](https://goreportcard.com/report/github.com/knoci/knociDB)
[![Go](https://github.com/knoci/knociDB/actions/workflows/go.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/go.yml)
[![OpenIssue](https://img.shields.io/github/issues/knoci/knociDB)](https://github.com/knoci/knociDB/issues)
![Stars](https://img.shields.io/github/stars/knoci/knociDB)
![KnociDB Logo](docs/logo.png)
### [English](README.md) | 中文

## 特性
- ⚡ **高性能**键值存储
- 🔄 **批量操作**具有ACD保证
- 🌲 **B+树**和**哈希索引**支持
- 🌐 基于Raft的**分布式部署**

## 快速开始
```go
package main

import (
	"github.com/knoci/knocidb"
)

// 此文件展示了如何使用LotusDB的基本操作
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

	// 删除一个键
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```

## 示例
💡 关于Raft的使用，请查看`examples/raft`目录中的使用示例，您可以通过该文件中的README文档获取使用cmd_client的具体方法。
```bash
$ cd examples/raft
$ go build -o raft-example main.go
```
🔆 如果您想体验knociDB的更多功能，在`examples/cmd_client`中有一个命令行实现客户端，您可以通过该文件中的README文档获取使用cmd_client的具体方法。
```bash
$ cd examples/cmd_client
$ go build -o knocidb-cli
```

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