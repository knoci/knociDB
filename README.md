# knociDB
[![Release](https://img.shields.io/github/v/release/knoci/knociDB)](https://github.com/knoci/knociDB/releases)
[![License](https://img.shields.io/github/license/knoci/knociDB)](https://github.com/knoci/knociDB/main/LICENSE)
[![codecov](https://codecov.io/gh/knoci/knociDB/graph/badge.svg?token=56I4EZVBTW)](https://codecov.io/gh/knoci/knociDB)
[![Go Report Card](https://goreportcard.com/badge/github.com/knoci/knociDB)](https://goreportcard.com/report/github.com/knoci/knociDB)
[![Go](https://github.com/knoci/knociDB/actions/workflows/go.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/go.yml)
[![OpenIssue](https://img.shields.io/github/issues/knoci/knociDB)](https://github.com/knoci/knociDB/issues)
![Stars](https://img.shields.io/github/stars/knoci/knociDB)
![KnociDB Logo](docs/logo.png)
### English | [中文](README_CN.md)

## Features
- ⚡ **High-performance** key-value storage
- 🔄 **Batch operations** with ACD guarantees
- 🌲 **B+Tree** and **Hash index** support
- 🌐 **Distributed deployment** with Raft protocol

## Quick Start
```go
package main

import (
	"knocidb"
)

// this file shows how to use the basic operations of LotusDB
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

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
```

## Examples
💡 For Basic usage ,please check out usage examples in the `examples/` directory:
```bash
$ cd examples/basic
$ go run main.go
```
🔆 If you want to  experience more features of knociDB, there is a cmd implementation client in the  `examples/cmd_client`, you can obtain the specific method of using cmd_client through the README document in the file.
```bash
$ cd examples/cmd_client
$ go build -o knocidb-cli
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
