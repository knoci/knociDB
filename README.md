# knociDB
[![Release](https://img.shields.io/github/v/release/knoci/knociDB)](https://github.com/knoci/knociDB/releases)
[![License](https://img.shields.io/github/license/knoci/knociDB)](https://github.com/knoci/knociDB/main/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/knoci/knociDB)](https://goreportcard.com/report/github.com/knoci/knociDB)
[![OpenIssue](https://img.shields.io/github/issues/knoci/knociDB)](https://github.com/knoci/knociDB/issues)
[![ClosedIssue](https://img.shields.io/github/issues-closed/knoci/knociDB)](https://github.com/knoci/knociDB/issues?q=is%3Aissue+is%3Aclosed)
![Stars](https://img.shields.io/github/stars/knoci/knociDB)
![Forks](https://img.shields.io/github/forks/knoci/knociDB)
![KnociDB Logo](docs/logo.png)
## Features
- ⚡ **High-performance** key-value storage
- 🔄 **Batch operations** with ACD guarantees
- 🌲 **B+Tree** and **Hash index** support
- 🌐 **Distributed deployment** with Raft protocol

## Info
📚 See the `docs/` directory for detailed documentation.

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

## Acknowledgments
👍️ The design and implementation of this project cannot be separated from the inspiration of the following excellent projects
- https://github.com/lni/dragonboat A Multi-Group Raft library in Go.
- https://github.com/skyzh/mini-lsm A course of building an LSM-Tree storage engine (database) in a week.
- https://github.com/hypermodeinc/badger Fast key-value DB in Go.
- https://github.com/rosedblabs/rosedb Lightweight, fast and reliable key/value storage engine based on Bitcask.
- https://github.com/lotusdblabs/lotusdb Most advanced key-value database written in Go, extremely fast, compatible with LSM tree and B+ tree.
- https://github.com/etcd-io/bbolt An embedded key/value database for Go.