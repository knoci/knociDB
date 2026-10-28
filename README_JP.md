# knociDB
[![Release](https://img.shields.io/github/v/release/knoci/knociDB)](https://github.com/knoci/knociDB/releases)
[![License](https://img.shields.io/github/license/knoci/knociDB)](https://github.com/knoci/knociDB/main/LICENSE)
[![codecov](https://codecov.io/gh/knoci/knociDB/graph/badge.svg?token=56I4EZVBTW)](https://codecov.io/gh/knoci/knociDB)
[![golangci-lint](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml)
[![Go](https://github.com/knoci/knociDB/actions/workflows/go.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/go.yml)
[![OpenIssue](https://img.shields.io/github/issues/knoci/knociDB)](https://github.com/knoci/knociDB/issues)
![Stars](https://img.shields.io/github/stars/knoci/knociDB)
![KnociDB Logo](docs/logo.png)
### [English](README.md) | [中文](README_CN.md) |日本語

## 機能
- ⚡ **高性能** キーバリューストレージ
- 🔄 ACD保証付きの**バッチ操作**
- 🌲 **B+Treeインデックス**のサポート（旧diskhashインデックスは削除）
- ⚙️ **シャードmemtable**とパーティション単位の書き込みロック（デフォルト5）
- 🌐 **ClusterDB** 単一Raftグループ、Leader-aware転送、任意のRPC認証
- 💾 **S3オブジェクトストレージ**の読み取りとwalのロード
- 📸 **非同期スナップショット** B+Tree専用のバックグラウンドスナップショット、アトミックアーカイブ、manifest
- 📋 **アトミックDB manifest** ファイル一覧とgeneration管理
- 🔌 **gRPC対応** クライアントストリームBatch、サーバーストリームScan、認証/TLSオプション
- 🧩 **Redis互換サブセット** string/hash/list/set/zset、TTL、SCAN、Pub/Sub、クラスタプロトコル

## クイックスタート
```go
package main

import (
    "time"
    "github.com/knoci/knocidb"
)

// このファイルはknociDBの基本操作の使い方を示しています
func main() {
    options := knocidb.DefaultOptions
    options.DirPath = "/tmp/knocidb_basic"

    // データベースを開く
    db, err := knocidb.Open(options)
    if err != nil {
        panic(err)
    }
    defer func() {
        _ = db.Close()
    }()

    // キーを設定する
    err = db.Put([]byte("name"), []byte("knocidb"))
    if err != nil {
        panic(err)
    }

    // キーを取得する
    val, err := db.Get([]byte("name"))
    if err != nil {
        panic(err)
    }
    println(string(val))

    // TTL付きでキーを設定する
    err = db.PutWithTTL([]byte("session"), []byte("alive"), 30*time.Second)
    if err != nil {
        panic(err)
    }

    // 残りTTLを取得する
    ttl, ok, err := db.GetTTL([]byte("session"))
    if err != nil {
        panic(err)
    }
    if ok {
        println(ttl.String())
    }

    // キーを削除する
    err = db.Delete([]byte("name"))
    if err != nil {
        panic(err)
    }
}
```
## 使用例
💡 Clusterの使用方法については、examples/clusterディレクトリの使用例を参照してください。READMEドキュメントでClusterの具体的な使用方法を確認できます。
```bash
$ cd examples/cluster
$ go build -o cluster-example main.go
```
🔆 knociDBのより多くの機能を体験したい場合は、examples/cmd_clientにコマンドラインクライアントの実装があります。ファイル内のREADMEドキュメントで具体的な使用方法を確認できます。
```bash
$ cd examples/cmd_client
$ go build -o knocidb-cli
```

🔌 gRPCの使用方法については `examples/gRPC` を参照してください。対話型クライアントと設定可能なサーバーを含みます。
```bash
$ cd examples/gRPC/server
$ go run main.go --dir ./grpc_data --addr 127.0.0.1:50051

$ cd ../client
$ go run main.go --addr 127.0.0.1:50051
```

🧩 Redis互換サブセットについては `examples/redis` を参照してください。string/hash/list/set/zset、TTL、SCAN、Pub/Sub に対応しています。
```bash
$ cd examples/redis
$ go run main.go --dir ./redis_data --addr 127.0.0.1:6379

$ redis-cli -p 6379
```

## 情報
📚 詳細なドキュメントについては、`docs/`ディレクトリを参照してください。

## 謝辞
👍️ このプロジェクトの設計と実装は、以下の優れたプロジェクトのインスピレーションなしでは成り立ちません
- https://github.com/lni/dragonboat GoのマルチグループRaftライブラリ
- https://github.com/skyzh/mini-lsm 1週間でLSMツリーストレージエンジンを構築するコース
- https://github.com/hypermodeinc/badger Goの高速キーバリューDB
- https://github.com/rosedblabs/rosedb Bitcaskベースの軽量で高速なキーバリューストレージエンジン
- https://github.com/lotusdblabs/lotusdb Goで書かれた最も先進的なキーバリューデータベース、LSMツリーとB+Treeー両方に対応
- https://github.com/etcd-io/bbolt Go用の組み込み型キーバリューデータベース
    
