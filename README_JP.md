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
- 🌲 **B+Treeインデックス**のサポート
- 🌐 **シャードClusterDB** 複数Raftグループ、ハッシュルーティング、Leader-aware転送
- 💾 **オブジェクト層** スナップショット + VLOGコールドセグメント（S3/R2/COS/OSS）
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

`OpenCluster` の前に `config.ShardCount` を設定すると、複数の固定 Raft グループを作成できます。

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

## パフォーマンス

knociDB は読み取り性能に優れ、大きな value の bulk-load も効率的です。テストではランダム読み取りが LevelDB の約 **2.1 倍**、順次読み取りが約 **9.8 倍**。単発書き込みは最速ではありませんが、バッチ書き込みは高速で、WAL リカバリ、アトミック Batch、B+Tree インデックス、オブジェクト層、シャード multi-Raft を備えています。

**📖 スループット**

| エンジン      | 順次書き込み | ランダム書き込み | ランダム読み取り | 順次読み取り |
| ------------- | -----------: | ---------------: | ---------------: | -----------: |
| 🟢 **knociDB** |       228.6k |           172.2k |      🥇 **1.27M** |  🥇 **3.07M** |
| LevelDB       |       294.0k |           205.4k |             610k |         313k |
| LotusDB       |       289.8k |           213.3k |             543k |         145k |
| BadgerDB      |       141.5k |           116.8k |             205k |         304k |
| BoltDB        |        35.6k |            35.9k |             763k |        1.51M |

**📦 大きな value の bulk-load**

| エンジン      | バッチ書き込み |
| ------------- | -------------: |
| 🟢 **knociDB** |      **96.2k** |
| BadgerDB      |          89.1k |
| BoltDB        |         103.7k |
| LevelDB       |          62.7k |

<sub><i><span style="color:#777">Windows、Go 1.24.4、シングルスレッド、fsync なし。スループット: 100k key × 1 KB、50k 回のランダム読み書き（3 回の中央値）。bulk-load: 20k key × 16 KB、100 件ずつ（3 回の最良値）。結果はワークロード依存です。BoltDB のスループットは単発 Update、bulk-load はトランザクションのバッチです。</span></i></sub>

**💡機能比較**

| 機能                  | knociDB           | BadgerDB       | RocksDB | BoltDB | LevelDB | LotusDB        |
| --------------------- | ----------------- | -------------- | ------- | ------ | ------- | -------------- |
| ストレージ設計        | B+Tree + ValueLog | LSM + ValueLog | LSM     | B+Tree | LSM     | LSM + ValueLog |
| Pure Go               | ✅                 | ✅              | ❌       | ✅      | ✅       | ✅              |
| 組み込み可能          | ✅                 | ✅              | ✅       | ✅      | ✅       | ✅              |
| ソート済み反復        | ✅                 | ✅              | ✅       | ✅      | ✅       | ✅              |
| アトミックバッチ      | ✅                 | ✅              | ✅       | ✅      | ✅       | ➖              |
| ACID トランザクション | ❌                 | ✅              | ✅       | ✅      | ❌       | ❌              |
| TTL                   | ✅                 | ✅              | ➖       | ❌      | ❌       | ➖              |
| スナップショット      | ✅                 | ✅              | ✅       | ✅      | ✅       | ➖              |
| キー・バージョン      | ❌                 | ✅              | ❌       | ❌      | ❌       | ❌              |
| シャード multi-Raft   | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |
| オブジェクト層        | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |
| Redis + gRPC          | ✅                 | ❌              | ❌       | ❌      | ❌       | ❌              |

<sub><i><span style="color:#777">✅ 対応 · ❌ 非対応 · ➖ 限定的または非ネイティブ</span></i></sub>

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
