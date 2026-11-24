# knociDB
[![Release](https://img.shields.io/github/v/release/knoci/knociDB)](https://github.com/knoci/knociDB/releases)
[![License](https://img.shields.io/github/license/knoci/knociDB)](https://github.com/knoci/knociDB/main/LICENSE)
[![codecov](https://codecov.io/gh/knoci/knociDB/graph/badge.svg?token=56I4EZVBTW)](https://codecov.io/gh/knoci/knociDB)
[![golangci-lint](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/golangci-lint.yml)
[![Go](https://github.com/knoci/knociDB/actions/workflows/go.yml/badge.svg)](https://github.com/knoci/knociDB/actions/workflows/go.yml)
[![OpenIssue](https://img.shields.io/github/issues/knoci/knociDB)](https://github.com/knoci/knociDB/issues)
![Stars](https://img.shields.io/github/stars/knoci/knociDB)
![KnociDB Logo](docs/logo.png)
### [English](README.md) | [中文](README_CN.md) | 日本語

## 機能
- ⚡ **超高速組み込みエンジン**：独自 MVCC Arena Skiplist（`arenaskl`）、WAL Group Commit + 32 KB ブロックキャッシュ、フリーズなし MVCC イテレータ、O(log N) プレフィックス探索
- 🔄 **アトミックバッチ & 埋め込み TTL**：シャード横断 ACD 保証とメタデータ埋め込み型 `ExpireAt`（`TTL.DB` 二重書き込み解消）
- 🌲 **B+Tree インデックス & オンライン CAS コンパクション**：単一トランザクションの昇順フラッシュ（`ApplyBatch`）とロックフリー `CompareAndSwapBatch` コンパクション
- 🌐 **本番向け Multi-Raft（`ClusterDB`）**：Dragonboat `IOnDiskStateMachine`、無停止オンラインストリーミングスナップショット（`KRS2`）、バイナリ Raft コーデック、mTLS、`ReadConsistency`、`BatchID` 冪等性、決定論的 Raft TTL
- 💾 **オブジェクト層 & アトミック manifest**：S3/R2/COS/OSS へのスナップショット・コールド VLOG 退避と generation 追跡
- 🔌 **gRPC v2 & 可観測性**：`PutWithTTL`/`GetTTL`/`Persist`、ページネーション `ScanLimit`、`grpc.health.v1`、Server Reflection、`DB.Stats()`、`/metrics`（Prometheus）、`/healthz`、`/readyz`、`/debug/pprof/`、統合エントリ `cmd/knocidb` & `Dockerfile`

## クイックスタート
```go
package main

import (
	"fmt"
	"time"

	"github.com/knoci/knocidb"
)

// このファイルはknociDBの基本操作の使い方を示しています
func main() {
	options := knocidb.NewDefaultOptions("/tmp/knocidb_basic")

	// データベースを開く
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// キーを設定する
	if err := db.Put([]byte("name"), []byte("knocidb")); err != nil {
		panic(err)
	}

	// キーを取得する
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	// メタデータ埋め込み TTL 付きでキーを設定する
	if err := db.PutWithTTL([]byte("session"), []byte("alive"), 30*time.Second); err != nil {
		panic(err)
	}

	// 残り TTL を取得する
	if ttl, ok, err := db.GetTTL([]byte("session")); err != nil {
		panic(err)
	} else if ok {
		fmt.Println(ttl.String())
	}

	// エンジンのランタイム統計を取得する
	stats := db.Stats()
	fmt.Printf("shards=%d seq=%d\n", stats.Shards, stats.Sequence)

	// キーを削除する
	if err := db.Delete([]byte("name")); err != nil {
		panic(err)
	}
}
```

## 使用例
🚀 本番環境でのスタンドアロン gRPC または Multi-Raft サーバー運用（Prometheus `/metrics`・`/healthz`・`/readyz`・`/debug/pprof/` 付き）には `cmd/knocidb` と `Dockerfile` を利用できます：
```bash
$ go run ./cmd/knocidb --mode=standalone --data-dir=./data --grpc-addr=:9090 --admin-addr=:9091
```

💡 Cluster の使用方法については、examples/cluster ディレクトリの使用例を参照してください。README ドキュメントで Cluster の具体的な使用方法を確認できます。
```bash
$ cd examples/cluster
$ go build -o cluster-example main.go
```

`OpenCluster` の前に `config.ShardCount` を設定すると、複数の固定 Raft グループを作成できます。

🔆 knociDB のより多くの機能を体験したい場合は、examples/cmd_client にコマンドラインクライアントの実装があります。ファイル内の README ドキュメントで具体的な使用方法を確認できます。

```bash
$ cd examples/cmd_client
$ go build -o knocidb-cli
```

🔌 gRPC の使用方法については `examples/gRPC` を参照してください。対話型クライアントと設定可能なサーバーを含みます。
```bash
$ cd examples/gRPC/server
$ go run main.go --dir ./grpc_data --addr 127.0.0.1:50051

$ cd ../client
$ go run main.go --addr 127.0.0.1:50051
```

## パフォーマンス

強化された knociDB v2 エンジン（ゼロ GC ポインタのフラットオフセット MVCC Arena Skiplist、ゼロアロケーション WAL バッチ符号化、ブロックキャッシュ、埋め込み TTL、フリーズなしイテレータ）は優れた読み書き性能を発揮します。テストではランダム読み取りが LevelDB の約 **2.32 倍**、順次読み取りが約 **9.78 倍**（読み取り両指標でテスト対象 Go エンジン中トップ）を達成し、WAL クラッシュリカバリ、アトミック Batch、オンライン CAS コンパクション、オブジェクト層、シャード Multi-Raft を備えています。

**📖 スループット**

| エンジン      | 順次書き込み | ランダム書き込み | ランダム読み取り | 順次読み取り |
| ------------- | -----------: | ---------------: | ---------------: | -----------: |
| 🟢 **knociDB** |       240.9k |           198.2k |     🥇 **1.42M** |  🥇 **3.06M** |
| LevelDB       |       294.0k |           205.4k |             610k |         313k |
| LotusDB       |       289.8k |           213.3k |             543k |         145k |
| BadgerDB      |       141.5k |           116.8k |             205k |         304k |
| BoltDB        |        35.6k |            35.9k |             763k |        1.51M |

**📦 大きな value の bulk-load**

| エンジン      | バッチ書き込み |
| ------------- | -------------: |
| 🟢 **knociDB** |      **89.7k** |
| BadgerDB      |          89.1k |
| BoltDB        |         103.7k |
| LevelDB       |          62.7k |

<sub><i><span style="color:#777">Windows、Go 1.24.4、シングルスレッド、fsync なし。スループット: 100k key × 1 KB、50k 回のランダム読み書き（新版 knociDB はウォームアップ後 3 回の中央値）。bulk-load: 20k key × 16 KB、100 件ずつ（3 回の最良値）。結果はワークロード依存です。BoltDB のスループットは単発 Update、bulk-load はトランザクションのバッチです。マイクロベンチマークは `go test -bench . -benchmem ./benchmark` でも実行できます（i9-13980HX 環境で `BenchmarkPut`: 2,900 ns/op・0 allocs/op、`BenchmarkGet`: 202.9 ns/op・0 allocs/op、`BenchmarkParallelPutGet`: 612.3 ns/op・0 allocs/op、`BenchmarkBatch` 100件: 116.7 µs/op）。</span></i></sub>

**💡 機能比較**

| 機能                  | knociDB                  | BadgerDB       | RocksDB | BoltDB | LevelDB | LotusDB        |
| --------------------- | ------------------------ | -------------- | ------- | ------ | ------- | -------------- |
| ストレージ設計        | B+Tree + ValueLog (MVCC) | LSM + ValueLog | LSM     | B+Tree | LSM     | LSM + ValueLog |
| Pure Go               | ✅                        | ✅              | ❌       | ✅      | ✅       | ✅              |
| 組み込み可能          | ✅                        | ✅              | ✅       | ✅      | ✅       | ✅              |
| ソート済み反復        | ✅（フリーズなし MVCC）  | ✅              | ✅       | ✅      | ✅       | ✅              |
| アトミックバッチ      | ✅                        | ✅              | ✅       | ✅      | ✅       | ➖              |
| ACID トランザクション | ❌                        | ✅              | ✅       | ✅      | ❌       | ❌              |
| TTL                   | ✅（埋め込みメタデータ） | ✅              | ➖       | ❌      | ❌       | ➖              |
| スナップショット      | ✅（オンラインストリーミング）| ✅           | ✅       | ✅      | ✅       | ➖              |
| キー・バージョン      | ➖（内部 MVCC）          | ✅              | ❌       | ❌      | ❌       | ❌              |
| シャード multi-Raft   | ✅（`IOnDiskStateMachine`）| ❌             | ❌       | ❌      | ❌       | ❌              |
| オブジェクト層        | ✅                        | ❌              | ❌       | ❌      | ❌       | ❌              |
| gRPC                  | ✅（v2 + Health/Reflect）| ❌              | ❌       | ❌      | ❌       | ❌              |

<sub><i><span style="color:#777">✅ 対応 · ❌ 非対応 · ➖ 限定的、内部機能または非ネイティブ</span></i></sub>

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
