# knociDB gRPC 示例

本示例展示如何通过 gRPC 使用 knociDB，包括基础的 `Put/Get/Delete`，批量操作 `Batch`（客户端流），以及范围扫描 `Scan`（服务端流）。支持可选的 Token 鉴权与 TLS/mTLS。

## 目录结构
- `examples/gRPC/server/`：gRPC 服务端示例
- `examples/gRPC/client/`：gRPC 客户端示例

## 启动服务端

基础（不启用 TLS/鉴权）：

```bash
go run ./examples/gRPC/server --dir ./grpc_data --addr 127.0.0.1:50051
```

启用 TLS（服务端证书）：

```bash
go run ./examples/gRPC/server \
  --dir ./grpc_data \
  --addr :8443 \
  --tls-cert server.crt \
  --tls-key server.key
```

启用 mTLS（双向证书）+ Token 鉴权：

```bash
go run ./examples/gRPC/server \
  --dir ./grpc_data \
  --addr :8443 \
  --tls-cert server.crt \
  --tls-key server.key \
  --tls-ca ca.crt \
  --mtls \
  --token secret
```

说明：
- 同时开启 `--mtls` 与 `--tls-ca` 时，客户端必须提供证书。
- 开启 `--token` 后，所有 RPC 必须携带 Token，否则返回 `Unauthenticated`。

## 运行客户端

交互式客户端（不启用 TLS/鉴权）：

```bash
go run ./examples/gRPC/client --addr 127.0.0.1:50051
```

启用 TLS/mTLS + Token：

```bash
go run ./examples/gRPC/client \
  --addr localhost:8443 \
  --tls-ca ca.crt \
  --tls-server-name localhost \
  --token secret
```

支持命令：`HELP`、`GET key`、`PUT key value`、`DEL key [key...]`、`SCAN start end`、`BEGIN`、`BPUT key value`、`BDEL key`、`COMMIT`、`DISCARD`、`EXIT`。

## API 说明

- 基础：
  - `Put(key,value)`：写入一条 KV
  - `Get(key)`：读取 KV；不存在返回 `NotFound`
  - `Delete(key)`：删除 KV

- 批量（客户端流）：
  - 载荷格式：`op(1=Put,2=Delete) | uint32 key_len | key | [value]`
  - 调用方式：客户端多次 `Send`，服务端一次性 `Commit`

- 扫描（服务端流）：
  - 载荷格式：`uint32 start_len | start | uint32 end_len | end`
  - 响应格式：`uint32 key_len | key | value`
  - 仅在 `IndexType=BTree` 下有效；Hash 索引返回 `FailedPrecondition`

## 错误码映射

- `ErrKeyNotFound` → `NotFound`
- `ErrKeyIsEmpty` → `InvalidArgument`
- `ErrDBClosed` → `Unavailable`
- 未知错误 → `Internal`

## 参考文件

- 服务描述与流式 Handler：`grpc/kv_service.go`
- 业务服务端：`grpc/server.go`
- 轻量客户端：`grpc/kv_client.go`
- 便捷封装：`grpc/client.go`
- 安全配置：`grpc/security.go`
