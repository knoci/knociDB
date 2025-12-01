# knociDB gRPC Examples

Examples for using knociDB over gRPC: basic `Put/Get/Delete`, client-stream `Batch`, and server-stream `Scan`. Optional token auth and TLS/mTLS supported.

## Structure
- `examples/gRPC/server/`: gRPC server example
- `examples/gRPC/client/`: gRPC client example

## Run Server

- Basic (no TLS/auth):

```bash
go run ./examples/gRPC/server --dir ./grpc_data --addr 127.0.0.1:50051
```

- TLS (server cert):

```bash
go run ./examples/gRPC/server \
  --dir ./grpc_data \
  --addr :8443 \
  --tls-cert server.crt \
  --tls-key server.key
```

- mTLS + Token auth:

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

Notes:
- With `--mtls` and `--tls-ca`, the client must present a certificate.
- With `--token`, all RPCs must carry the token or return `Unauthenticated`.

## Run Client

Start interactive client (no TLS/auth):

```bash
go run ./examples/gRPC/client --addr 127.0.0.1:50051
```

TLS/mTLS + Token:

```bash
go run ./examples/gRPC/client \
  --addr localhost:8443 \
  --tls-ca ca.crt \
  --tls-server-name localhost \
  --token secret
```

Commands: `HELP`, `GET key`, `PUT key value`, `DEL key [key...]`, `SCAN start end`, `BEGIN`, `BPUT key value`, `BDEL key`, `COMMIT`, `DISCARD`, `EXIT`.

## API

- Basic:
  - `Put(key,value)` write KV
  - `Get(key)` read KV; returns `NotFound` when missing
  - `Delete(key)` delete KV

- Batch (client-stream):
  - Payload: `op(1=Put,2=Delete) | uint32 key_len | key | [value]`
  - Client sends multiple entries; server commits once

- Scan (server-stream):
  - Request: `uint32 start_len | start | uint32 end_len | end`
  - Response: `uint32 key_len | key | value`
  - Only valid with `IndexType=BTree`; Hash returns `FailedPrecondition`

## Error Mapping

- `ErrKeyNotFound` → `NotFound`
- `ErrKeyIsEmpty` → `InvalidArgument`
- `ErrDBClosed` → `Unavailable`
- Others → `Internal`

## References

- Service desc & stream handlers: `grpc/kv_service.go`
- Server logic: `grpc/server.go`
- Lightweight client: `grpc/kv_client.go`
- Convenience client: `grpc/client.go`
- Security config: `grpc/security.go`
