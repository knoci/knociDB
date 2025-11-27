package knocigrpc

import (
    "bytes"
    "context"
    "encoding/binary"
    "io"
    "net"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/protobuf/types/known/wrapperspb"
)

type Client struct {
    cc  *grpc.ClientConn
    cli KVServiceClient
}

// Dial 以默认配置建立到 gRPC 服务的连接（默认 insecure）
func Dial(addr string, opts ...grpc.DialOption) (*Client, error) {
    if len(opts) == 0 {
        opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
    }
    cc, err := grpc.Dial(addr, opts...)
    if err != nil {
        return nil, err
    }
    return &Client{cc: cc, cli: NewKVServiceClient(cc)}, nil
}

// Close 关闭客户端连接
func (c *Client) Close() error {
    return c.cc.Close()
}

// Get 读取指定 key 的值
func (c *Client) Get(ctx context.Context, key []byte, opts ...grpc.CallOption) ([]byte, error) {
    resp, err := c.cli.Get(ctx, &wrapperspb.BytesValue{Value: key}, opts...)
    if err != nil {
        return nil, err
    }
    return resp.Value, nil
}

// Put 写入一条 key/value（内部打包为二进制载荷）
func (c *Client) Put(ctx context.Context, key, value []byte, opts ...grpc.CallOption) error {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, uint32(len(key)))
    buf.Write(key)
    buf.Write(value)
    _, err := c.cli.Put(ctx, &wrapperspb.BytesValue{Value: buf.Bytes()}, opts...)
    return err
}

// Delete 删除指定 key
func (c *Client) Delete(ctx context.Context, key []byte, opts ...grpc.CallOption) error {
    _, err := c.cli.Delete(ctx, &wrapperspb.BytesValue{Value: key}, opts...)
    return err
}

// ListenOn 在指定地址创建一个 TCP 监听器
func ListenOn(addr string) (net.Listener, error) {
    return net.Listen("tcp", addr)
}

// EncodeBatchPut 将一条 Put 操作编码为批处理载荷
func EncodeBatchPut(key, value []byte) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, byte(1))
    _ = binary.Write(&buf, binary.LittleEndian, uint32(len(key)))
    buf.Write(key)
    buf.Write(value)
    return buf.Bytes()
}

// EncodeBatchDelete 将一条 Delete 操作编码为批处理载荷
func EncodeBatchDelete(key []byte) []byte {
    var buf bytes.Buffer
    _ = binary.Write(&buf, binary.LittleEndian, byte(2))
    _ = binary.Write(&buf, binary.LittleEndian, uint32(len(key)))
    buf.Write(key)
    return buf.Bytes()
}

// Batch 发送批处理操作（客户端流），服务端一次性提交
func (c *Client) Batch(ctx context.Context, ops [][]byte, opts ...grpc.CallOption) error {
    bc, err := c.cli.Batch(ctx, opts...)
    if err != nil { return err }
    for _, op := range ops {
        if err := bc.Send(&wrapperspb.BytesValue{Value: op}); err != nil { return err }
    }
    _, err = bc.CloseAndRecv()
    return err
}

// Scan 发送范围请求并接收服务端流式返回
// 回调 fn 返回 false 时提前结束
func (c *Client) Scan(ctx context.Context, start, end []byte, fn func(key, value []byte) bool, opts ...grpc.CallOption) error {
    var req bytes.Buffer
    _ = binary.Write(&req, binary.LittleEndian, uint32(len(start)))
    req.Write(start)
    _ = binary.Write(&req, binary.LittleEndian, uint32(len(end)))
    req.Write(end)
    sc, err := c.cli.Scan(ctx, &wrapperspb.BytesValue{Value: req.Bytes()}, opts...)
    if err != nil { return err }
    for {
        msg, err := sc.Recv()
        if err == io.EOF { break }
        if err != nil { return err }
        r := bytes.NewReader(msg.Value)
        var klen uint32
        _ = binary.Read(r, binary.LittleEndian, &klen)
        key := make([]byte, klen)
        _, _ = io.ReadFull(r, key)
        value, _ := io.ReadAll(r)
        if !fn(key, value) { break }
    }
    return nil
}
