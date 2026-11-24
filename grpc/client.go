package knocigrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type Client struct {
	cc  *grpc.ClientConn
	cli KVServiceClient
}

// Dial 使用标准 grpc.NewClient 建立到 gRPC 服务的连接（默认 insecure）
func Dial(addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	cc, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{cc: cc, cli: NewKVServiceClient(cc)}, nil
}

// HealthCheck 调用标准 grpc.health.v1 检查服务健康状态。
func (c *Client) HealthCheck(ctx context.Context, service string, opts ...grpc.CallOption) (healthpb.HealthCheckResponse_ServingStatus, error) {
	hc := healthpb.NewHealthClient(c.cc)
	resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{Service: service}, opts...)
	if err != nil {
		return healthpb.HealthCheckResponse_UNKNOWN, err
	}
	return resp.GetStatus(), nil
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

// PutWithTTL 写入带相对 TTL 的 key/value
func (c *Client) PutWithTTL(ctx context.Context, key, value []byte, ttl time.Duration, opts ...grpc.CallOption) error {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, ttl.Milliseconds())
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(key)))
	buf.Write(key)
	buf.Write(value)
	_, err := c.cli.PutWithTTL(ctx, &wrapperspb.BytesValue{Value: buf.Bytes()}, opts...)
	return err
}

// GetTTL 查询 key 的剩余 TTL；hasTTL=false 表示不存在或未设置 TTL。
func (c *Client) GetTTL(ctx context.Context, key []byte, opts ...grpc.CallOption) (time.Duration, bool, error) {
	resp, err := c.cli.GetTTL(ctx, &wrapperspb.BytesValue{Value: key}, opts...)
	if err != nil {
		return 0, false, err
	}
	if resp.Value < 0 {
		return 0, false, nil
	}
	return time.Duration(resp.Value) * time.Millisecond, true, nil
}

// Persist 移除 key 的 TTL。
func (c *Client) Persist(ctx context.Context, key []byte, opts ...grpc.CallOption) (bool, error) {
	resp, err := c.cli.Persist(ctx, &wrapperspb.BytesValue{Value: key}, opts...)
	if err != nil {
		return false, err
	}
	return resp.Value, nil
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
	if err != nil {
		return err
	}
	for _, op := range ops {
		if err := bc.Send(&wrapperspb.BytesValue{Value: op}); err != nil {
			return err
		}
	}
	_, err = bc.CloseAndRecv()
	return err
}

// Scan 发送范围请求并接收服务端流式返回
// 回调 fn 返回 false 时立即取消子 context，通知服务端尽快关闭迭代器释放锁。
func (c *Client) Scan(ctx context.Context, start, end []byte, fn func(key, value []byte) bool, opts ...grpc.CallOption) error {
	return c.ScanLimit(ctx, start, end, 0, fn, opts...)
}

// ScanLimit 发送带条数上限的分页范围扫描请求（limit==0 表示不限）。
func (c *Client) ScanLimit(ctx context.Context, start, end []byte, limit uint32, fn func(key, value []byte) bool, opts ...grpc.CallOption) error {
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var req bytes.Buffer
	_ = binary.Write(&req, binary.LittleEndian, uint32(len(start)))
	req.Write(start)
	_ = binary.Write(&req, binary.LittleEndian, uint32(len(end)))
	req.Write(end)
	if limit > 0 {
		_ = binary.Write(&req, binary.LittleEndian, limit)
	}
	sc, err := c.cli.Scan(scanCtx, &wrapperspb.BytesValue{Value: req.Bytes()}, opts...)
	if err != nil {
		return err
	}
	for {
		msg, err := sc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		r := bytes.NewReader(msg.Value)
		key, err := readLengthPrefixedBytes(r)
		if err != nil {
			return err
		}
		value, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if !fn(key, value) {
			cancel()
			break
		}
	}
	return nil
}
