package knocigrpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type KVServiceClient interface {
	Get(context.Context, *wrapperspb.BytesValue, ...grpc.CallOption) (*wrapperspb.BytesValue, error)
	Put(context.Context, *wrapperspb.BytesValue, ...grpc.CallOption) (*emptypb.Empty, error)
	Delete(context.Context, *wrapperspb.BytesValue, ...grpc.CallOption) (*emptypb.Empty, error)
	// Batch 客户端流：发送多条 Put/Delete 操作
	Batch(context.Context, ...grpc.CallOption) (KVService_BatchClient, error)
	// Scan 服务端流：按范围返回 KV
	Scan(context.Context, *wrapperspb.BytesValue, ...grpc.CallOption) (KVService_ScanClient, error)
}

type kvServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewKVServiceClient(cc grpc.ClientConnInterface) KVServiceClient {
	return &kvServiceClient{cc: cc}
}

func (c *kvServiceClient) Get(ctx context.Context, in *wrapperspb.BytesValue, opts ...grpc.CallOption) (*wrapperspb.BytesValue, error) {
	out := new(wrapperspb.BytesValue)
	err := c.cc.Invoke(ctx, "/knocidb.KVService/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Put(ctx context.Context, in *wrapperspb.BytesValue, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/knocidb.KVService/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Delete(ctx context.Context, in *wrapperspb.BytesValue, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/knocidb.KVService/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVService_BatchClient 客户端流：向服务端发送批处理操作
type KVService_BatchClient interface {
	Send(*wrapperspb.BytesValue) error
	CloseAndRecv() (*emptypb.Empty, error)
	grpc.ClientStream
}

type kvServiceBatchClient struct{ grpc.ClientStream }

// Send 发送一条批处理操作
func (x *kvServiceBatchClient) Send(m *wrapperspb.BytesValue) error { return x.ClientStream.SendMsg(m) }
func (x *kvServiceBatchClient) CloseAndRecv() (*emptypb.Empty, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(emptypb.Empty)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *kvServiceClient) Batch(ctx context.Context, opts ...grpc.CallOption) (KVService_BatchClient, error) {
	desc := &KVService_ServiceDesc.Streams[0]
	cs, err := c.cc.NewStream(ctx, desc, "/knocidb.KVService/Batch", opts...)
	if err != nil {
		return nil, err
	}
	return &kvServiceBatchClient{cs}, nil
}

type KVService_ScanClient interface {
	Recv() (*wrapperspb.BytesValue, error)
	grpc.ClientStream
}

type kvServiceScanClient struct{ grpc.ClientStream }

// Recv 接收一条服务端流返回的 KV 数据
func (x *kvServiceScanClient) Recv() (*wrapperspb.BytesValue, error) {
	m := new(wrapperspb.BytesValue)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *kvServiceClient) Scan(ctx context.Context, in *wrapperspb.BytesValue, opts ...grpc.CallOption) (KVService_ScanClient, error) {
	desc := &KVService_ServiceDesc.Streams[1]
	cs, err := c.cc.NewStream(ctx, desc, "/knocidb.KVService/Scan", opts...)
	if err != nil {
		return nil, err
	}
	if err := cs.SendMsg(in); err != nil {
		return nil, err
	}
	if err := cs.CloseSend(); err != nil {
		return nil, err
	}
	return &kvServiceScanClient{cs}, nil
}
