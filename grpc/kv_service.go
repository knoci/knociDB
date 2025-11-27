package knocigrpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type KVServiceServer interface {
	Get(context.Context, *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error)
	Put(context.Context, *wrapperspb.BytesValue) (*emptypb.Empty, error)
	Delete(context.Context, *wrapperspb.BytesValue) (*emptypb.Empty, error)
	// Batch 客户端流：接收多条操作（Put/Delete），在服务端批量提交
	Batch(KVService_BatchServer) error
	// Scan 服务端流：在B+Tree模式下按范围扫描并流式返回KV
	Scan(*wrapperspb.BytesValue, KVService_ScanServer) error
}

// RegisterKVServiceServer 注册 KVService 到 gRPC Server
func RegisterKVServiceServer(s *grpc.Server, srv KVServiceServer) {
	s.RegisterService(&KVService_ServiceDesc, srv)
}

var KVService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "knocidb.KVService",
	HandlerType: (*KVServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _KVService_Get_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _KVService_Put_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _KVService_Delete_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Batch",
			Handler:       _KVService_Batch_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "Scan",
			Handler:       _KVService_Scan_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "grpc/kv.proto",
}

// _KVService_Get_Handler 服务端处理 Get 的入口
func _KVService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(wrapperspb.BytesValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/knocidb.KVService/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Get(ctx, req.(*wrapperspb.BytesValue))
	}
	return interceptor(ctx, in, info, handler)
}

// _KVService_Put_Handler 服务端处理 Put 的入口
func _KVService_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(wrapperspb.BytesValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/knocidb.KVService/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Put(ctx, req.(*wrapperspb.BytesValue))
	}
	return interceptor(ctx, in, info, handler)
}

// _KVService_Delete_Handler 服务端处理 Delete 的入口
func _KVService_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(wrapperspb.BytesValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServiceServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/knocidb.KVService/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServiceServer).Delete(ctx, req.(*wrapperspb.BytesValue))
	}
	return interceptor(ctx, in, info, handler)
}

type KVService_BatchServer interface {
	SendAndClose(*emptypb.Empty) error
	Recv() (*wrapperspb.BytesValue, error)
	grpc.ServerStream
}

type kvServiceBatchServer struct{ grpc.ServerStream }

// SendAndClose 发送关闭消息
func (x *kvServiceBatchServer) SendAndClose(m *emptypb.Empty) error { return x.ServerStream.SendMsg(m) }

// Recv 接收一条客户端流消息
func (x *kvServiceBatchServer) Recv() (*wrapperspb.BytesValue, error) {
	m := new(wrapperspb.BytesValue)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// _KVService_Batch_Handler 服务端处理 Batch（客户端流）的入口
func _KVService_Batch_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(KVServiceServer).Batch(&kvServiceBatchServer{stream})
}

type KVService_ScanServer interface {
	Send(*wrapperspb.BytesValue) error
	grpc.ServerStream
}

type kvServiceScanServer struct{ grpc.ServerStream }

// Send 发送一条服务端流消息
func (x *kvServiceScanServer) Send(m *wrapperspb.BytesValue) error { return x.ServerStream.SendMsg(m) }

// _KVService_Scan_Handler 服务端处理 Scan（服务端流）的入口
func _KVService_Scan_Handler(srv interface{}, stream grpc.ServerStream) error {
	in := new(wrapperspb.BytesValue)
	if err := stream.RecvMsg(in); err != nil {
		return err
	}
	return srv.(KVServiceServer).Scan(in, &kvServiceScanServer{stream})
}
