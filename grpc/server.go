package knocigrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/knoci/knocidb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type Server struct {
	db *knocidb.DB
	// EnableAuth 指示是否启用基于 token 的鉴权
	EnableAuth bool
	// AuthToken 服务端校验的令牌（通过 metadata: authorization / x-api-key 传入）
	AuthToken string
}

// NewServer 创建一个 gRPC 服务器适配器
func NewServer(db *knocidb.DB) *Server {
	return &Server{db: db}
}

// Start 在指定地址启动 gRPC 服务
// 可通过 opts 传入 TLS/拦截器等配置
func Start(addr string, db *knocidb.DB, opts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	s := grpc.NewServer(opts...)
	RegisterKVServiceServer(s, NewServer(db))
	go s.Serve(lis)
	return s, lis, nil
}

// Get 读取指定 key 的值
func (s *Server) Get(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	v, err := s.db.Get(in.Value)
	if err != nil {
		if errors.Is(err, knocidb.ErrKeyNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errors.Is(err, knocidb.ErrKeyIsEmpty) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, knocidb.ErrDBClosed) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if v == nil {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &wrapperspb.BytesValue{Value: v}, nil
}

// Put 写入一条 key/value（payload 自定义二进制：uint32 key_len | key | value）
func (s *Server) Put(ctx context.Context, in *wrapperspb.BytesValue) (*emptypb.Empty, error) {
	r := bytes.NewReader(in.Value)
	var klen uint32
	if err := binary.Read(r, binary.LittleEndian, &klen); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	key := make([]byte, klen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	val, err := io.ReadAll(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	if err := s.db.Put(key, val); err != nil {
		if errors.Is(err, knocidb.ErrKeyIsEmpty) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, knocidb.ErrDBClosed) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

// Delete 删除指定 key
func (s *Server) Delete(ctx context.Context, in *wrapperspb.BytesValue) (*emptypb.Empty, error) {
	if err := s.db.Delete(in.Value); err != nil {
		if errors.Is(err, knocidb.ErrKeyIsEmpty) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, knocidb.ErrKeyNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errors.Is(err, knocidb.ErrDBClosed) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

const (
	opPut    byte = 1
	opDelete byte = 2
)

// Batch 客户端流：接收多条操作并批量提交
func (s *Server) Batch(stream KVService_BatchServer) error {
	batch := s.db.NewBatch(knocidb.DefaultBatchOptions)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		r := bytes.NewReader(msg.Value)
		var op byte
		if err := binary.Read(r, binary.LittleEndian, &op); err != nil {
			return status.Error(codes.InvalidArgument, "invalid payload")
		}
		var klen uint32
		if err := binary.Read(r, binary.LittleEndian, &klen); err != nil {
			return status.Error(codes.InvalidArgument, "invalid payload")
		}
		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			return status.Error(codes.InvalidArgument, "invalid payload")
		}
		switch op {
		case opPut:
			val, err := io.ReadAll(r)
			if err != nil {
				return status.Error(codes.InvalidArgument, "invalid payload")
			}
			if err := batch.Put(key, val); err != nil {
				if errors.Is(err, knocidb.ErrKeyIsEmpty) {
					return status.Error(codes.InvalidArgument, err.Error())
				}
				if errors.Is(err, knocidb.ErrDBClosed) {
					return status.Error(codes.Unavailable, err.Error())
				}
				return status.Error(codes.Internal, err.Error())
			}
		case opDelete:
			if err := batch.Delete(key); err != nil {
				if errors.Is(err, knocidb.ErrKeyIsEmpty) {
					return status.Error(codes.InvalidArgument, err.Error())
				}
				if errors.Is(err, knocidb.ErrDBClosed) {
					return status.Error(codes.Unavailable, err.Error())
				}
				return status.Error(codes.Internal, err.Error())
			}
		default:
			return status.Error(codes.InvalidArgument, "unknown op")
		}
	}
	if err := batch.Commit(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return stream.SendAndClose(&emptypb.Empty{})
}

// Scan 服务端流：在B+Tree模式下按范围扫描并流式返回KV
// 请求载荷：uint32 start_len | start | uint32 end_len | end
func (s *Server) Scan(in *wrapperspb.BytesValue, stream KVService_ScanServer) error {
	if s.db.GetOptions().IndexType == knocidb.Hash {
		return status.Error(codes.FailedPrecondition, knocidb.ErrDBIteratorUnsupportedTypeHASH.Error())
	}
	r := bytes.NewReader(in.Value)
	var sLen uint32
	if err := binary.Read(r, binary.LittleEndian, &sLen); err != nil {
		return status.Error(codes.InvalidArgument, "invalid payload")
	}
	start := make([]byte, sLen)
	if _, err := io.ReadFull(r, start); err != nil {
		return status.Error(codes.InvalidArgument, "invalid payload")
	}
	var eLen uint32
	if err := binary.Read(r, binary.LittleEndian, &eLen); err != nil {
		return status.Error(codes.InvalidArgument, "invalid payload")
	}
	end := make([]byte, eLen)
	if _, err := io.ReadFull(r, end); err != nil {
		return status.Error(codes.InvalidArgument, "invalid payload")
	}

	itr, err := s.db.NewIterator(knocidb.IteratorOptions{Reverse: false})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if len(start) == 0 {
		itr.Rewind()
	} else {
		itr.Seek(start)
	}
	for itr.Valid() {
		key := itr.Key()
		if len(end) != 0 && bytes.Compare(key, end) > 0 {
			break
		}
		val := itr.Value()
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(key)))
		buf.Write(key)
		buf.Write(val)
		if err := stream.Send(&wrapperspb.BytesValue{Value: buf.Bytes()}); err != nil {
			_ = itr.Close()
			return err
		}
		itr.Next()
	}
	return itr.Close()
}
