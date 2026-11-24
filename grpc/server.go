package knocigrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/knoci/knocidb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	DefaultMaxRecvMsgSize       = 64 << 20 // 64 MiB
	DefaultMaxSendMsgSize       = 64 << 20 // 64 MiB
	DefaultMaxConcurrentStreams = 1024
	DefaultMaxBatchOps          = 100000
	DefaultMaxBatchBytes        = 64 << 20 // 64 MiB
)

type Server struct {
	db            *knocidb.DB
	MaxBatchOps   int
	MaxBatchBytes int64
}

// NewServer 创建一个 gRPC 服务器适配器
func NewServer(db *knocidb.DB) *Server {
	return &Server{
		db:            db,
		MaxBatchOps:   DefaultMaxBatchOps,
		MaxBatchBytes: DefaultMaxBatchBytes,
	}
}

// Start 在指定地址启动 gRPC 服务
// 可通过 opts 传入 TLS/拦截器等配置
func Start(addr string, db *knocidb.DB, opts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	baseOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(DefaultMaxRecvMsgSize),
		grpc.MaxSendMsgSize(DefaultMaxSendMsgSize),
		grpc.MaxConcurrentStreams(DefaultMaxConcurrentStreams),
	}
	baseOpts = append(baseOpts, opts...)
	s := grpc.NewServer(baseOpts...)
	RegisterKVServiceServer(s, NewServer(db))
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("knocidb.KVService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, hs)
	reflection.Register(s)
	go func() {
		_ = s.Serve(lis)
	}()
	return s, lis, nil
}

func mapDBError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, knocidb.ErrKeyIsEmpty) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, knocidb.ErrDBClosed) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// readLengthPrefixedBytes 安全读取带 uint32 小端长度前缀的字节切片，
// 在分配内存前校验长度不超过剩余字节数，防止恶意大长度导致 OOM。
func readLengthPrefixedBytes(r *bytes.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	if uint64(n) > uint64(r.Len()) {
		return nil, status.Error(codes.InvalidArgument, "invalid payload length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	return buf, nil
}

// Get 读取指定 key 的值
func (s *Server) Get(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}
	v, err := s.db.Get(in.Value)
	if err != nil {
		return nil, mapDBError(err)
	}
	if v == nil {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return &wrapperspb.BytesValue{Value: v}, nil
}

// Put 写入一条 key/value（payload 自定义二进制：uint32 key_len | key | value）
func (s *Server) Put(ctx context.Context, in *wrapperspb.BytesValue) (*emptypb.Empty, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	r := bytes.NewReader(in.Value)
	key, err := readLengthPrefixedBytes(r)
	if err != nil {
		return nil, err
	}
	val, err := io.ReadAll(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	if err := s.db.Put(key, val); err != nil {
		return nil, mapDBError(err)
	}
	return &emptypb.Empty{}, nil
}

// PutWithTTL 写入带相对 TTL 的 key/value（载荷：int64 ttl_ms | uint32 key_len | key | value）
func (s *Server) PutWithTTL(ctx context.Context, in *wrapperspb.BytesValue) (*emptypb.Empty, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	r := bytes.NewReader(in.Value)
	var ttlMs int64
	if err := binary.Read(r, binary.LittleEndian, &ttlMs); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	if ttlMs <= 0 {
		return nil, status.Error(codes.InvalidArgument, "ttl must be positive")
	}
	key, err := readLengthPrefixedBytes(r)
	if err != nil {
		return nil, err
	}
	val, err := io.ReadAll(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid payload")
	}
	if err := s.db.PutWithTTL(key, val, time.Duration(ttlMs)*time.Millisecond); err != nil {
		return nil, mapDBError(err)
	}
	return &emptypb.Empty{}, nil
}

// GetTTL 返回 key 的剩余 TTL（毫秒）；无 TTL 或不存在时返回 -1。
func (s *Server) GetTTL(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.Int64Value, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}
	left, ok, err := s.db.GetTTL(in.Value)
	if err != nil {
		return nil, mapDBError(err)
	}
	if !ok {
		return &wrapperspb.Int64Value{Value: -1}, nil
	}
	return &wrapperspb.Int64Value{Value: left.Milliseconds()}, nil
}

// Persist 移除 key 的 TTL。
func (s *Server) Persist(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BoolValue, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}
	ok, err := s.db.Persist(in.Value)
	if err != nil {
		return nil, mapDBError(err)
	}
	return &wrapperspb.BoolValue{Value: ok}, nil
}

// Delete 删除指定 key
func (s *Server) Delete(ctx context.Context, in *wrapperspb.BytesValue) (*emptypb.Empty, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}
	if err := s.db.Delete(in.Value); err != nil {
		return nil, mapDBError(err)
	}
	return &emptypb.Empty{}, nil
}

const (
	opPut    byte = 1
	opDelete byte = 2
)

// Batch 客户端流：接收多条操作并批量提交
func (s *Server) Batch(stream KVService_BatchServer) error {
	maxOps := s.MaxBatchOps
	if maxOps <= 0 {
		maxOps = DefaultMaxBatchOps
	}
	maxBytes := s.MaxBatchBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBatchBytes
	}

	batch := s.db.NewBatch(knocidb.DefaultBatchOptions)
	defer batch.Close()

	var opCount int
	var totalBytes int64
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
			return status.FromContextError(err).Err()
		}
		if msg == nil {
			return status.Error(codes.InvalidArgument, "invalid payload")
		}
		opCount++
		totalBytes += int64(len(msg.Value))
		if opCount > maxOps || totalBytes > maxBytes {
			return status.Error(codes.ResourceExhausted, "batch limit exceeded")
		}

		r := bytes.NewReader(msg.Value)
		var op byte
		if err := binary.Read(r, binary.LittleEndian, &op); err != nil {
			return status.Error(codes.InvalidArgument, "invalid payload")
		}
		key, err := readLengthPrefixedBytes(r)
		if err != nil {
			return err
		}
		switch op {
		case opPut:
			val, err := io.ReadAll(r)
			if err != nil {
				return status.Error(codes.InvalidArgument, "invalid payload")
			}
			if err := batch.Put(key, val); err != nil {
				return mapDBError(err)
			}
		case opDelete:
			if err := batch.Delete(key); err != nil {
				return mapDBError(err)
			}
		default:
			return status.Error(codes.InvalidArgument, "unknown op")
		}
	}
	if err := batch.Commit(); err != nil {
		return mapDBError(err)
	}
	return stream.SendAndClose(&emptypb.Empty{})
}

// Scan 服务端流：在B+Tree模式下按范围扫描并流式返回KV
// 请求载荷：uint32 start_len | start | uint32 end_len | end
func (s *Server) Scan(in *wrapperspb.BytesValue, stream KVService_ScanServer) error {
	if in == nil {
		return status.Error(codes.InvalidArgument, "invalid payload")
	}
	r := bytes.NewReader(in.Value)
	start, err := readLengthPrefixedBytes(r)
	if err != nil {
		return err
	}
	end, err := readLengthPrefixedBytes(r)
	if err != nil {
		return err
	}
	// 可选分页上限：若载荷后紧跟 4 字节小端 uint32，则作为返回条数上限（0 表示不限）。
	var limit uint32
	if r.Len() >= 4 {
		_ = binary.Read(r, binary.LittleEndian, &limit)
	}

	itr, err := s.db.NewIterator(knocidb.IteratorOptions{Reverse: false})
	if err != nil {
		return mapDBError(err)
	}
	defer func() {
		_ = itr.Close()
	}()
	if len(start) == 0 {
		itr.Rewind()
	} else {
		itr.Seek(start)
	}
	var sent uint32
	for itr.Valid() {
		if err := stream.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		key := itr.Key()
		if len(end) != 0 && bytes.Compare(key, end) > 0 {
			break
		}
		val := itr.Value()
		if err := itr.Err(); err != nil {
			return mapDBError(err)
		}
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(key)))
		buf.Write(key)
		buf.Write(val)
		if err := stream.Send(&wrapperspb.BytesValue{Value: buf.Bytes()}); err != nil {
			return err
		}
		sent++
		if limit > 0 && sent >= limit {
			break
		}
		itr.Next()
	}
	if err := itr.Err(); err != nil {
		return mapDBError(err)
	}
	return nil
}
