package knocigrpc

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/knoci/knocidb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestGRPCBasic(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "knocidb_grpc_test")
	_ = os.RemoveAll(dir)
	opts := knocidb.DefaultOptions
	opts.DirPath = dir
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s, lis, err := Start("127.0.0.1:0", db)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.GracefulStop()
	cc, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cc.Put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, err := cc.Get(ctx, []byte("k1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "v1" {
		t.Fatalf("value: %s", string(v))
	}
	if err := cc.Delete(ctx, []byte("k1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = cc.Get(ctx, []byte("k1"))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expect notfound, got: %v", err)
	}
	_ = db.Close()
}

func TestGRPCBatchAndScan(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "knocidb_grpc_batch_scan")
	_ = os.RemoveAll(dir)
	opts := knocidb.DefaultOptions
	opts.DirPath = dir
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s, lis, err := Start("127.0.0.1:0", db)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.GracefulStop()
	cli, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ops := [][]byte{
		EncodeBatchPut([]byte("a1"), []byte("va1")),
		EncodeBatchPut([]byte("a2"), []byte("va2")),
		EncodeBatchDelete([]byte("a1")),
		EncodeBatchPut([]byte("b1"), []byte("vb1")),
	}
	if err := cli.Batch(ctx, ops); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// 扫描 ["a", "bz")
	var items [][2]string
	err = cli.Scan(ctx, []byte("a"), []byte("bz"), func(k, v []byte) bool {
		items = append(items, [2]string{string(k), string(v)})
		return true
	})
	if err != nil && err != io.EOF {
		t.Fatalf("scan: %v", err)
	}
	// 期望存在 a2, b1，不存在 a1
	found := map[string]string{}
	for _, it := range items {
		found[it[0]] = it[1]
	}
	if _, ok := found["a1"]; ok {
		t.Fatalf("a1 should be deleted")
	}
	if found["a2"] != "va2" {
		t.Fatalf("a2 value mismatch")
	}
	if found["b1"] != "vb1" {
		t.Fatalf("b1 value mismatch")
	}
	_ = db.Close()
}

func TestGRPCTokenAuth(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "knocidb_grpc_auth")
	_ = os.RemoveAll(dir)
	opts := knocidb.DefaultOptions
	opts.DirPath = dir
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s, lis, err := Start("127.0.0.1:0", db,
		grpc.ChainUnaryInterceptor(authUnaryInterceptor("secret")),
		grpc.ChainStreamInterceptor(authStreamInterceptor("secret")),
	)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.GracefulStop()
	// 无token应失败
	cli1, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli1.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = cli1.Put(ctx, []byte("k"), []byte("v"))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expect unauthenticated, got %v", err)
	}
	// 携带token应成功
	cli2, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(tokenCredentials{token: "secret"}))
	if err != nil {
		t.Fatalf("dial secure: %v", err)
	}
	defer cli2.Close()
	if err = cli2.Put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, err := cli2.Get(ctx, []byte("k1"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("get: %v val=%s", err, string(v))
	}
	_ = db.Close()
}

func TestGRPCScanHashIndex(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "knocidb_grpc_hash_scan")
	_ = os.RemoveAll(dir)
	opts := knocidb.DefaultOptions
	opts.DirPath = dir
	opts.IndexType = knocidb.Hash
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s, lis, err := Start("127.0.0.1:0", db)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.GracefulStop()
	cli, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = cli.Scan(ctx, []byte("a"), []byte("z"), func(k, v []byte) bool { return true })
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expect failed precondition, got %v", err)
	}
	_ = db.Close()
}

func TestGRPCInvalidPayloads(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "knocidb_grpc_invalid")
	_ = os.RemoveAll(dir)
	opts := knocidb.DefaultOptions
	opts.DirPath = dir
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s, lis, err := Start("127.0.0.1:0", db)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.GracefulStop()
	// 原始客户端
	cli, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()
	raw := NewKVServiceClient(cli)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Put 载荷错误
	_, err = raw.Put(ctx, &wrapperspb.BytesValue{Value: []byte{1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expect invalid arg, got %v", err)
	}
	// Batch 未知操作码
	bc, err := raw.Batch(ctx)
	if err != nil {
		t.Fatalf("batch open: %v", err)
	}
	// op=3, klen=1, key=0x61
	bad := []byte{3, 1, 0, 0, 0, 'a'}
	if err = bc.Send(&wrapperspb.BytesValue{Value: bad}); err != nil {
		t.Fatalf("batch send: %v", err)
	}
	_, err = bc.CloseAndRecv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expect invalid arg, got %v", err)
	}
	// Scan 载荷错误
	sc, err := raw.Scan(ctx, &wrapperspb.BytesValue{Value: []byte{1}})
	if err != nil {
		t.Fatalf("scan open: %v", err)
	}
	_, err = sc.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expect invalid arg, got %v", err)
	}
	_ = db.Close()
}

func TestGRPCDBClosed(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "knocidb_grpc_closed")
	_ = os.RemoveAll(dir)
	opts := knocidb.DefaultOptions
	opts.DirPath = dir
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s, lis, err := Start("127.0.0.1:0", db)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.GracefulStop()
	cli, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()
	_ = db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = cli.Put(ctx, []byte("k"), []byte("v"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expect unavailable, got %v", err)
	}
}
