package knocigrpc

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/knoci/knocidb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestP0_GRPCMaliciousLengthPrefixNoOOM(t *testing.T) {
	opts := knocidb.DefaultOptions
	opts.DirPath = t.TempDir()
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	s, lis, err := Start("127.0.0.1:0", db)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.GracefulStop()

	cc, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()
	raw := NewKVServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 4-byte 0xffffffff length prefix must fail with InvalidArgument without allocating 4 GiB.
	boom := make([]byte, 4)
	binary.LittleEndian.PutUint32(boom, 0xffffffff)

	if _, err := raw.Put(ctx, &wrapperspb.BytesValue{Value: boom}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put malicious klen: got %v, want InvalidArgument", err)
	}

	batchBoom := append([]byte{opPut}, boom...)
	bc, err := raw.Batch(ctx)
	if err != nil {
		t.Fatalf("batch open: %v", err)
	}
	if err := bc.Send(&wrapperspb.BytesValue{Value: batchBoom}); err != nil {
		t.Fatalf("batch send: %v", err)
	}
	if _, err := bc.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Batch malicious klen: got %v, want InvalidArgument", err)
	}

	sc, err := raw.Scan(ctx, &wrapperspb.BytesValue{Value: boom})
	if err != nil {
		t.Fatalf("scan open: %v", err)
	}
	if _, err := sc.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Scan malicious sLen: got %v, want InvalidArgument", err)
	}
}

func TestP0_GRPCBatchLimitExceededAndEarlyScanCancel(t *testing.T) {
	opts := knocidb.DefaultOptions
	opts.DirPath = t.TempDir()
	db, err := knocidb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	srv := NewServer(db)
	srv.MaxBatchOps = 2

	lis, err := ListenOn("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	RegisterKVServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	defer gs.GracefulStop()

	cli, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Exceeding MaxBatchOps must return ResourceExhausted.
	ops := [][]byte{
		EncodeBatchPut([]byte("k1"), []byte("v1")),
		EncodeBatchPut([]byte("k2"), []byte("v2")),
		EncodeBatchPut([]byte("k3"), []byte("v3")),
	}
	if err := cli.Batch(ctx, ops); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("want ResourceExhausted, got %v", err)
	}

	// Valid 2-op batch + early Scan cancel.
	if err := cli.Batch(ctx, ops[:2]); err != nil {
		t.Fatalf("valid batch failed: %v", err)
	}
	count := 0
	if err := cli.Scan(ctx, nil, nil, func(k, v []byte) bool {
		count++
		return false
	}); err != nil {
		t.Fatalf("early-cancel scan failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 scanned entry before cancel, got %d", count)
	}
}
