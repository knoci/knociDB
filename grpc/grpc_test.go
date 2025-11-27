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
    if err != nil { t.Fatalf("open: %v", err) }
    s, lis, err := Start("127.0.0.1:0", db)
    if err != nil { t.Fatalf("start: %v", err) }
    defer s.GracefulStop()
    cli, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil { t.Fatalf("dial: %v", err) }
    defer cli.Close()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    ops := [][]byte{
        EncodeBatchPut([]byte("a1"), []byte("va1")),
        EncodeBatchPut([]byte("a2"), []byte("va2")),
        EncodeBatchDelete([]byte("a1")),
        EncodeBatchPut([]byte("b1"), []byte("vb1")),
    }
    if err := cli.Batch(ctx, ops); err != nil { t.Fatalf("batch: %v", err) }
    // 扫描 ["a", "bz")
    var items [][2]string
    err = cli.Scan(ctx, []byte("a"), []byte("bz"), func(k, v []byte) bool {
        items = append(items, [2]string{string(k), string(v)})
        return true
    })
    if err != nil && err != io.EOF { t.Fatalf("scan: %v", err) }
    // 期望存在 a2, b1，不存在 a1
    found := map[string]string{}
    for _, it := range items { found[it[0]] = it[1] }
    if _, ok := found["a1"]; ok { t.Fatalf("a1 should be deleted") }
    if found["a2"] != "va2" { t.Fatalf("a2 value mismatch") }
    if found["b1"] != "vb1" { t.Fatalf("b1 value mismatch") }
    _ = db.Close()
}
