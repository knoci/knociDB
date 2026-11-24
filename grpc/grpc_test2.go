package knocigrpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/knoci/knocidb"
	"github.com/stretchr/testify/require"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestM3_GRPCHealthTTLAndScanLimit(t *testing.T) {
	opts := knocidb.DefaultOptions
	opts.DirPath = t.TempDir()
	db, err := knocidb.Open(opts)
	require.NoError(t, err)
	defer db.Close()

	srv, lis, err := Start("127.0.0.1:0", db)
	require.NoError(t, err)
	defer srv.GracefulStop()

	cli, err := Dial(lis.Addr().String())
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Standard grpc.health.v1 check
	st, err := cli.HealthCheck(ctx, "")
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, st)

	st, err = cli.HealthCheck(ctx, "knocidb.KVService")
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, st)

	// TTL RPCs: PutWithTTL, GetTTL, Persist
	require.NoError(t, cli.PutWithTTL(ctx, []byte("k-ttl"), []byte("v-ttl"), 5*time.Second))
	left, hasTTL, err := cli.GetTTL(ctx, []byte("k-ttl"))
	require.NoError(t, err)
	require.True(t, hasTTL)
	require.Greater(t, left, time.Duration(0))

	ok, err := cli.Persist(ctx, []byte("k-ttl"))
	require.NoError(t, err)
	require.True(t, ok)

	_, hasTTL, err = cli.GetTTL(ctx, []byte("k-ttl"))
	require.NoError(t, err)
	require.False(t, hasTTL)

	// Paginated ScanLimit
	for i := 0; i < 10; i++ {
		require.NoError(t, cli.Put(ctx, []byte(fmt.Sprintf("page-%02d", i)), []byte("val")))
	}
	var keys []string
	err = cli.ScanLimit(ctx, []byte("page-00"), []byte("page-99"), 3, func(k, v []byte) bool {
		keys = append(keys, string(k))
		return true
	})
	require.NoError(t, err)
	require.Equal(t, []string{"page-00", "page-01", "page-02"}, keys)
}
