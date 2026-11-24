package knocidb

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestM4_DBStatsAndAdminEndpoints(t *testing.T) {
	opts := NewDefaultOptions(t.TempDir())
	opts.TTLScanInterval = 0
	db, err := Open(opts)
	require.NoError(t, err)

	require.NoError(t, db.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, db.PutWithTTL([]byte("k2"), []byte("v2"), 10*time.Second))

	st := db.Stats()
	require.False(t, st.Closed)
	require.Equal(t, opts.PartitionNum, st.Shards)
	require.Greater(t, st.ActiveMemtableBytes, uint64(0))
	require.GreaterOrEqual(t, st.TTLTrackedKeys, 1)
	require.GreaterOrEqual(t, st.Sequence, uint64(3))

	handler := NewAdminHandler(db, nil)

	// /healthz -> 200
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// /readyz -> 200
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// /metrics -> Prometheus text exposition
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	text := string(body)
	require.True(t, strings.Contains(text, "knocidb_active_memtable_bytes"))
	require.True(t, strings.Contains(text, "knocidb_mvcc_sequence"))
	require.True(t, strings.Contains(text, "knocidb_ttl_tracked_keys"))

	// /debug/pprof/cmdline -> 200
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/cmdline", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// After Close(), /healthz and /readyz must return 503 Service Unavailable
	require.NoError(t, db.Close())
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestM4_DiskIOZeroWindowSafeInit(t *testing.T) {
	dio := &DiskIO{
		targetPath:       t.TempDir(),
		samplingInterval: 0,
		windowSize:       0,
		busyRate:         0.5,
	}
	dio.Init()
	require.Equal(t, 10, dio.windowSize)
	require.Equal(t, 100, dio.samplingInterval)
	free, err := dio.IsFree()
	require.NoError(t, err)
	require.True(t, free)

	// Verify directory boundary matching: /data must not match /data2
	require.False(t, isPathOnDevice("/data2/file", "/data"))
	require.True(t, isPathOnDevice("/data/file", "/data"))
}
