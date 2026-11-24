package knocidb

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

// DBStats 提供单机存储引擎的实时运行指标快照，供监控、健康检查与 Prometheus 导出使用。
type DBStats struct {
	Shards              int
	ActiveMemtableBytes uint64
	ImmuMemtableCount   int
	VLogTotalEntries    uint64
	VLogDiscardEntries  uint64
	VLogDiscardRatio    float64
	TTLTrackedKeys      int
	Sequence            uint64
	Closed              bool
	FlushError          string
}

// Stats 返回当前 DB 的实时运行指标快照。
func (db *DB) Stats() DBStats {
	st := DBStats{
		Shards:   len(db.shards),
		Sequence: db.seq.Load(),
		Closed:   db.closed.Load(),
	}
	if err := db.flushError(); err != nil {
		st.FlushError = err.Error()
	}
	if !st.Closed {
		for _, s := range db.shards {
			s.mu.RLock()
			if s.active != nil && s.active.skl != nil {
				st.ActiveMemtableBytes += uint64(s.active.skl.MemSize())
			}
			st.ImmuMemtableCount += len(s.immu)
			s.mu.RUnlock()
		}
		if db.vlog != nil {
			total, discard := db.vlog.stats()
			st.VLogTotalEntries = total
			st.VLogDiscardEntries = discard
			if total > 0 {
				st.VLogDiscardRatio = float64(discard) / float64(total)
			}
		}
		if db.ttlIndex != nil {
			db.ttlIndex.mu.RLock()
			st.TTLTrackedKeys = len(db.ttlIndex.entries)
			db.ttlIndex.mu.RUnlock()
		}
	}
	return st
}

// NewAdminHandler 构建独立 HTTP 运维处理器（不污染 http.DefaultServeMux），提供：
// - GET /healthz      存活探针
// - GET /readyz       就绪探针（校验 DB 未关闭、无 Flush 致命错误、集群已启动）
// - GET /metrics      Prometheus 文本格式指标
// - GET /debug/pprof/ Go pprof 性能剖析端点
func NewAdminHandler(db *DB, cluster *ClusterDB) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if db != nil && db.closed.Load() {
			http.Error(w, "db closed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if db != nil {
			st := db.Stats()
			if st.Closed || st.FlushError != "" {
				http.Error(w, "db not ready: "+st.FlushError, http.StatusServiceUnavailable)
				return
			}
		}
		if cluster != nil {
			if !cluster.state.Load() {
				http.Error(w, "cluster not started", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		fmt.Fprintf(w, "# TYPE knocidb_go_goroutines gauge\nknocidb_go_goroutines %d\n", runtime.NumGoroutine())
		fmt.Fprintf(w, "# TYPE knocidb_go_heap_alloc_bytes gauge\nknocidb_go_heap_alloc_bytes %d\n", ms.HeapAlloc)

		if db != nil {
			st := db.Stats()
			fmt.Fprintf(w, "# TYPE knocidb_shards gauge\nknocidb_shards %d\n", st.Shards)
			fmt.Fprintf(w, "# TYPE knocidb_active_memtable_bytes gauge\nknocidb_active_memtable_bytes %d\n", st.ActiveMemtableBytes)
			fmt.Fprintf(w, "# TYPE knocidb_immutable_memtables gauge\nknocidb_immutable_memtables %d\n", st.ImmuMemtableCount)
			fmt.Fprintf(w, "# TYPE knocidb_vlog_total_entries gauge\nknocidb_vlog_total_entries %d\n", st.VLogTotalEntries)
			fmt.Fprintf(w, "# TYPE knocidb_vlog_discard_entries gauge\nknocidb_vlog_discard_entries %d\n", st.VLogDiscardEntries)
			fmt.Fprintf(w, "# TYPE knocidb_vlog_discard_ratio gauge\nknocidb_vlog_discard_ratio %.6f\n", st.VLogDiscardRatio)
			fmt.Fprintf(w, "# TYPE knocidb_ttl_tracked_keys gauge\nknocidb_ttl_tracked_keys %d\n", st.TTLTrackedKeys)
			fmt.Fprintf(w, "# TYPE knocidb_mvcc_sequence counter\nknocidb_mvcc_sequence %d\n", st.Sequence)
		}
		if cluster != nil {
			cs := cluster.Stats()
			isLeader := 0
			if cs.IsLeader {
				isLeader = 1
			}
			fmt.Fprintf(w, "# TYPE knocidb_cluster_is_leader gauge\nknocidb_cluster_is_leader %d\n", isLeader)
			fmt.Fprintf(w, "# TYPE knocidb_cluster_rpc_requests_total counter\nknocidb_cluster_rpc_requests_total %d\n", cs.RPCRequests)
			fmt.Fprintf(w, "# TYPE knocidb_cluster_rpc_errors_total counter\nknocidb_cluster_rpc_errors_total %d\n", cs.RPCErrors)
			fmt.Fprintf(w, "# TYPE knocidb_cluster_forwarded_ops_total counter\nknocidb_cluster_forwarded_ops_total %d\n", cs.ForwardedOps)
		}
	})

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}

// StartAdminServer 在指定地址启动生产运维 HTTP 服务（/healthz, /readyz, /metrics, /debug/pprof/）。
func StartAdminServer(addr string, db *DB, cluster *ClusterDB) (*http.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	srv := &http.Server{
		Handler:           NewAdminHandler(db, cluster),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = srv.Serve(lis)
	}()
	return srv, lis, nil
}
