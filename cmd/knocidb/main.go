package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/knoci/knocidb"
	knocigrpc "github.com/knoci/knocidb/grpc"
)

func main() {
	var (
		mode           = flag.String("mode", "standalone", "run mode: standalone or cluster")
		dataDir        = flag.String("data-dir", "./data", "database data directory")
		grpcAddr       = flag.String("grpc-addr", ":9090", "gRPC listen address (standalone mode)")
		adminAddr      = flag.String("admin-addr", ":9091", "admin HTTP listen address (/healthz, /readyz, /metrics, /debug/pprof)")
		syncWrite      = flag.Bool("sync", false, "fsync WAL on every write")
		partitions     = flag.Int("partitions", 5, "local storage partition count")
		nodeID         = flag.Uint64("node-id", 1, "Raft node ID (cluster mode)")
		clusterID      = flag.Uint64("cluster-id", 1, "base Raft cluster/shard group ID (cluster mode)")
		raftAddr       = flag.String("raft-addr", "127.0.0.1:63001", "Dragonboat Raft listen address")
		rpcAddr        = flag.String("rpc-addr", "127.0.0.1:63101", "internal cluster RPC address")
		initialMembers = flag.String("initial-members", "", "comma-separated id=raftAddr pairs (e.g. 1=127.0.0.1:63001,2=127.0.0.1:63002)")
		peerRPC        = flag.String("peer-rpc", "", "comma-separated id=rpcAddr pairs")
		shards         = flag.Int("shards", 1, "Multi-Raft shard count")
		join           = flag.Bool("join", false, "join an existing cluster")
		mtls           = flag.Bool("mtls", false, "enable Mutual TLS for Raft and Cluster RPC")
		caFile         = flag.String("ca-file", "", "mTLS CA certificate path")
		certFile       = flag.String("cert-file", "", "mTLS node certificate path")
		keyFile        = flag.String("key-file", "", "mTLS node private key path")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	dbOpts := knocidb.NewDefaultOptions(*dataDir)
	dbOpts.Sync = *syncWrite
	dbOpts.PartitionNum = *partitions

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch strings.ToLower(*mode) {
	case "standalone":
		db, err := knocidb.Open(dbOpts)
		if err != nil {
			logger.Error("failed to open standalone db", "err", err)
			os.Exit(1)
		}
		grpcSrv, grpcLis, err := knocigrpc.Start(*grpcAddr, db)
		if err != nil {
			_ = db.Close()
			logger.Error("failed to start gRPC server", "addr", *grpcAddr, "err", err)
			os.Exit(1)
		}
		adminSrv, adminLis, err := knocidb.StartAdminServer(*adminAddr, db, nil)
		if err != nil {
			grpcSrv.GracefulStop()
			_ = db.Close()
			logger.Error("failed to start admin server", "addr", *adminAddr, "err", err)
			os.Exit(1)
		}
		logger.Info("knocidb standalone started",
			"data_dir", *dataDir,
			"grpc_addr", grpcLis.Addr().String(),
			"admin_addr", adminLis.Addr().String(),
		)

		<-ctx.Done()
		logger.Info("shutting down knocidb standalone")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = adminSrv.Shutdown(shutCtx)
		grpcSrv.GracefulStop()
		if err := db.Close(); err != nil {
			logger.Error("error closing db", "err", err)
			os.Exit(1)
		}

	case "cluster":
		cfg := knocidb.DefaultClusterOptions(*nodeID, *clusterID, *raftAddr, filepath.Join(*dataDir, "raft"))
		cfg.ShardCount = *shards
		cfg.JoinCluster = *join
		cfg.RPCAddress = *rpcAddr
		cfg.MutualTLS = *mtls
		cfg.CAFile = *caFile
		cfg.CertFile = *certFile
		cfg.KeyFile = *keyFile
		if *initialMembers != "" {
			cfg.InitialMembers = parsePeerMap(*initialMembers, logger)
		}
		if *peerRPC != "" {
			cfg.PeerRPC = parsePeerMap(*peerRPC, logger)
		}
		dbOpts.DirPath = filepath.Join(*dataDir, "db")
		dbOpts.SnapshotPath = filepath.Join(*dataDir, "snapshots")

		cluster, err := knocidb.OpenCluster(cfg, dbOpts)
		if err != nil {
			logger.Error("failed to open cluster node", "err", err)
			os.Exit(1)
		}
		adminSrv, adminLis, err := knocidb.StartAdminServer(*adminAddr, nil, cluster)
		if err != nil {
			_ = cluster.Close()
			logger.Error("failed to start admin server", "addr", *adminAddr, "err", err)
			os.Exit(1)
		}
		logger.Info("knocidb cluster node started",
			"node_id", *nodeID,
			"raft_addr", *raftAddr,
			"rpc_addr", *rpcAddr,
			"shards", *shards,
			"admin_addr", adminLis.Addr().String(),
		)

		<-ctx.Done()
		logger.Info("shutting down knocidb cluster node")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = adminSrv.Shutdown(shutCtx)
		if err := cluster.Close(); err != nil {
			logger.Error("error closing cluster node", "err", err)
			os.Exit(1)
		}

	default:
		logger.Error("unsupported mode (use standalone or cluster)", "mode", *mode)
		os.Exit(2)
	}
}

func parsePeerMap(raw string, logger *slog.Logger) map[uint64]string {
	out := make(map[uint64]string)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(pair, "=")
		if !ok {
			logger.Warn("ignoring invalid peer mapping", "entry", pair)
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			logger.Warn("ignoring invalid peer node id", "entry", pair, "err", err)
			continue
		}
		out[id] = strings.TrimSpace(addr)
	}
	return out
}
