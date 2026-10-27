package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/knoci/knocidb"
	knociredis "github.com/knoci/knocidb/redis"
)

func main() {
	dir := flag.String("dir", "./redis_data", "database dir path")
	addr := flag.String("addr", "127.0.0.1:6379", "redis listen addr")
	password := flag.String("password", "", "optional AUTH password")
	cluster := flag.Bool("cluster", false, "enable single-node Redis Cluster protocol")
	nodeID := flag.String("node-id", "knocidb-node-1", "cluster node id")
	flag.Parse()

	options := knocidb.DefaultOptions
	options.DirPath = *dir
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()

	opts := []knociredis.ServerOption{}
	if *password != "" {
		opts = append(opts, knociredis.WithPassword(*password))
	}
	if *cluster {
		opts = append(opts, knociredis.WithCluster(knociredis.ClusterOptions{Enabled: true, NodeID: *nodeID}))
	}
	srv, lis, err := knociredis.Start(*addr, db, opts...)
	if err != nil {
		panic(err)
	}
	defer func() { _ = srv.Close() }()

	fmt.Println("knociDB Redis-compatible server listening on", lis.Addr().String())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
