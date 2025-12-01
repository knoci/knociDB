package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/knoci/knocidb"
	knocigrpc "github.com/knoci/knocidb/grpc"
)

func main() {
	dir := flag.String("dir", "./grpc_data", "database dir path")
	addr := flag.String("addr", "127.0.0.1:50051", "grpc listen addr")
	tlsCert := flag.String("tls-cert", "", "server tls cert file")
	tlsKey := flag.String("tls-key", "", "server tls key file")
	tlsCA := flag.String("tls-ca", "", "client ca cert file for mTLS")
	mtls := flag.Bool("mtls", false, "enable mTLS and require client cert")
	token := flag.String("token", "", "enable token auth when set")
	flag.Parse()

	opts := knocidb.DefaultOptions
	opts.DirPath = *dir
	db, err := knocidb.Open(opts)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()

	var lisAddr string
	if (*tlsCert != "" && *tlsKey != "") || *token != "" || (*mtls && *tlsCA != "") {
		_, lis, err := knocigrpc.StartSecure(*addr, db, knocigrpc.ServerSecurityOptions{
			TLSCertFile:       *tlsCert,
			TLSKeyFile:        *tlsKey,
			TLSClientCAFile:   *tlsCA,
			RequireClientCert: *mtls,
			EnableAuth:        *token != "",
			AuthToken:         *token,
		})
		if err != nil {
			panic(err)
		}
		lisAddr = lis.Addr().String()
		fmt.Println("listening on", lisAddr)
	} else {
		gs, lis, err := knocigrpc.Start(*addr, db)
		if err != nil {
			panic(err)
		}
		lisAddr = lis.Addr().String()
		fmt.Println("listening on", lisAddr)
		_ = gs
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
