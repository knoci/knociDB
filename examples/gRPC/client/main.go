package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	knocigrpc "github.com/knoci/knocidb/grpc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "grpc server addr")
	tlsEnabled := flag.Bool("tls", false, "use TLS with system trust roots when no CA file is given")
	tlsCA := flag.String("tls-ca", "", "ca cert file")
	tlsServerName := flag.String("tls-server-name", "", "tls server name for verify")
	clientCert := flag.String("client-cert", "", "client certificate file for mTLS")
	clientKey := flag.String("client-key", "", "client key file for mTLS")
	token := flag.String("token", "", "auth token")
	flag.Parse()

	var cli *knocigrpc.Client
	var err error
	if *tlsEnabled || *tlsCA != "" || *tlsServerName != "" || *clientCert != "" || *clientKey != "" || *token != "" {
		cli, err = knocigrpc.DialSecure(*addr, knocigrpc.ClientSecurityOptions{
			TLSCACertFile:  *tlsCA,
			TLSServerName:  *tlsServerName,
			ClientCertFile: *clientCert,
			ClientKeyFile:  *clientKey,
			EnableAuth:     *token != "",
			AuthToken:      *token,
		})
	} else {
		cli, err = knocigrpc.Dial(*addr)
	}
	if err != nil {
		fmt.Println("dial:", err)
		return
	}
	defer func() { _ = cli.Close() }()

	reader := bufio.NewReader(os.Stdin)
	var batchOps [][]byte
	inBatch := false

	fmt.Println("knociDB gRPC client. Type HELP for commands.")
	for {
		fmt.Print("> ")
		line, readErr := reader.ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			fmt.Println("read error:", readErr)
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			if errors.Is(readErr, io.EOF) {
				return
			}
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])
		args := parts[1:]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		switch cmd {
		case "HELP":
			fmt.Println("GET key | PUT key value | PUTTTL key value ttl_ms | TTL key | PERSIST key | DEL key [key...] | SCAN start end [limit] | HEALTH | BEGIN | BPUT key value | BDEL key | COMMIT | DISCARD | EXIT")
		case "EXIT":
			cancel()
			return
		case "HEALTH":
			st, err := cli.HealthCheck(ctx, "knocidb.KVService")
			if err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Println(st.String())
			}
		case "GET":
			if len(args) != 1 {
				fmt.Println("ERR: GET key")
				cancel()
				continue
			}
			v, err := cli.Get(ctx, []byte(args[0]))
			if err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Printf("\"%s\"\n", string(v))
			}
		case "PUT":
			if len(args) != 2 {
				fmt.Println("ERR: PUT key value")
				cancel()
				continue
			}
			err := cli.Put(ctx, []byte(args[0]), []byte(args[1]))
			if err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Println("OK")
			}
		case "PUTTTL":
			if len(args) != 3 {
				fmt.Println("ERR: PUTTTL key value ttl_ms")
				cancel()
				continue
			}
			ms, parseErr := strconv.ParseInt(args[2], 10, 64)
			if parseErr != nil || ms <= 0 {
				fmt.Println("ERR: ttl_ms must be positive integer")
				cancel()
				continue
			}
			if err := cli.PutWithTTL(ctx, []byte(args[0]), []byte(args[1]), time.Duration(ms)*time.Millisecond); err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Println("OK")
			}
		case "TTL":
			if len(args) != 1 {
				fmt.Println("ERR: TTL key")
				cancel()
				continue
			}
			left, hasTTL, err := cli.GetTTL(ctx, []byte(args[0]))
			if err != nil {
				fmt.Println("ERR:", err)
			} else if !hasTTL {
				fmt.Println("-1")
			} else {
				fmt.Printf("%d ms\n", left.Milliseconds())
			}
		case "PERSIST":
			if len(args) != 1 {
				fmt.Println("ERR: PERSIST key")
				cancel()
				continue
			}
			ok, err := cli.Persist(ctx, []byte(args[0]))
			if err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Println(ok)
			}
		case "DEL":
			if len(args) < 1 {
				fmt.Println("ERR: DEL key [key...]")
				cancel()
				continue
			}
			deleted := 0
			for _, k := range args {
				if err := cli.Delete(ctx, []byte(k)); err == nil {
					deleted++
				} else {
					fmt.Println("ERR:", err)
				}
			}
			fmt.Printf("(integer) %d\n", deleted)
		case "SCAN":
			if len(args) < 2 || len(args) > 3 {
				fmt.Println("ERR: SCAN start end [limit]")
				cancel()
				continue
			}
			var limit uint32
			if len(args) == 3 {
				v, parseErr := strconv.ParseUint(args[2], 10, 32)
				if parseErr != nil {
					fmt.Println("ERR: invalid limit")
					cancel()
					continue
				}
				limit = uint32(v)
			}
			start := []byte(args[0])
			end := []byte(args[1])
			if args[0] == "-" {
				start = nil
			}
			if args[1] == "-" {
				end = nil
			}
			var err error
			if limit > 0 {
				err = cli.ScanLimit(ctx, start, end, limit, func(k, v []byte) bool { fmt.Printf("%s=%s\n", string(k), string(v)); return true })
			} else {
				err = cli.Scan(ctx, start, end, func(k, v []byte) bool { fmt.Printf("%s=%s\n", string(k), string(v)); return true })
			}
			if err != nil {
				fmt.Println("ERR:", err)
			}
		case "BEGIN":
			inBatch = true
			batchOps = nil
			fmt.Println("OK")
		case "BPUT":
			if !inBatch {
				fmt.Println("ERR: not in batch")
				cancel()
				continue
			}
			if len(args) != 2 {
				fmt.Println("ERR: BPUT key value")
				cancel()
				continue
			}
			batchOps = append(batchOps, knocigrpc.EncodeBatchPut([]byte(args[0]), []byte(args[1])))
		case "BDEL":
			if !inBatch {
				fmt.Println("ERR: not in batch")
				cancel()
				continue
			}
			if len(args) != 1 {
				fmt.Println("ERR: BDEL key")
				cancel()
				continue
			}
			batchOps = append(batchOps, knocigrpc.EncodeBatchDelete([]byte(args[0])))
		case "COMMIT":
			if !inBatch {
				fmt.Println("ERR: not in batch")
				cancel()
				continue
			}
			err := cli.Batch(ctx, batchOps)
			if err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Println("OK")
			}
			inBatch = false
			batchOps = nil
		case "DISCARD":
			if !inBatch {
				fmt.Println("ERR: not in batch")
				cancel()
				continue
			}
			inBatch = false
			batchOps = nil
			fmt.Println("OK")
		default:
			fmt.Println("ERR: unknown command")
		}
		cancel()
		if errors.Is(readErr, io.EOF) {
			return
		}
	}
}
