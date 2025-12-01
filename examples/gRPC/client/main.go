package main

import (
    "bufio"
    "context"
    "flag"
    "fmt"
    "os"
    "strings"
    "time"

    knocigrpc "github.com/knoci/knocidb/grpc"
)

func main() {
    addr := flag.String("addr", "127.0.0.1:50051", "grpc server addr")
    tlsCA := flag.String("tls-ca", "", "ca cert file")
    tlsServerName := flag.String("tls-server-name", "", "tls server name for verify")
    token := flag.String("token", "", "auth token")
    flag.Parse()

    var cli *knocigrpc.Client
    var err error
    if *tlsCA != "" || *token != "" {
        cli, err = knocigrpc.DialSecure(*addr, knocigrpc.ClientSecurityOptions{
            TLSCACertFile: *tlsCA,
            TLSServerName: *tlsServerName,
            EnableAuth:    *token != "",
            AuthToken:     *token,
        })
    } else {
        cli, err = knocigrpc.Dial(*addr)
    }
    if err != nil { fmt.Println("dial:", err); return }
    defer func() { _ = cli.Close() }()

    reader := bufio.NewReader(os.Stdin)
    var batchOps [][]byte
    inBatch := false

    fmt.Println("knociDB gRPC client. Type HELP for commands.")
    for {
        fmt.Print("> ")
        line, _ := reader.ReadString('\n')
        line = strings.TrimSpace(line)
        if line == "" { continue }
        parts := strings.Fields(line)
        cmd := strings.ToUpper(parts[0])
        args := parts[1:]
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        switch cmd {
        case "HELP":
            fmt.Println("GET key | PUT key value | DEL key [key...] | SCAN start end | BEGIN | BPUT key value | BDEL key | COMMIT | DISCARD | EXIT")
        case "EXIT":
            cancel(); return
        case "GET":
            if len(args) != 1 { fmt.Println("ERR: GET key"); cancel(); continue }
            v, err := cli.Get(ctx, []byte(args[0]))
            if err != nil { fmt.Println("ERR:", err) } else { fmt.Printf("\"%s\"\n", string(v)) }
        case "PUT":
            if len(args) != 2 { fmt.Println("ERR: PUT key value"); cancel(); continue }
            err := cli.Put(ctx, []byte(args[0]), []byte(args[1]))
            if err != nil { fmt.Println("ERR:", err) } else { fmt.Println("OK") }
        case "DEL":
            if len(args) < 1 { fmt.Println("ERR: DEL key [key...]"); cancel(); continue }
            deleted := 0
            for _, k := range args {
                if err := cli.Delete(ctx, []byte(k)); err == nil { deleted++ } else { fmt.Println("ERR:", err) }
            }
            fmt.Printf("(integer) %d\n", deleted)
        case "SCAN":
            if len(args) != 2 { fmt.Println("ERR: SCAN start end"); cancel(); continue }
            start := []byte(args[0])
            end := []byte(args[1])
            if args[0] == "-" { start = nil }
            if args[1] == "-" { end = nil }
            err := cli.Scan(ctx, start, end, func(k, v []byte) bool { fmt.Printf("%s=%s\n", string(k), string(v)); return true })
            if err != nil { fmt.Println("ERR:", err) }
        case "BEGIN":
            inBatch = true; batchOps = nil; fmt.Println("OK")
        case "BPUT":
            if !inBatch { fmt.Println("ERR: not in batch"); cancel(); continue }
            if len(args) != 2 { fmt.Println("ERR: BPUT key value"); cancel(); continue }
            batchOps = append(batchOps, knocigrpc.EncodeBatchPut([]byte(args[0]), []byte(args[1])))
        case "BDEL":
            if !inBatch { fmt.Println("ERR: not in batch"); cancel(); continue }
            if len(args) != 1 { fmt.Println("ERR: BDEL key"); cancel(); continue }
            batchOps = append(batchOps, knocigrpc.EncodeBatchDelete([]byte(args[0])))
        case "COMMIT":
            if !inBatch { fmt.Println("ERR: not in batch"); cancel(); continue }
            err := cli.Batch(ctx, batchOps)
            if err != nil { fmt.Println("ERR:", err) } else { fmt.Println("OK") }
            inBatch = false; batchOps = nil
        case "DISCARD":
            if !inBatch { fmt.Println("ERR: not in batch"); cancel(); continue }
            inBatch = false; batchOps = nil; fmt.Println("OK")
        default:
            fmt.Println("ERR: unknown command")
        }
        cancel()
    }
}
