package redis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/knoci/knocidb"
	"github.com/stretchr/testify/require"
)

type testError string

func (e testError) Error() string { return string(e) }

func newTestServer(t *testing.T, options ...ServerOption) (*Server, *knocidb.DB, net.Conn) {
	t.Helper()
	dbOptions := knocidb.DefaultOptions
	dbOptions.DirPath = t.TempDir()
	db, err := knocidb.Open(dbOptions)
	require.NoError(t, err)
	srv, lis, err := Start("127.0.0.1:0", db, options...)
	require.NoError(t, err)
	conn, err := net.Dial("tcp", lis.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
		_ = db.Close()
	})
	return srv, db, conn
}

func sendCommand(t *testing.T, conn net.Conn, args ...string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := io.WriteString(conn, b.String())
	require.NoError(t, err)
}

func readValue(t *testing.T, r *bufio.Reader) any {
	t.Helper()
	prefix, err := r.ReadByte()
	require.NoError(t, err)
	line := func() string {
		s, err := r.ReadString('\n')
		require.NoError(t, err)
		return strings.TrimSuffix(strings.TrimSuffix(s, "\n"), "\r")
	}
	switch prefix {
	case '+':
		return line()
	case '-':
		return testError(line())
	case ':':
		n, err := strconv.ParseInt(line(), 10, 64)
		require.NoError(t, err)
		return n
	case '$':
		n, err := strconv.Atoi(line())
		require.NoError(t, err)
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		_, err = io.ReadFull(r, buf)
		require.NoError(t, err)
		return string(buf[:n])
	case '*':
		n, err := strconv.Atoi(line())
		require.NoError(t, err)
		if n < 0 {
			return nil
		}
		out := make([]any, n)
		for i := range out {
			out[i] = readValue(t, r)
		}
		return out
	default:
		t.Fatalf("unexpected RESP prefix %q", prefix)
		return nil
	}
}

func do(t *testing.T, conn net.Conn, args ...string) any {
	t.Helper()
	sendCommand(t, conn, args...)
	return readValue(t, bufio.NewReader(conn))
}

func TestServerBasicsAndDataTypes(t *testing.T) {
	_, _, conn := newTestServer(t)

	require.Equal(t, "PONG", do(t, conn, "PING"))
	require.Equal(t, "hello", do(t, conn, "ECHO", "hello"))

	require.Equal(t, "OK", do(t, conn, "SET", "name", "knoci"))
	require.Equal(t, "knoci", do(t, conn, "GET", "name"))
	require.Equal(t, int64(1), do(t, conn, "EXISTS", "name"))
	require.Equal(t, "string", do(t, conn, "TYPE", "name"))
	require.Equal(t, "OK", do(t, conn, "MSET", "m1", "v1", "m2", "v2"))
	require.Equal(t, []any{"v1", "v2"}, do(t, conn, "MGET", "m1", "m2"))

	require.Equal(t, int64(2), do(t, conn, "HSET", "user:1", "name", "Alice", "age", "20"))
	require.Equal(t, "Alice", do(t, conn, "HGET", "user:1", "name"))
	require.Equal(t, []any{"age", "20", "name", "Alice"}, do(t, conn, "HGETALL", "user:1"))
	require.Equal(t, "hash", do(t, conn, "TYPE", "user:1"))
	require.Equal(t, []any{"knoci", nil}, do(t, conn, "MGET", "name", "user:1"))

	require.Equal(t, int64(3), do(t, conn, "RPUSH", "queue", "a", "b", "c"))
	require.Equal(t, []any{"a", "b", "c"}, do(t, conn, "LRANGE", "queue", "0", "-1"))
	require.Equal(t, "a", do(t, conn, "LPOP", "queue"))
	require.Equal(t, int64(2), do(t, conn, "LLEN", "queue"))

	require.Equal(t, int64(2), do(t, conn, "SADD", "tags", "go", "db", "go"))
	require.Equal(t, int64(1), do(t, conn, "SISMEMBER", "tags", "go"))
	require.ElementsMatch(t, []any{"go", "db"}, do(t, conn, "SMEMBERS", "tags"))

	require.Equal(t, int64(1), do(t, conn, "EXPIRE", "name", "60"))
	ttl := do(t, conn, "TTL", "name")
	require.NotEqual(t, int64(-1), ttl)
	require.NotEqual(t, int64(-2), ttl)
	require.Equal(t, int64(1), do(t, conn, "PERSIST", "name"))
	require.Equal(t, int64(-1), do(t, conn, "TTL", "name"))

	_, isErr := do(t, conn, "LPUSH", "name", "x").(testError)
	require.True(t, isErr, "wrong type should return WRONGTYPE")
	require.Equal(t, int64(1), do(t, conn, "DEL", "name"))
	require.Equal(t, "none", do(t, conn, "TYPE", "name"))
}

func TestServerTransactionUnsupported(t *testing.T) {
	_, _, conn := newTestServer(t)
	_, isErr := do(t, conn, "MULTI").(testError)
	require.True(t, isErr)
}

func TestServerAuthAndHello(t *testing.T) {
	_, _, conn := newTestServer(t, WithPassword("secret"))

	_, isErr := do(t, conn, "GET", "k").(testError)
	require.True(t, isErr)
	_, isErr = do(t, conn, "GET", "k").(testError)
	require.True(t, isErr)

	require.Equal(t, "OK", do(t, conn, "AUTH", "secret"))
	require.Equal(t, "PONG", do(t, conn, "PING"))

	hello, ok := do(t, conn, "HELLO", "2").([]any)
	require.True(t, ok)
	require.Contains(t, hello, "knocidb")
}

func TestServerExtendedCommands(t *testing.T) {
	_, _, conn := newTestServer(t)

	require.Equal(t, int64(1), do(t, conn, "INCR", "counter"))
	require.Equal(t, int64(3), do(t, conn, "INCRBY", "counter", "2"))
	require.Equal(t, int64(2), do(t, conn, "DECR", "counter"))
	require.Equal(t, int64(1), do(t, conn, "DECRBY", "counter", "1"))
	require.Equal(t, int64(1), do(t, conn, "APPEND", "text", "a"))
	require.Equal(t, int64(2), do(t, conn, "APPEND", "text", "b"))
	require.Equal(t, int64(2), do(t, conn, "STRLEN", "text"))

	require.Equal(t, "OK", do(t, conn, "SET", "opt", "v1", "EX", "60"))
	require.Nil(t, do(t, conn, "SET", "opt", "v2", "NX"))
	require.Equal(t, "OK", do(t, conn, "SET", "opt", "v3", "XX"))
	require.Equal(t, "v3", do(t, conn, "GET", "opt"))
	require.Equal(t, int64(0), do(t, conn, "PERSIST", "opt"))

	require.Equal(t, int64(2), do(t, conn, "HSET", "h", "a", "1", "b", "2"))
	require.Equal(t, []any{"a", "b"}, do(t, conn, "HKEYS", "h"))
	require.Equal(t, []any{"1", "2"}, do(t, conn, "HVALS", "h"))
	require.Equal(t, int64(1), do(t, conn, "HEXISTS", "h", "a"))
	require.Equal(t, int64(2), do(t, conn, "HLEN", "h"))
	require.Equal(t, int64(1), do(t, conn, "HDEL", "h", "a"))

	require.Equal(t, int64(1), do(t, conn, "LPUSH", "list", "a"))
	require.Equal(t, int64(2), do(t, conn, "RPUSH", "list", "b"))
	require.Equal(t, int64(2), do(t, conn, "LLEN", "list"))
	require.Equal(t, "a", do(t, conn, "LPOP", "list"))
	require.Equal(t, "b", do(t, conn, "RPOP", "list"))

	require.Equal(t, int64(2), do(t, conn, "SADD", "set", "a", "b"))
	require.Equal(t, int64(2), do(t, conn, "SCARD", "set"))
	require.Equal(t, int64(1), do(t, conn, "SISMEMBER", "set", "a"))
	require.Equal(t, int64(1), do(t, conn, "SREM", "set", "a"))

	require.Equal(t, int64(2), do(t, conn, "ZADD", "z", "1", "a", "2", "b"))
	require.Equal(t, int64(2), do(t, conn, "ZCARD", "z"))
	require.Equal(t, "1", do(t, conn, "ZSCORE", "z", "a"))
	require.Equal(t, int64(0), do(t, conn, "ZRANK", "z", "a"))
	require.Equal(t, int64(0), do(t, conn, "ZREVRANK", "z", "b"))
	require.Equal(t, []any{"a", "b"}, do(t, conn, "ZRANGE", "z", "0", "-1"))
	require.Equal(t, []any{"b", "a"}, do(t, conn, "ZREVRANGE", "z", "0", "-1"))
	require.Equal(t, int64(1), do(t, conn, "ZCOUNT", "z", "1", "1"))
	require.Equal(t, "1.5", do(t, conn, "ZINCRBY", "z", "0.5", "a"))
	require.Equal(t, int64(1), do(t, conn, "ZREM", "z", "b"))

	require.NotNil(t, do(t, conn, "KEYS", "*"))
	require.NotNil(t, do(t, conn, "SCAN", "0"))
	require.NotNil(t, do(t, conn, "DBSIZE"))
	require.Equal(t, "string", do(t, conn, "TYPE", "opt"))
	require.NotNil(t, do(t, conn, "INFO"))
	require.Equal(t, "OK", do(t, conn, "CLIENT", "SETNAME", "test"))
	require.Nil(t, do(t, conn, "CLIENT", "GETNAME"))
	require.Equal(t, "OK", do(t, conn, "SELECT", "0"))
	require.NotNil(t, do(t, conn, "COMMAND"))
}
