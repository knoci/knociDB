package redis

import (
	"bufio"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/knoci/knocidb"

	"github.com/stretchr/testify/require"
)

func TestStoreZSet(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	n, err := store.ZAdd([]byte("z"), []zEntry{
		{member: []byte("a"), score: 2},
		{member: []byte("b"), score: 1},
		{member: []byte("c"), score: 3},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
	card, err := store.ZCard([]byte("z"))
	require.NoError(t, err)
	require.Equal(t, int64(3), card)

	score, ok, err := store.ZScore([]byte("z"), []byte("b"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, float64(1), score)

	entries, err := store.ZRange([]byte("z"), 0, -1, false)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "a", "c"}, []string{string(entries[0].member), string(entries[1].member), string(entries[2].member)})
	rev, err := store.ZRange([]byte("z"), 0, -1, true)
	require.NoError(t, err)
	require.Equal(t, []string{"c", "a", "b"}, []string{string(rev[0].member), string(rev[1].member), string(rev[2].member)})

	rank, ok, err := store.ZRank([]byte("z"), []byte("a"), false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), rank)

	next, err := store.ZIncrBy([]byte("z"), []byte("b"), 5)
	require.NoError(t, err)
	require.Equal(t, float64(6), next)
	rank, _, _ = store.ZRank([]byte("z"), []byte("b"), false)
	require.Equal(t, int64(2), rank)

	removed, err := store.ZRem([]byte("z"), []byte("a"), []byte("a"))
	require.NoError(t, err)
	require.Equal(t, int64(1), removed)
}

func TestStoreKeysAndScan(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	_, err := store.Set([]byte("user:1"), []byte("a"), 0, false, false)
	require.NoError(t, err)
	_, err = store.Set([]byte("user:2"), []byte("b"), 0, false, false)
	require.NoError(t, err)
	_, err = store.HSet([]byte("hash:1"), []byte("f"), []byte("v"))
	require.NoError(t, err)

	keys, err := store.Keys("user:*")
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("user:1"), []byte("user:2")}, keys)

	cursor, batch, err := store.Scan(0, "*", 1, "")
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NotZero(t, cursor)
	_, nextBatch, err := store.Scan(cursor, "*", 10, "")
	require.NoError(t, err)
	require.Len(t, nextBatch, 2)

	_, typed, err := store.Scan(0, "*", 10, "hash")
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("hash:1")}, typed)
}

func TestServerZSetAndScan(t *testing.T) {
	_, _, conn := newTestServer(t)

	require.Equal(t, int64(2), do(t, conn, "ZADD", "z", "1", "a", "2", "b"))
	require.Equal(t, "1", do(t, conn, "ZSCORE", "z", "a"))
	require.Equal(t, []any{"a", "b"}, do(t, conn, "ZRANGE", "z", "0", "-1"))
	require.Equal(t, []any{"b", "2", "a", "1"}, do(t, conn, "ZREVRANGE", "z", "0", "-1", "WITHSCORES"))
	require.Equal(t, int64(0), do(t, conn, "ZRANK", "z", "a"))
	require.Equal(t, int64(2), do(t, conn, "ZADD", "z", "3", "c", "4", "d"))
	require.Equal(t, int64(4), do(t, conn, "ZCARD", "z"))
	require.Equal(t, int64(2), do(t, conn, "ZCOUNT", "z", "1", "2"))
	require.Equal(t, []any{"b", "2", "c", "3"}, do(t, conn, "ZRANGEBYSCORE", "z", "(1", "3", "WITHSCORES"))

	require.Equal(t, "OK", do(t, conn, "MSET", "k1", "v1", "k2", "v2"))
	keys := do(t, conn, "KEYS", "k*")
	require.ElementsMatch(t, []any{"k1", "k2"}, keys)
	scan, ok := do(t, conn, "SCAN", "0", "MATCH", "k*", "COUNT", "1").([]any)
	require.True(t, ok)
	require.Len(t, scan, 2)
	require.IsType(t, "", scan[0])
}

func TestServerPubSub(t *testing.T) {
	srv, _, conn1 := newTestServer(t)
	conn2, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	defer conn2.Close()
	r1 := bufio.NewReader(conn1)
	r2 := bufio.NewReader(conn2)

	sendCommand(t, conn1, "SUBSCRIBE", "news")
	ack, ok := readValue(t, r1).([]any)
	require.True(t, ok)
	require.Equal(t, []any{"subscribe", "news", int64(1)}, ack)

	sendCommand(t, conn2, "PUBLISH", "news", "hello")
	require.Equal(t, int64(1), readValue(t, r2))
	message, ok := readValue(t, r1).([]any)
	require.True(t, ok)
	require.Equal(t, []any{"message", "news", "hello"}, message)

	require.Equal(t, []any{"news"}, do(t, conn2, "PUBSUB", "CHANNELS"))
	require.Equal(t, []any{"news", int64(1)}, do(t, conn2, "PUBSUB", "NUMSUB", "news"))

	sendCommand(t, conn1, "UNSUBSCRIBE", "news")
	unsub, ok := readValue(t, r1).([]any)
	require.True(t, ok)
	require.Equal(t, []any{"unsubscribe", "news", int64(0)}, unsub)

	sendCommand(t, conn1, "PSUBSCRIBE", "news.*")
	pack, ok := readValue(t, r1).([]any)
	require.True(t, ok)
	require.Equal(t, []any{"psubscribe", "news.*", int64(1)}, pack)
	sendCommand(t, conn2, "PUBLISH", "news.world", "hi")
	require.Equal(t, int64(1), readValue(t, r2))
	pmessage, ok := readValue(t, r1).([]any)
	require.True(t, ok)
	require.Equal(t, []any{"pmessage", "news.*", "news.world", "hi"}, pmessage)
}

func TestServerTTLBackground(t *testing.T) {
	_, _, conn := newTestServer(t, WithExpiryInterval(10*time.Millisecond))
	require.Equal(t, "OK", do(t, conn, "SET", "ttl-key", "v", "PX", "30"))
	time.Sleep(200 * time.Millisecond)
	require.Nil(t, do(t, conn, "GET", "ttl-key"))
	require.Equal(t, []any{}, do(t, conn, "KEYS", "*"))
}

func TestServerClusterRouting(t *testing.T) {
	nodes := []ClusterNode{
		{ID: "n1", Host: "127.0.0.1", Port: 10600, StartSlot: 0, EndSlot: 8191},
		{ID: "n2", Host: "127.0.0.1", Port: 10601, StartSlot: 8192, EndSlot: 16383},
	}
	start := func(id, addr string) (*Server, *knocidb.DB, net.Conn) {
		options := knocidb.DefaultOptions
		options.DirPath = t.TempDir()
		db, err := knocidb.Open(options)
		require.NoError(t, err)
		srv, lis, err := Start(addr, db, WithCluster(ClusterOptions{Enabled: true, NodeID: id, Nodes: nodes}))
		require.NoError(t, err)
		conn, err := net.Dial("tcp", lis.Addr().String())
		require.NoError(t, err)
		return srv, db, conn
	}
	srv1, db1, conn1 := start("n1", "127.0.0.1:10600")
	srv2, db2, conn2 := start("n2", "127.0.0.1:10601")
	defer func() {
		_ = conn1.Close()
		_ = conn2.Close()
		_ = srv1.Close()
		_ = srv2.Close()
		_ = db1.Close()
		_ = db2.Close()
	}()

	var local, remote string
	for i := 0; i < 1000 && (local == "" || remote == ""); i++ {
		key := "key-" + strconv.Itoa(i)
		slot := clusterSlot([]byte(key))
		if slot <= 8191 {
			local = key
		} else {
			remote = key
		}
	}
	require.NotEmpty(t, local)
	require.NotEmpty(t, remote)

	require.Equal(t, "OK", do(t, conn1, "SET", local, "v"))
	cross, ok := do(t, conn1, "MGET", local, remote).(testError)
	require.True(t, ok)
	require.Contains(t, cross.Error(), "CROSSSLOT")
	moved, ok := do(t, conn1, "SET", remote, "v").(testError)
	require.True(t, ok)
	require.Contains(t, moved.Error(), "MOVED")
	require.Equal(t, "OK", do(t, conn2, "SET", remote, "v"))
	require.Equal(t, "v", do(t, conn2, "GET", remote))
	slots, ok := do(t, conn1, "CLUSTER", "SLOTS").([]any)
	require.True(t, ok)
	require.Len(t, slots, 2)
}

func TestServerCluster(t *testing.T) {
	_, _, conn := newTestServer(t, WithCluster(ClusterOptions{Enabled: true, NodeID: "node-test"}))
	info := do(t, conn, "CLUSTER", "INFO")
	text, ok := info.(string)
	require.True(t, ok)
	require.Contains(t, text, "cluster_state:ok")
	require.Equal(t, "node-test", do(t, conn, "CLUSTER", "MYID"))
	slots, ok := do(t, conn, "CLUSTER", "SLOTS").([]any)
	require.True(t, ok)
	require.Len(t, slots, 1)
	slot := do(t, conn, "CLUSTER", "KEYSLOT", "foo")
	require.Equal(t, int64(12182), slot)
	hashTag := do(t, conn, "CLUSTER", "KEYSLOT", "{user1000}.following")
	require.Equal(t, do(t, conn, "CLUSTER", "KEYSLOT", "{user1000}"), hashTag)
}
