// Package redis provides a small Redis-compatible RESP server on top of knociDB.
//
// It is intentionally a subset: string, hash, list, set, zset, TTL, SCAN/KEYS,
// Pub/Sub, static Cluster routing and a few connection commands.
// Transactions, Streams, Cluster gossip/failover/replication and RESP3 are not implemented.
package redis

import (
	"bufio"
	"crypto/subtle"
	"errors"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/knoci/knocidb"
)

// ServerOption configures the Redis server.
type ServerOption func(*Server)

const defaultExpiryInterval = time.Second

// WithPassword enables a single-password AUTH check.
func WithPassword(password string) ServerOption {
	return func(s *Server) { s.password = password }
}

// WithExpiryInterval controls the background TTL sweeper interval.
// A non-positive value disables the sweeper.
func WithExpiryInterval(interval time.Duration) ServerOption {
	return func(s *Server) { s.expiryInterval = interval }
}

// Server is a Redis-compatible TCP server backed by knociDB.
type Server struct {
	db       *knocidb.DB
	store    *Store
	password string

	mu             sync.Mutex
	commandMu      sync.Mutex // serialize commands, matching Redis' single-threaded execution model
	listener       net.Listener
	conns          map[net.Conn]struct{}
	wg             sync.WaitGroup
	closeCh        chan struct{}
	closeOne       sync.Once
	hub            *pubsubHub
	expiryInterval time.Duration
	cluster        *ClusterOptions
}

type client struct {
	server        *Server
	conn          net.Conn
	writer        *bufio.Writer
	writeMu       sync.Mutex
	authenticated bool
	selectedDB    int

	subMu sync.Mutex
	subs  map[string]struct{}
	psubs map[string]struct{}
}

// Start listens on addr and serves RESP commands.
// The returned listener can be used to discover the actual port when addr uses :0.
func Start(addr string, db *knocidb.DB, options ...ServerOption) (*Server, net.Listener, error) {
	store, err := NewStore(db)
	if err != nil {
		return nil, nil, err
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	s := &Server{
		db:             db,
		store:          store,
		conns:          make(map[net.Conn]struct{}),
		closeCh:        make(chan struct{}),
		listener:       lis,
		hub:            newPubsubHub(),
		expiryInterval: defaultExpiryInterval,
	}
	for _, option := range options {
		option(s)
	}
	s.wg.Add(1)
	go s.acceptLoop()
	if s.expiryInterval > 0 {
		s.wg.Add(1)
		go s.expireLoop()
	}
	return s, lis, nil
}

// Addr returns the actual listen address.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close stops accepting connections and closes active clients.
func (s *Server) Close() error {
	var err error
	s.closeOne.Do(func() {
		close(s.closeCh)
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("redis accept failed: %v", err)
			continue
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) removeConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer s.removeConn(conn)
	defer conn.Close()

	c := &client{
		server:        s,
		conn:          conn,
		writer:        bufio.NewWriterSize(conn, 64*1024),
		authenticated: s.password == "",
		subs:          make(map[string]struct{}),
		psubs:         make(map[string]struct{}),
	}
	defer s.hub.removeClient(c)
	reader := bufio.NewReaderSize(conn, 64*1024)
	for {
		args, err := readCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			_ = c.sendError(err)
			return
		}
		value, quit, err := c.execute(args)
		if err != nil {
			if writeErr := c.sendError(err); writeErr != nil {
				return
			}
		} else if writeErr := c.send(value); writeErr != nil {
			return
		}
		if quit {
			return
		}
	}
}

func (c *client) send(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := writeRESP(c.writer, value); err != nil {
		return err
	}
	return c.writer.Flush()
}

func (c *client) sendError(err error) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if writeErr := writeError(c.writer, err); writeErr != nil {
		return writeErr
	}
	return c.writer.Flush()
}

func (c *client) subscriptionCount() int {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	return len(c.subs) + len(c.psubs)
}

func (s *Server) expireLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.expiryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.commandMu.Lock()
			if _, err := s.store.ExpireDue(nowMillis()); err != nil {
				log.Printf("redis ttl sweep failed: %v", err)
			}
			s.commandMu.Unlock()
		}
	}
}

func (c *client) execute(args [][]byte) (any, bool, error) {
	c.server.commandMu.Lock()
	defer c.server.commandMu.Unlock()
	if len(args) == 0 {
		return nil, false, errf("ERR empty command")
	}
	cmd := strings.ToLower(string(args[0]))
	if !c.authenticated {
		switch cmd {
		case "auth", "hello", "quit":
		default:
			return nil, false, errf("NOAUTH Authentication required.")
		}
	}
	if c.subscriptionCount() > 0 {
		switch cmd {
		case "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ping", "quit":
		default:
			return nil, false, errf("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context", cmd)
		}
	}
	if err := c.routeCluster(cmd, args); err != nil {
		return nil, false, err
	}

	switch cmd {
	case "ping":
		if len(args) > 2 {
			return nil, false, errf("ERR wrong number of arguments for 'ping' command")
		}
		if len(args) == 2 {
			return args[1], false, nil
		}
		return simpleString("PONG"), false, nil
	case "echo":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'echo' command")
		}
		return args[1], false, nil
	case "quit":
		return simpleString("OK"), true, nil
	case "auth":
		return c.auth(args)
	case "hello":
		return c.hello(args)
	case "select":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'select' command")
		}
		db, err := parseInt(args[1])
		if err != nil || db != 0 {
			return nil, false, errf("ERR DB index is out of range")
		}
		c.selectedDB = 0
		return simpleString("OK"), false, nil
	case "client":
		return c.clientCommand(args)
	case "command":
		return []any{}, false, nil
	case "info":
		return []byte("# Server\r\nredis_version:knocidb\r\nredis_mode:standalone\r\n"), false, nil
	case "set":
		return c.set(args)
	case "get":
		return c.get(args)
	case "setnx":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for 'setnx' command")
		}
		ok, err := c.server.store.Set(args[1], args[2], 0, true, false)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return int64(1), false, nil
		}
		return int64(0), false, nil
	case "del", "unlink":
		if len(args) < 2 {
			return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
		}
		n, err := c.server.store.Del(args[1:]...)
		return n, false, err
	case "exists":
		if len(args) < 2 {
			return nil, false, errf("ERR wrong number of arguments for 'exists' command")
		}
		n, err := c.server.store.Exists(args[1:]...)
		return n, false, err
	case "type":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'type' command")
		}
		t, err := c.server.store.Type(args[1])
		if err != nil {
			return nil, false, err
		}
		return simpleString(t), false, nil
	case "mset":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			return nil, false, errf("ERR wrong number of arguments for 'mset' command")
		}
		if err := c.server.store.MSet(args[1:]...); err != nil {
			return nil, false, err
		}
		return simpleString("OK"), false, nil
	case "mget":
		if len(args) < 2 {
			return nil, false, errf("ERR wrong number of arguments for 'mget' command")
		}
		out := make([]any, 0, len(args)-1)
		for _, key := range args[1:] {
			value, err := c.server.store.Get(key)
			if errors.Is(err, knocidb.ErrKeyNotFound) || errors.Is(err, ErrWrongType) {
				out = append(out, nil)
			} else if err != nil {
				return nil, false, err
			} else {
				out = append(out, value)
			}
		}
		return out, false, nil
	case "incr", "decr", "incrby", "decrby":
		return c.incr(cmd, args)
	case "append":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for 'append' command")
		}
		n, err := c.server.store.Append(args[1], args[2])
		return n, false, err
	case "strlen":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'strlen' command")
		}
		value, err := c.server.store.Get(args[1])
		if errors.Is(err, knocidb.ErrKeyNotFound) {
			return int64(0), false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return int64(len(value)), false, nil
	case "expire", "pexpire":
		return c.expire(cmd, args)
	case "ttl", "pttl":
		return c.ttl(cmd, args)
	case "persist":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'persist' command")
		}
		ok, err := c.server.store.Persist(args[1])
		if err != nil {
			return nil, false, err
		}
		if ok {
			return int64(1), false, nil
		}
		return int64(0), false, nil
	case "hset", "hmset":
		return c.hset(cmd, args)
	case "hget":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for 'hget' command")
		}
		value, err := c.server.store.HGet(args[1], args[2])
		if errors.Is(err, knocidb.ErrKeyNotFound) {
			return nil, false, nil
		}
		return value, false, err
	case "hdel":
		if len(args) < 3 {
			return nil, false, errf("ERR wrong number of arguments for 'hdel' command")
		}
		n, err := c.server.store.HDel(args[1], args[2:]...)
		return n, false, err
	case "hexists":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for 'hexists' command")
		}
		ok, err := c.server.store.HExists(args[1], args[2])
		if err != nil {
			return nil, false, err
		}
		if ok {
			return int64(1), false, nil
		}
		return int64(0), false, nil
	case "hlen":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'hlen' command")
		}
		n, err := c.server.store.HLen(args[1])
		return n, false, err
	case "hgetall", "hkeys", "hvals":
		return c.hgetall(cmd, args)
	case "lpush", "rpush":
		return c.push(cmd, args)
	case "lpop", "rpop":
		return c.pop(cmd, args)
	case "llen":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'llen' command")
		}
		n, err := c.server.store.LLen(args[1])
		return n, false, err
	case "lrange":
		return c.lrange(args)
	case "sadd":
		if len(args) < 3 {
			return nil, false, errf("ERR wrong number of arguments for 'sadd' command")
		}
		n, err := c.server.store.SAdd(args[1], args[2:]...)
		return n, false, err
	case "srem":
		if len(args) < 3 {
			return nil, false, errf("ERR wrong number of arguments for 'srem' command")
		}
		n, err := c.server.store.SRem(args[1], args[2:]...)
		return n, false, err
	case "smembers":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'smembers' command")
		}
		members, err := c.server.store.SMembers(args[1])
		return toAnyArray(members), false, err
	case "sismember":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for 'sismember' command")
		}
		ok, err := c.server.store.SIsMember(args[1], args[2])
		if err != nil {
			return nil, false, err
		}
		if ok {
			return int64(1), false, nil
		}
		return int64(0), false, nil
	case "scard":
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for 'scard' command")
		}
		n, err := c.server.store.SCard(args[1])
		return n, false, err
	case "zadd":
		return c.zadd(args)
	case "zscore":
		return c.zscore(args)
	case "zrem":
		return c.zrem(args)
	case "zcard":
		return c.zcard(args)
	case "zrange", "zrevrange":
		return c.zrange(cmd, args)
	case "zrank", "zrevrank":
		return c.zrank(cmd, args)
	case "zincrby":
		return c.zincrby(args)
	case "zcount":
		return c.zcount(args)
	case "zrangebyscore":
		return c.zrangebyscore(args)
	case "keys":
		return c.keys(args)
	case "scan":
		return c.scan(args)
	case "dbsize":
		if len(args) != 1 {
			return nil, false, errf("ERR wrong number of arguments for 'dbsize' command")
		}
		keys, err := c.server.store.Keys("*")
		return int64(len(keys)), false, err
	case "subscribe", "psubscribe":
		return c.subscribe(cmd, args)
	case "unsubscribe", "punsubscribe":
		return c.unsubscribe(cmd, args)
	case "publish":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for 'publish' command")
		}
		return c.server.hub.publish(string(args[1]), args[2]), false, nil
	case "pubsub":
		return c.pubsub(args)
	case "cluster":
		return c.clusterCommand(args)
	case "readonly", "asking":
		if c.server.cluster == nil || !c.server.cluster.Enabled {
			return nil, false, errf("ERR This instance has cluster support disabled")
		}
		return simpleString("OK"), false, nil
	case "multi", "exec", "discard", "watch", "unwatch":
		return nil, false, errf("ERR MULTI/EXEC/WATCH is not supported in knociDB Redis Level 2")
	default:
		return nil, false, errf("ERR unknown command '%s'", string(args[0]))
	}
}

func (c *client) auth(args [][]byte) (any, bool, error) {
	if c.server.password == "" {
		return nil, false, errf("ERR Client sent AUTH, but no password is set")
	}
	var password []byte
	switch len(args) {
	case 2:
		password = args[1]
	case 3:
		password = args[2]
	default:
		return nil, false, errf("ERR wrong number of arguments for 'auth' command")
	}
	if subtle.ConstantTimeCompare(password, []byte(c.server.password)) != 1 {
		return nil, false, errf("ERR invalid password")
	}
	c.authenticated = true
	return simpleString("OK"), false, nil
}

func (c *client) hello(args [][]byte) (any, bool, error) {
	if len(args) >= 2 {
		proto, err := parseInt(args[1])
		if err != nil || proto != 2 {
			return nil, false, errf("NOPROTO unsupported protocol version")
		}
	}
	return []any{
		[]byte("server"), []byte("knocidb"),
		[]byte("version"), []byte("0.1.0"),
		[]byte("proto"), int64(2),
		[]byte("id"), int64(1),
		[]byte("mode"), []byte("standalone"),
		[]byte("role"), []byte("master"),
		[]byte("modules"), []any{},
	}, false, nil
}

func (c *client) clientCommand(args [][]byte) (any, bool, error) {
	if len(args) < 2 {
		return nil, false, errf("ERR wrong number of arguments for 'client' command")
	}
	switch strings.ToLower(string(args[1])) {
	case "setname", "setinfo":
		return simpleString("OK"), false, nil
	case "getname":
		return nil, false, nil
	default:
		return simpleString("OK"), false, nil
	}
}

func (c *client) set(args [][]byte) (any, bool, error) {
	if len(args) < 3 {
		return nil, false, errf("ERR wrong number of arguments for 'set' command")
	}
	var expireAt int64
	var nx, xx bool
	for i := 3; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "ex", "px":
			if i+1 >= len(args) {
				return nil, false, errf("ERR syntax error")
			}
			n, err := parseInt(args[i+1])
			if err != nil || n <= 0 {
				return nil, false, errf("ERR invalid expire time in 'set' command")
			}
			if strings.EqualFold(string(args[i]), "ex") {
				n *= 1000
			}
			expireAt = nowMillis() + n
			i++
		case "nx":
			nx = true
		case "xx":
			xx = true
		default:
			return nil, false, errf("ERR syntax error")
		}
	}
	ok, err := c.server.store.Set(args[1], args[2], expireAt, nx, xx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return simpleString("OK"), false, nil
}

func (c *client) get(args [][]byte) (any, bool, error) {
	if len(args) != 2 {
		return nil, false, errf("ERR wrong number of arguments for 'get' command")
	}
	value, err := c.server.store.Get(args[1])
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return nil, false, nil
	}
	return value, false, err
}

func (c *client) incr(cmd string, args [][]byte) (any, bool, error) {
	var delta int64 = 1
	switch cmd {
	case "decr":
		delta = -1
	case "incrby", "decrby":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
		}
		n, err := parseInt(args[2])
		if err != nil {
			return nil, false, errf("ERR value is not an integer or out of range")
		}
		delta = n
		if cmd == "decrby" {
			delta = -n
		}
	default:
		if len(args) != 2 {
			return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
		}
	}
	n, err := c.server.store.IncrBy(args[1], delta)
	return n, false, err
}

func (c *client) expire(cmd string, args [][]byte) (any, bool, error) {
	if len(args) != 3 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	n, err := parseInt(args[2])
	if err != nil {
		return nil, false, errf("ERR value is not an integer or out of range")
	}
	if cmd == "expire" {
		n *= 1000
	}
	ok, err := c.server.store.ExpireAt(args[1], nowMillis()+n)
	if err != nil {
		return nil, false, err
	}
	if ok {
		return int64(1), false, nil
	}
	return int64(0), false, nil
}

func (c *client) ttl(cmd string, args [][]byte) (any, bool, error) {
	if len(args) != 2 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	ms, err := c.server.store.TTLms(args[1])
	if err != nil {
		return nil, false, err
	}
	if cmd == "ttl" && ms >= 0 {
		ms /= 1000
	}
	return ms, false, nil
}

func (c *client) hset(cmd string, args [][]byte) (any, bool, error) {
	if len(args) < 4 || len(args)%2 != 0 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	n, err := c.server.store.HSet(args[1], args[2:]...)
	if err != nil {
		return nil, false, err
	}
	if cmd == "hmset" {
		return simpleString("OK"), false, nil
	}
	return n, false, nil
}

func (c *client) hgetall(cmd string, args [][]byte) (any, bool, error) {
	if len(args) != 2 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	values, err := c.server.store.HGetAll(args[1])
	if err != nil {
		return nil, false, err
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys)*2)
	for _, k := range keys {
		if cmd == "hkeys" {
			out = append(out, []byte(k))
			continue
		}
		if cmd == "hvals" {
			out = append(out, values[k])
			continue
		}
		out = append(out, []byte(k), values[k])
	}
	return out, false, nil
}

func (c *client) push(cmd string, args [][]byte) (any, bool, error) {
	if len(args) < 3 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	var n int64
	var err error
	if cmd == "lpush" {
		n, err = c.server.store.LPush(args[1], args[2:]...)
	} else {
		n, err = c.server.store.RPush(args[1], args[2:]...)
	}
	return n, false, err
}

func (c *client) pop(cmd string, args [][]byte) (any, bool, error) {
	if len(args) != 2 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	var value []byte
	var err error
	if cmd == "lpop" {
		value, err = c.server.store.LPop(args[1])
	} else {
		value, err = c.server.store.RPop(args[1])
	}
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return nil, false, nil
	}
	return value, false, err
}

func (c *client) lrange(args [][]byte) (any, bool, error) {
	if len(args) != 4 {
		return nil, false, errf("ERR wrong number of arguments for 'lrange' command")
	}
	start, err := parseInt(args[2])
	if err != nil {
		return nil, false, errf("ERR value is not an integer or out of range")
	}
	stop, err := parseInt(args[3])
	if err != nil {
		return nil, false, errf("ERR value is not an integer or out of range")
	}
	values, err := c.server.store.LRange(args[1], start, stop)
	if err != nil {
		return nil, false, err
	}
	return toAnyArray(values), false, nil
}

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func toAnyArray(values [][]byte) []any {
	out := make([]any, len(values))
	for i := range values {
		out[i] = values[i]
	}
	return out
}
