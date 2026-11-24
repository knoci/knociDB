package knocidb

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

const (
	clusterRPCVersion byte = 1

	rpcOpPut      byte = 1
	rpcOpDelete   byte = 2
	rpcOpGet      byte = 3
	rpcOpBatch    byte = 4
	rpcOpMeta     byte = 5
	rpcOpProgress byte = 6
	rpcOpPutTTL   byte = 7

	rpcStatusOK           byte = 0
	rpcStatusNotFound     byte = 1
	rpcStatusNotLeader    byte = 2
	rpcStatusError        byte = 3
	rpcStatusUnauthorized byte = 4

	maxRPCValueSize = 64 << 20
)

// Put 写入 key。若当前节点不是 Leader，会尝试转发给已知 Leader。
func (c *ClusterDB) Put(key, value []byte) error {
	return c.PutContext(context.Background(), key, value)
}

// PutContext 带 context 写入 key。
func (c *ClusterDB) PutContext(ctx context.Context, key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	shard := c.shardFor(key)
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if ctx != nil && ctx.Err() != nil {
			return ErrCanceled
		}
		if _, addr, ok := c.forwardTarget(shard); ok {
			err := c.forwardRequest(addr, rpcOpPut, key, value)
			if !errors.Is(err, ErrNotLeader) {
				return err
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := c.proposeWriteContext(ctx, Command{Op: OpPut, Key: key, Value: value}, shard)
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

// PutWithTTL 写入带确定性 Raft TTL 的 key（由 Leader/提议端转换为绝对时间戳后经 Raft 复制）。
func (c *ClusterDB) PutWithTTL(key, value []byte, ttl time.Duration) error {
	return c.PutWithTTLContext(context.Background(), key, value, ttl)
}

// PutWithTTLContext 带 context 写入带确定性 Raft TTL 的 key。
func (c *ClusterDB) PutWithTTLContext(ctx context.Context, key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	expireAt := time.Now().Add(ttl).UnixMilli()
	shard := c.shardFor(key)
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if ctx != nil && ctx.Err() != nil {
			return ErrCanceled
		}
		if _, addr, ok := c.forwardTarget(shard); ok {
			payload := make([]byte, 8+len(value))
			binary.BigEndian.PutUint64(payload[:8], uint64(expireAt))
			copy(payload[8:], value)
			err := c.forwardRequest(addr, rpcOpPutTTL, key, payload)
			if !errors.Is(err, ErrNotLeader) {
				return err
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := c.putLocalWithExpireAt(ctx, key, value, expireAt, shard)
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

// Delete 删除 key。若当前节点不是 Leader，会尝试转发给已知 Leader。
func (c *ClusterDB) Delete(key []byte) error {
	return c.DeleteContext(context.Background(), key)
}

// DeleteContext 带 context 删除 key。
func (c *ClusterDB) DeleteContext(ctx context.Context, key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	shard := c.shardFor(key)
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if ctx != nil && ctx.Err() != nil {
			return ErrCanceled
		}
		if _, addr, ok := c.forwardTarget(shard); ok {
			err := c.forwardRequest(addr, rpcOpDelete, key, nil)
			if !errors.Is(err, ErrNotLeader) {
				return err
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := c.proposeWriteContext(ctx, Command{Op: OpDelete, Key: key}, shard)
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

// Get 读取 key（默认 ReadLinearizable）。若当前节点不是 Leader，会优先转发给 Leader。
func (c *ClusterDB) Get(key []byte) ([]byte, error) {
	return c.GetContext(context.Background(), key, ReadLinearizable)
}

// GetContext 按指定一致级别（ReadLinearizable / ReadFollowerIndex / ReadStaleLocal）读取 key。
func (c *ClusterDB) GetContext(ctx context.Context, key []byte, consistency ReadConsistency) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	shard := c.shardFor(key)
	if consistency == ReadStaleLocal || consistency == ReadFollowerIndex {
		return c.getLocalContext(ctx, key, shard, consistency)
	}
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if ctx != nil && ctx.Err() != nil {
			return nil, ErrCanceled
		}
		if _, addr, ok := c.forwardTarget(shard); ok {
			value, err := c.forwardGet(addr, key)
			if !errors.Is(err, ErrNotLeader) {
				return value, err
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		value, err := c.getLocalContext(ctx, key, shard, ReadLinearizable)
		if !errors.Is(err, ErrNotLeader) {
			return value, err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return nil, lastErr
}

func (c *ClusterDB) forwardTarget(shard uint32) (uint64, string, bool) {
	leaderID, err := c.leaderForShard(shard)
	if err != nil || leaderID == 0 || leaderID == c.config.NodeID {
		return 0, "", false
	}
	c.mu.RLock()
	addr, ok := c.config.PeerRPC[leaderID]
	c.mu.RUnlock()
	return leaderID, addr, ok && addr != ""
}

func (c *ClusterDB) buildRPCTLSConfig(isServer bool) (*tls.Config, error) {
	if !c.config.MutualTLS {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.config.CertFile, c.config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load cluster rpc tls keypair: %w", err)
	}
	caPEM, err := os.ReadFile(c.config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read cluster rpc ca file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse cluster rpc ca certificate failed")
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if isServer {
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func (c *ClusterDB) startRPC() error {
	var (
		lis net.Listener
		err error
	)
	if c.config.MutualTLS {
		tlsCfg, tlsErr := c.buildRPCTLSConfig(true)
		if tlsErr != nil {
			return tlsErr
		}
		lis, err = tls.Listen("tcp", c.config.RPCAddress, tlsCfg)
	} else {
		lis, err = net.Listen("tcp", c.config.RPCAddress)
	}
	if err != nil {
		return fmt.Errorf("listen cluster rpc: %w", err)
	}
	c.mu.Lock()
	c.rpcListener = lis
	c.mu.Unlock()
	c.rpcWG.Add(1)
	go c.acceptRPC(lis)
	return nil
}

func (c *ClusterDB) acceptRPC(lis net.Listener) {
	defer c.rpcWG.Done()
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		c.rpcWG.Add(1)
		go func() {
			defer c.rpcWG.Done()
			c.handleRPC(conn)
		}()
	}
}

func (c *ClusterDB) rpcAuthorized(token []byte) bool {
	secret := []byte(c.config.RPCSecret)
	if len(secret) == 0 {
		return true
	}
	return len(token) == len(secret) && subtle.ConstantTimeCompare(token, secret) == 1
}

func (c *ClusterDB) rpcTimeout() time.Duration {
	if c.config.RPCTimeout <= 0 {
		return 5 * time.Second
	}
	return c.config.RPCTimeout
}

func (c *ClusterDB) handleRPC(conn net.Conn) {
	defer conn.Close()
	for {
		if !c.handleRPCOnce(conn) {
			return
		}
	}
}

const (
	clusterRPCIdleTimeout = 30 * time.Second
)

func (c *ClusterDB) handleRPCOnce(conn net.Conn) bool {
	// 等待下一条请求头时使用独立的空闲超时；空闲超时或 EOF 静默断开，不向连接写错帧也不计入 rpcErrors。
	_ = conn.SetReadDeadline(time.Now().Add(clusterRPCIdleTimeout))
	hdr := make([]byte, 12)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		if errors.Is(err, io.EOF) {
			return false
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return false
		}
		c.rpcErrors.Add(1)
		return false
	}

	c.rpcRequests.Add(1)
	fail := func(status byte, message string) {
		c.rpcErrors.Add(1)
		_ = writeRPCResponse(conn, status, []byte(message))
	}

	if hdr[0] != clusterRPCVersion {
		fail(rpcStatusError, "unsupported cluster rpc version")
		return false
	}
	op := hdr[1]
	tokenLen := binary.BigEndian.Uint16(hdr[2:4])
	keyLen := binary.BigEndian.Uint32(hdr[4:8])
	valueLen := binary.BigEndian.Uint32(hdr[8:12])
	if keyLen > maxRPCValueSize || valueLen > maxRPCValueSize {
		fail(rpcStatusError, "cluster rpc payload too large")
		return false
	}

	execTimeout := c.rpcTimeout()
	if op == rpcOpBatch && execTimeout < 15*time.Second {
		execTimeout = 15 * time.Second
	}
	_ = conn.SetDeadline(time.Now().Add(execTimeout))

	// 先读取并校验 token，鉴权失败直接断开连接，避免认证前分配高达 128MB 的 key/value 缓冲区。
	token := make([]byte, tokenLen)
	if _, err := io.ReadFull(conn, token); err != nil {
		fail(rpcStatusError, err.Error())
		return false
	}
	if !c.rpcAuthorized(token) {
		fail(rpcStatusUnauthorized, "unauthorized")
		return false
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(conn, key); err != nil {
		fail(rpcStatusError, err.Error())
		return false
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(conn, value); err != nil {
		fail(rpcStatusError, err.Error())
		return false
	}

	if op == rpcOpProgress {
		if len(key) != 4 {
			fail(rpcStatusError, "invalid progress shard id")
			return true
		}
		shardID := binary.BigEndian.Uint32(key)
		idx, err := c.shardAppliedIndex(shardID)
		if err != nil {
			fail(rpcStatusForError(err), err.Error())
			return true
		}
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, idx)
		_ = writeRPCResponse(conn, rpcStatusOK, out)
		return true
	}
	if op == rpcOpMeta {
		var meta shardMapUpdate
		if err := json.Unmarshal(value, &meta); err != nil || meta.ShardCount != c.config.ShardCount ||
			(meta.Migration == nil && meta.Epoch == 0) ||
			(meta.Migration != nil && (meta.Epoch != 0 || !meta.Migration.valid(c.config.ShardCount))) {
			fail(rpcStatusError, ErrInvalidRaftCommand.Error())
			return true
		}
		mid := c.config.MetadataGroupID()
		if mid == 0 {
			fail(rpcStatusError, "metadata group not enabled")
			return true
		}
		c.mu.RLock()
		nodeHost := c.nodeHost
		started := c.state.Load() && nodeHost != nil
		c.mu.RUnlock()
		if !started {
			fail(rpcStatusError, ErrNodeNotStarted.Error())
			return true
		}
		leader, _, valid, err := nodeHost.GetLeaderID(mid)
		if err != nil || !valid || leader != c.config.NodeID {
			fail(rpcStatusNotLeader, "not metadata leader")
			return true
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := nodeHost.SyncPropose(ctx, nodeHost.GetNoOPSession(mid), value)
		if err == nil {
			err = raftResultError(result)
		}
		if err != nil {
			fail(rpcStatusForError(err), err.Error())
			return true
		}
		if meta.Migration != nil {
			var out [8]byte
			binary.BigEndian.PutUint64(out[:], result.Value)
			_ = writeRPCResponse(conn, rpcStatusOK, out[:])
			return true
		}
		_ = writeRPCResponse(conn, rpcStatusOK, nil)
		return true
	}
	shard := c.shardFor(key)
	if op == rpcOpBatch {
		batch, err := decodeRaftWriteCommand(value)
		if err != nil || batch.Op != OpBatch {
			fail(rpcStatusError, ErrInvalidRaftCommand.Error())
			return true
		}
		shard, err = c.shardForBatch(batch.Commands)
		if err != nil {
			fail(rpcStatusError, err.Error())
			return true
		}
	}
	if leader, err := c.leaderForShard(shard); err != nil || leader != c.config.NodeID {
		fail(rpcStatusNotLeader, "not leader")
		return true
	}
	switch op {
	case rpcOpPut:
		if err := c.putLocal(key, value, shard); err != nil {
			fail(rpcStatusForError(err), err.Error())
			return true
		}
		_ = writeRPCResponse(conn, rpcStatusOK, nil)
	case rpcOpPutTTL:
		if len(value) < 8 {
			fail(rpcStatusError, ErrInvalidRaftCommand.Error())
			return true
		}
		expireAt := int64(binary.BigEndian.Uint64(value[:8]))
		if err := c.putLocalWithExpireAt(context.Background(), key, value[8:], expireAt, shard); err != nil {
			fail(rpcStatusForError(err), err.Error())
			return true
		}
		_ = writeRPCResponse(conn, rpcStatusOK, nil)
	case rpcOpDelete:
		if err := c.deleteLocal(key, shard); err != nil {
			fail(rpcStatusForError(err), err.Error())
			return true
		}
		_ = writeRPCResponse(conn, rpcStatusOK, nil)
	case rpcOpGet:
		got, err := c.getLocal(key, shard)
		if err != nil {
			fail(rpcStatusForError(err), err.Error())
			return true
		}
		_ = writeRPCResponse(conn, rpcStatusOK, got)
	case rpcOpBatch:
		if err := c.proposeData(shard, value, 10*time.Second); err != nil {
			fail(rpcStatusForError(err), err.Error())
			return true
		}
		_ = writeRPCResponse(conn, rpcStatusOK, nil)
	default:
		fail(rpcStatusError, "unknown rpc op")
	}
	_ = conn.SetDeadline(time.Time{})
	return true
}

func (c *ClusterDB) shardAppliedIndex(shardID uint32) (uint64, error) {
	c.mu.RLock()
	if c.nodeHost == nil || !c.state.Load() || int(shardID) >= len(c.shards) {
		c.mu.RUnlock()
		return 0, ErrNodeNotStarted
	}
	sm := c.shards[shardID].sm
	c.mu.RUnlock()
	if sm == nil {
		return 0, fmt.Errorf("shard %d state machine is not ready", shardID)
	}
	return sm.AppliedIndex(), nil
}

func (c *ClusterDB) queryAppliedIndex(addr string, shardID uint32) (uint64, error) {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, shardID)
	payload, err := c.rpcCall(addr, rpcOpProgress, key, nil)
	if err != nil {
		return 0, err
	}
	if len(payload) != 8 {
		return 0, fmt.Errorf("invalid progress response")
	}
	return binary.BigEndian.Uint64(payload), nil
}

func (c *ClusterDB) waitForLearnerApplied(shardID uint32, nodeID uint64, timeout time.Duration) error {
	leaderIndex, err := c.shardAppliedIndex(shardID)
	if err != nil {
		return err
	}
	c.mu.RLock()
	addr := c.config.PeerRPC[nodeID]
	c.mu.RUnlock()
	if addr == "" {
		return fmt.Errorf("replica %d has no rpc address", nodeID)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx, err := c.queryAppliedIndex(addr, shardID)
		if err == nil && idx >= leaderIndex {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("learner %d did not catch up shard %d", nodeID, shardID)
}
func (c *ClusterDB) forwardRequest(addr string, op byte, key, value []byte) error {
	_, err := c.rpcCall(addr, op, key, value)
	return err
}

func (c *ClusterDB) forwardGet(addr string, key []byte) ([]byte, error) {
	value, err := c.rpcCall(addr, rpcOpGet, key, nil)
	if errors.Is(err, ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	return value, err
}

func (c *ClusterDB) acquireRPCConn(addr string, dialTimeout, timeout time.Duration) (net.Conn, bool, error) {
	c.rpcPoolMu.Lock()
	list := c.rpcPool[addr]
	if len(list) > 0 {
		conn := list[len(list)-1]
		c.rpcPool[addr] = list[:len(list)-1]
		c.rpcPoolMu.Unlock()
		_ = conn.SetDeadline(time.Now().Add(timeout))
		return conn, true, nil
	}
	c.rpcPoolMu.Unlock()
	var (
		conn net.Conn
		err  error
	)
	if c.config.MutualTLS {
		tlsCfg, tlsErr := c.buildRPCTLSConfig(false)
		if tlsErr != nil {
			return nil, false, tlsErr
		}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: dialTimeout}, "tcp", addr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, dialTimeout)
	}
	if err != nil {
		return nil, false, err
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	return conn, false, nil
}

func (c *ClusterDB) releaseRPCConn(addr string, conn net.Conn) {
	if conn == nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	c.rpcPoolMu.Lock()
	defer c.rpcPoolMu.Unlock()
	if c.rpcPool == nil {
		c.rpcPool = make(map[string][]net.Conn)
	}
	if len(c.rpcPool[addr]) >= 8 {
		_ = conn.Close()
		return
	}
	c.rpcPool[addr] = append(c.rpcPool[addr], conn)
}

func (c *ClusterDB) closeRPCPool() {
	c.rpcPoolMu.Lock()
	defer c.rpcPoolMu.Unlock()
	for _, list := range c.rpcPool {
		for _, conn := range list {
			_ = conn.Close()
		}
	}
	c.rpcPool = nil
}

func (c *ClusterDB) rpcCall(addr string, op byte, key, value []byte) ([]byte, error) {
	c.forwardedOps.Add(1)
	timeout := c.rpcTimeout()
	if op == rpcOpBatch && timeout < 15*time.Second {
		timeout = 15 * time.Second
	}
	dialTimeout := 2 * time.Second
	if timeout < dialTimeout {
		dialTimeout = timeout
	}

	var (
		status  byte
		payload []byte
		err     error
	)
	for attempt := 0; attempt < 2; attempt++ {
		var conn net.Conn
		var reused bool
		if attempt == 0 {
			conn, reused, err = c.acquireRPCConn(addr, dialTimeout, timeout)
		} else {
			conn, err = net.DialTimeout("tcp", addr, dialTimeout)
			if err == nil {
				_ = conn.SetDeadline(time.Now().Add(timeout))
			}
		}
		if err != nil {
			c.rpcErrors.Add(1)
			return nil, err
		}
		if err = writeRPCRequest(conn, []byte(c.config.RPCSecret), op, key, value); err != nil {
			_ = conn.Close()
			if reused && attempt == 0 {
				continue
			}
			c.rpcErrors.Add(1)
			return nil, err
		}
		status, payload, err = readRPCResponse(conn)
		if err != nil {
			_ = conn.Close()
			if reused && attempt == 0 {
				continue
			}
			c.rpcErrors.Add(1)
			return nil, err
		}
		c.releaseRPCConn(addr, conn)
		break
	}
	switch status {
	case rpcStatusOK:
		return payload, nil
	case rpcStatusNotFound:
		return nil, ErrKeyNotFound
	case rpcStatusNotLeader:
		return nil, ErrNotLeader
	case rpcStatusUnauthorized:
		c.rpcErrors.Add(1)
		return nil, ErrClusterUnauthorized
	default:
		c.rpcErrors.Add(1)
		return nil, decodeRaftError(string(payload))
	}
}

func rpcStatusForError(err error) byte {
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return rpcStatusNotFound
	case errors.Is(err, ErrNotLeader), strings.Contains(strings.ToLower(err.Error()), "not leader"):
		return rpcStatusNotLeader
	default:
		return rpcStatusError
	}
}

func writeRPCRequest(w io.Writer, token []byte, op byte, key, value []byte) error {
	if len(token) > 65535 || len(key) > maxRPCValueSize || len(value) > maxRPCValueSize {
		return fmt.Errorf("cluster rpc payload too large")
	}
	hdr := make([]byte, 12)
	hdr[0] = clusterRPCVersion
	hdr[1] = op
	binary.BigEndian.PutUint16(hdr[2:4], uint16(len(token)))
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(value)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if _, err := w.Write(token); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if _, err := w.Write(value); err != nil {
		return err
	}
	return nil
}

func readRPCRequest(r io.Reader) ([]byte, byte, []byte, []byte, error) {
	hdr := make([]byte, 12)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, 0, nil, nil, err
	}
	if hdr[0] != clusterRPCVersion {
		return nil, 0, nil, nil, fmt.Errorf("unsupported cluster rpc version")
	}
	tokenLen := binary.BigEndian.Uint16(hdr[2:4])
	keyLen := binary.BigEndian.Uint32(hdr[4:8])
	valueLen := binary.BigEndian.Uint32(hdr[8:12])
	if keyLen > maxRPCValueSize || valueLen > maxRPCValueSize {
		return nil, 0, nil, nil, fmt.Errorf("cluster rpc payload too large")
	}
	token := make([]byte, tokenLen)
	if _, err := io.ReadFull(r, token); err != nil {
		return nil, 0, nil, nil, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, 0, nil, nil, err
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, 0, nil, nil, err
	}
	return token, hdr[1], key, value, nil
}

func writeRPCResponse(w io.Writer, status byte, payload []byte) error {
	hdr := make([]byte, 6)
	hdr[0] = clusterRPCVersion
	hdr[1] = status
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(payload)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readRPCResponse(r io.Reader) (byte, []byte, error) {
	hdr := make([]byte, 6)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, nil, err
	}
	if hdr[0] != clusterRPCVersion {
		return 0, nil, fmt.Errorf("unsupported cluster rpc version")
	}
	size := binary.BigEndian.Uint32(hdr[2:6])
	if size > maxRPCValueSize {
		return 0, nil, fmt.Errorf("cluster rpc payload too large")
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return hdr[1], payload, nil
}
