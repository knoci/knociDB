package knocidb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

const (
	clusterRPCVersion byte = 1

	rpcOpPut    byte = 1
	rpcOpDelete byte = 2
	rpcOpGet    byte = 3

	rpcStatusOK        byte = 0
	rpcStatusNotFound  byte = 1
	rpcStatusNotLeader byte = 2
	rpcStatusError     byte = 3

	maxRPCValueSize = 64 << 20
)

// Put 写入 key。若当前节点不是 Leader，会尝试转发给已知 Leader。
func (c *ClusterDB) Put(key, value []byte) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if _, addr, ok := c.forwardTarget(); ok {
			err := c.forwardRequest(addr, rpcOpPut, key, value)
			if !errors.Is(err, ErrNotLeader) {
				return err
			}
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return c.putLocal(key, value)
	}
	return lastErr
}

// Delete 删除 key。若当前节点不是 Leader，会尝试转发给已知 Leader。
func (c *ClusterDB) Delete(key []byte) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if _, addr, ok := c.forwardTarget(); ok {
			err := c.forwardRequest(addr, rpcOpDelete, key, nil)
			if !errors.Is(err, ErrNotLeader) {
				return err
			}
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return c.deleteLocal(key)
	}
	return lastErr
}

// Get 读取 key。若当前节点不是 Leader，会优先转发给 Leader，避免依赖
// 具体 Dragonboat 版本对 follower ReadIndex 的行为。
func (c *ClusterDB) Get(key []byte) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if _, addr, ok := c.forwardTarget(); ok {
			value, err := c.forwardGet(addr, key)
			if !errors.Is(err, ErrNotLeader) {
				return value, err
			}
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return c.getLocal(key)
	}
	return nil, lastErr
}

func (c *ClusterDB) isLeaderNow() bool {
	leaderID, err := c.GetLeaderID()
	return err == nil && leaderID == c.config.NodeID
}

func (c *ClusterDB) forwardTarget() (uint64, string, bool) {
	leaderID, err := c.GetLeaderID()
	if err != nil || leaderID == 0 || leaderID == c.config.NodeID {
		return 0, "", false
	}
	c.mu.RLock()
	addr, ok := c.config.PeerRPC[leaderID]
	c.mu.RUnlock()
	return leaderID, addr, ok && addr != ""
}

func (c *ClusterDB) startRPC() error {
	lis, err := net.Listen("tcp", c.config.RPCAddress)
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

func (c *ClusterDB) handleRPC(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	op, key, value, err := readRPCRequest(conn)
	if err != nil {
		_ = writeRPCResponse(conn, rpcStatusError, []byte(err.Error()))
		return
	}
	if !c.isLeaderNow() {
		_ = writeRPCResponse(conn, rpcStatusNotLeader, []byte("not leader"))
		return
	}
	switch op {
	case rpcOpPut:
		err = c.putLocal(key, value)
		if err != nil {
			_ = writeRPCResponse(conn, rpcStatusForError(err), []byte(err.Error()))
			return
		}
		_ = writeRPCResponse(conn, rpcStatusOK, nil)
	case rpcOpDelete:
		err = c.deleteLocal(key)
		if err != nil {
			_ = writeRPCResponse(conn, rpcStatusForError(err), []byte(err.Error()))
			return
		}
		_ = writeRPCResponse(conn, rpcStatusOK, nil)
	case rpcOpGet:
		got, err := c.getLocal(key)
		if err != nil {
			_ = writeRPCResponse(conn, rpcStatusForError(err), []byte(err.Error()))
			return
		}
		_ = writeRPCResponse(conn, rpcStatusOK, got)
	default:
		_ = writeRPCResponse(conn, rpcStatusError, []byte("unknown rpc op"))
	}
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

func (c *ClusterDB) rpcCall(addr string, op byte, key, value []byte) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := writeRPCRequest(conn, op, key, value); err != nil {
		return nil, err
	}
	status, payload, err := readRPCResponse(conn)
	if err != nil {
		return nil, err
	}
	switch status {
	case rpcStatusOK:
		return payload, nil
	case rpcStatusNotFound:
		return nil, ErrKeyNotFound
	case rpcStatusNotLeader:
		return nil, ErrNotLeader
	default:
		return nil, errors.New(string(payload))
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

func writeRPCRequest(w io.Writer, op byte, key, value []byte) error {
	if len(key) > maxRPCValueSize || len(value) > maxRPCValueSize {
		return fmt.Errorf("cluster rpc payload too large")
	}
	hdr := make([]byte, 10)
	hdr[0] = clusterRPCVersion
	hdr[1] = op
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[6:10], uint32(len(value)))
	if _, err := w.Write(hdr); err != nil {
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

func readRPCRequest(r io.Reader) (byte, []byte, []byte, error) {
	hdr := make([]byte, 10)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, nil, nil, err
	}
	if hdr[0] != clusterRPCVersion {
		return 0, nil, nil, fmt.Errorf("unsupported cluster rpc version")
	}
	keyLen := binary.BigEndian.Uint32(hdr[2:6])
	valueLen := binary.BigEndian.Uint32(hdr[6:10])
	if keyLen > maxRPCValueSize || valueLen > maxRPCValueSize {
		return 0, nil, nil, fmt.Errorf("cluster rpc payload too large")
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return 0, nil, nil, err
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return 0, nil, nil, err
	}
	return hdr[1], key, value, nil
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
