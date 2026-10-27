package redis

import (
	"fmt"
	"net"
)

// ClusterNode describes one static Redis Cluster node.
//
// This implementation uses static slot ownership and MOVED redirects.
// It does not implement gossip, automatic failover or replication.
type ClusterNode struct {
	ID        string
	Host      string
	Port      int
	StartSlot int
	EndSlot   int
}

// ClusterOptions enables Redis Cluster protocol support.
//
// If Nodes is empty, this node owns all 16384 slots (single-node cluster).
// If Nodes is set, every node must know the full static topology; commands for
// slots owned by another node return MOVED. There is no gossip or failover.
type ClusterOptions struct {
	Enabled       bool
	NodeID        string
	AdvertiseHost string
	AdvertisePort int
	Nodes         []ClusterNode
}

// WithCluster enables the cluster protocol on the server.
func WithCluster(options ClusterOptions) ServerOption {
	return func(s *Server) { s.cluster = &options }
}

func (s *Server) clusterNodeID() string {
	if s.cluster != nil && s.cluster.NodeID != "" {
		return s.cluster.NodeID
	}
	return "knocidb-node-1"
}

func (s *Server) clusterAddr() (string, int) {
	host := ""
	port := 0
	if s.cluster != nil {
		host = s.cluster.AdvertiseHost
		port = s.cluster.AdvertisePort
	}
	if addr, ok := s.listener.Addr().(*net.TCPAddr); ok {
		if host == "" {
			host = addr.IP.String()
			if host == "" || host == "::" || host == "0.0.0.0" {
				host = "127.0.0.1"
			}
		}
		if port == 0 {
			port = addr.Port
		}
	}
	return host, port
}

func (s *Server) clusterTopology() []ClusterNode {
	if s.cluster == nil {
		return nil
	}
	if len(s.cluster.Nodes) > 0 {
		nodes := make([]ClusterNode, len(s.cluster.Nodes))
		copy(nodes, s.cluster.Nodes)
		return nodes
	}
	host, port := s.clusterAddr()
	return []ClusterNode{{
		ID:        s.clusterNodeID(),
		Host:      host,
		Port:      port,
		StartSlot: 0,
		EndSlot:   16383,
	}}
}

func (s *Server) clusterOwner(slot uint16) (ClusterNode, bool) {
	for _, node := range s.clusterTopology() {
		if int(slot) >= node.StartSlot && int(slot) <= node.EndSlot {
			return node, true
		}
	}
	return ClusterNode{}, false
}

func (s *Server) clusterLocalNodeID() string { return s.clusterNodeID() }

func (c *client) clusterCommand(args [][]byte) (any, bool, error) {
	if c.server.cluster == nil || !c.server.cluster.Enabled {
		return nil, false, errf("ERR This instance has cluster support disabled")
	}
	if len(args) < 2 {
		return nil, false, errf("ERR wrong number of arguments for 'cluster' command")
	}
	nodeID := c.server.clusterNodeID()
	nodes := c.server.clusterTopology()
	switch lower(args[1]) {
	case "info":
		known := len(nodes)
		if known == 0 {
			known = 1
		}
		return []byte(fmt.Sprintf("cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:%d\r\ncluster_size:%d\r\n", known, known)), false, nil
	case "myid":
		return []byte(nodeID), false, nil
	case "slots":
		out := make([]any, 0, len(nodes))
		for _, node := range nodes {
			host, port := node.Host, node.Port
			if host == "" || port == 0 {
				host, port = c.server.clusterAddr()
			}
			master := []any{[]byte(host), int64(port), []byte(node.ID)}
			out = append(out, []any{int64(node.StartSlot), int64(node.EndSlot), master})
		}
		return out, false, nil
	case "nodes":
		var out string
		for _, node := range nodes {
			host, port := node.Host, node.Port
			if host == "" || port == 0 {
				host, port = c.server.clusterAddr()
			}
			flags := "master"
			if node.ID == nodeID {
				flags = "myself,master"
			}
			out += fmt.Sprintf("%s %s:%d@%d %s - 0 0 1 connected %d-%d\n",
				node.ID, host, port, port+10000, flags, node.StartSlot, node.EndSlot)
		}
		return []byte(out), false, nil
	case "keyslot":
		if len(args) != 3 {
			return nil, false, errf("ERR wrong number of arguments for 'cluster keyslot' command")
		}
		return int64(clusterSlot(args[2])), false, nil
	case "shards":
		out := make([]any, 0, len(nodes))
		for _, node := range nodes {
			host, port := node.Host, node.Port
			if host == "" || port == 0 {
				host, port = c.server.clusterAddr()
			}
			nodeMap := []any{
				[]byte("id"), []byte(node.ID),
				[]byte("endpoint"), []byte(host),
				[]byte("ip"), []byte(host),
				[]byte("port"), int64(port),
				[]byte("tls-port"), int64(0),
				[]byte("role"), []byte("master"),
				[]byte("replication-offset"), int64(0),
				[]byte("health"), []byte("online"),
			}
			shard := []any{
				[]byte("slots"), []any{int64(node.StartSlot), int64(node.EndSlot)},
				[]byte("nodes"), []any{nodeMap},
			}
			out = append(out, shard)
		}
		return out, false, nil
	case "replicas", "slaves":
		return []any{}, false, nil
	case "getkeysinslot":
		return []any{}, false, nil
	case "countkeysinslot":
		return int64(0), false, nil
	default:
		return nil, false, errf("ERR unknown CLUSTER subcommand '%s'", string(args[1]))
	}
}

func (c *client) routeCluster(cmd string, args [][]byte) error {
	if c.server.cluster == nil || !c.server.cluster.Enabled {
		return nil
	}
	keys := commandKeys(cmd, args)
	if len(keys) == 0 {
		return nil
	}
	slot := clusterSlot(keys[0])
	for _, key := range keys[1:] {
		if clusterSlot(key) != slot {
			return errf("CROSSSLOT Keys in request don't hash to the same slot")
		}
	}
	owner, ok := c.server.clusterOwner(slot)
	if !ok {
		return errf("CLUSTERDOWN Hash slot not served")
	}
	if owner.ID == c.server.clusterLocalNodeID() {
		return nil
	}
	host, port := owner.Host, owner.Port
	if host == "" || port == 0 {
		host, port = c.server.clusterAddr()
	}
	return errf("MOVED %d %s:%d", slot, host, port)
}

func commandKeys(cmd string, args [][]byte) [][]byte {
	if len(args) < 2 {
		return nil
	}
	switch cmd {
	case "del", "unlink", "exists", "mget":
		return args[1:]
	case "mset":
		var keys [][]byte
		for i := 1; i < len(args); i += 2 {
			keys = append(keys, args[i])
		}
		return keys
	case "rename", "renamenx":
		if len(args) >= 3 {
			return args[1:3]
		}
		return nil
	case "set", "get", "setnx", "incr", "decr", "incrby", "decrby", "append", "strlen",
		"expire", "pexpire", "ttl", "pttl", "persist", "type",
		"hset", "hmset", "hget", "hdel", "hexists", "hlen", "hgetall", "hkeys", "hvals",
		"lpush", "rpush", "lpop", "rpop", "llen", "lrange",
		"sadd", "srem", "smembers", "sismember", "scard",
		"zadd", "zscore", "zrem", "zcard", "zrange", "zrevrange", "zrank", "zrevrank",
		"zincrby", "zcount", "zrangebyscore":
		return args[1:2]
	default:
		return nil
	}
}

func lower(b []byte) string { return string(bytesToLower(b)) }

func bytesToLower(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return out
}

// clusterSlot 计算 Redis Cluster 的 CRC16(key) % 16384，支持 {hashtag}。
func clusterSlot(key []byte) uint16 {
	if start := indexByte(key, '{'); start >= 0 {
		if end := indexByte(key[start+1:], '}'); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	return crc16(key) % 16384
}

func indexByte(b []byte, target byte) int {
	for i, c := range b {
		if c == target {
			return i
		}
	}
	return -1
}

func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
