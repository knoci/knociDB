package redis

import "testing"

func TestServerCommandArityErrors(t *testing.T) {
	_, _, conn := newTestServer(t)
	commands := [][]string{
		{"GET"}, {"SET", "k"}, {"MSET", "k"}, {"MGET"}, {"DEL"}, {"EXISTS"},
		{"EXPIRE", "k"}, {"TTL"}, {"PERSIST"}, {"HSET", "h"}, {"HGET", "h"},
		{"HDEL", "h"}, {"HLEN"}, {"HEXISTS", "h"}, {"LPUSH", "l"}, {"RPUSH", "l"},
		{"LPOP", "l", "extra"}, {"LLEN"}, {"LRANGE", "l", "0"}, {"SADD", "s"},
		{"SREM", "s"}, {"SMEMBERS"}, {"SISMEMBER", "s"}, {"SCARD"},
		{"ZADD", "z"}, {"ZSCORE", "z"}, {"ZREM", "z"}, {"ZCARD"},
		{"ZRANGE", "z", "0"}, {"ZRANK", "z"}, {"ZINCRBY", "z", "1"},
		{"ZCOUNT", "z", "1"}, {"ZRANGEBYSCORE", "z", "1"},
		{"KEYS"}, {"SCAN"}, {"EXPIRE"}, {"CLIENT"}, {"SELECT", "x"},
		{"TYPE"}, {"APPEND", "k"}, {"STRLEN"}, {"INCRBY", "k"},
	}
	for _, cmd := range commands {
		_ = do(t, conn, cmd...)
	}
}
