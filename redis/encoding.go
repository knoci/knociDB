package redis

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const (
	// namespace 是 Redis 数据类型层在 knociDB 中的键前缀。
	// 改版本时换前缀，避免和旧数据混淆。
	namespace = "r1:"

	typeString byte = 1
	typeHash   byte = 2
	typeList   byte = 3
	typeSet    byte = 4
	typeZSet   byte = 5

	// listSeqOffset 把有符号序号映射到 uint64，保证字节序排序与数值序一致。
	listSeqOffset int64 = 1 << 62
)

type meta struct {
	typ      byte
	expireAt int64 // unix milliseconds; 0 = no expiration
	size     int64
	head     int64
	tail     int64
}

func nowMillis() int64 { return time.Now().UnixMilli() }

func b64(v []byte) string { return base64.RawURLEncoding.EncodeToString(v) }

func decodeB64(v string) ([]byte, error) {
	out, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return nil, fmt.Errorf("decode redis key part: %w", err)
	}
	return out, nil
}

func metaKey(key []byte) []byte {
	return []byte(namespace + "m:" + b64(key))
}

func stringKey(key []byte) []byte {
	return []byte(namespace + "s:" + b64(key))
}

func hashPrefix(key []byte) []byte {
	return []byte(namespace + "h:" + b64(key) + ":")
}

func listPrefix(key []byte) []byte {
	return []byte(namespace + "l:" + b64(key) + ":")
}

func setPrefix(key []byte) []byte {
	return []byte(namespace + "t:" + b64(key) + ":")
}

func hashFieldKey(key, field []byte) []byte {
	return append(hashPrefix(key), b64(field)...)
}

func setMemberKey(key, member []byte) []byte {
	return append(setPrefix(key), b64(member)...)
}

func zsetScorePrefix(key []byte) []byte {
	return []byte(namespace + "z:" + b64(key) + ":s:")
}

func zsetMemberPrefix(key []byte) []byte {
	return []byte(namespace + "z:" + b64(key) + ":m:")
}

func zsetMemberKey(key, member []byte) []byte {
	return append(zsetMemberPrefix(key), b64(member)...)
}

func zsetScoreKey(key []byte, score float64, member []byte) []byte {
	prefix := zsetScorePrefix(key)
	out := make([]byte, len(prefix)+8+len(member))
	copy(out, prefix)
	putSortableScore(out[len(prefix):], score)
	copy(out[len(prefix)+8:], member)
	return out
}

// putSortableScore 把 float64 编码成可按字节序排序的 8 字节。
// 负数取反、正数翻符号位，保证 -inf < ... < +inf。
func putSortableScore(dst []byte, score float64) {
	bits := math.Float64bits(score)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	binary.BigEndian.PutUint64(dst, bits)
}

func decodeSortableScore(src []byte) (float64, error) {
	if len(src) < 8 {
		return 0, fmt.Errorf("invalid score encoding")
	}
	bits := binary.BigEndian.Uint64(src[:8])
	if bits&(1<<63) != 0 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits), nil
}

func encodeScoreValue(score float64) []byte {
	out := make([]byte, 8)
	putSortableScore(out, score)
	return out
}

func listEntryKey(key []byte, seq int64) []byte {
	prefix := listPrefix(key)
	out := make([]byte, len(prefix)+8)
	copy(out, prefix)
	binary.BigEndian.PutUint64(out[len(prefix):], uint64(seq+listSeqOffset))
	return out
}

func decodeListSeq(key, prefix []byte) (int64, error) {
	if len(key) != len(prefix)+8 {
		return 0, fmt.Errorf("invalid list entry key")
	}
	raw := binary.BigEndian.Uint64(key[len(prefix):])
	return int64(raw) - listSeqOffset, nil
}

func encodeMeta(m meta) []byte {
	out := make([]byte, 0, 35)
	out = append(out, 'K', '1', m.typ)
	out = appendInt64(out, m.expireAt)
	out = appendInt64(out, m.size)
	out = appendInt64(out, m.head)
	out = appendInt64(out, m.tail)
	return out
}

func decodeMeta(data []byte) (meta, error) {
	if len(data) != 35 || data[0] != 'K' || data[1] != '1' {
		return meta{}, fmt.Errorf("invalid redis metadata")
	}
	return meta{
		typ:      data[2],
		expireAt: int64(binary.BigEndian.Uint64(data[3:11])),
		size:     int64(binary.BigEndian.Uint64(data[11:19])),
		head:     int64(binary.BigEndian.Uint64(data[19:27])),
		tail:     int64(binary.BigEndian.Uint64(data[27:35])),
	}, nil
}

func appendInt64(dst []byte, v int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return append(dst, buf[:]...)
}

func typeName(typ byte) string {
	switch typ {
	case typeString:
		return "string"
	case typeHash:
		return "hash"
	case typeList:
		return "list"
	case typeSet:
		return "set"
	case typeZSet:
		return "zset"
	default:
		return "none"
	}
}

func typeByte(name string) (byte, bool) {
	switch name {
	case "string":
		return typeString, true
	case "hash":
		return typeHash, true
	case "list":
		return typeList, true
	case "set":
		return typeSet, true
	case "zset":
		return typeZSet, true
	default:
		return 0, false
	}
}
