package redis

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/knoci/knocidb"
)

var (
	// ErrWrongType 对应 Redis 的 WRONGTYPE 错误。
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	// ErrNotInteger 对应 Redis 的整数解析错误。
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
)

// Store 是 Redis 数据结构层。
//
// 编码约定：
//   - r1:m:<key>                 元数据：类型、过期时间、元素数量、list 头尾
//   - r1:s:<key>                 string 值
//   - r1:h:<key>:<field>         hash 字段
//   - r1:l:<key>:<8-byte seq>    list 元素（seq 是逻辑序号）
//   - r1:t:<key>:<member>        set 成员
//
// 所有 key/field/member 都用 base64url 编码，避免分隔符和二进制安全问题。
type Store struct {
	db *knocidb.DB
}

// NewStore 在 knociDB 上创建 Redis 数据类型层。
func NewStore(db *knocidb.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("nil knociDB")
	}
	return &Store{db: db}, nil
}

func (s *Store) newBatch() *knocidb.Batch {
	return s.db.NewBatch(knocidb.BatchOptions{})
}

func (s *Store) putMeta(b *knocidb.Batch, key []byte, m meta) error {
	return b.Put(metaKey(key), encodeMeta(m))
}

func (s *Store) readMeta(key []byte) (meta, bool, error) {
	data, err := s.db.Get(metaKey(key))
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return meta{}, false, nil
	}
	if err != nil {
		return meta{}, false, err
	}
	m, err := decodeMeta(data)
	if err != nil {
		return meta{}, false, err
	}
	return m, true, nil
}

func (s *Store) getMeta(key []byte) (meta, bool, error) {
	m, exists, err := s.readMeta(key)
	if err != nil || !exists {
		return meta{}, exists, err
	}
	if m.expireAt > 0 && m.expireAt <= nowMillis() {
		if err := s.removeWithMeta(key, m); err != nil {
			return meta{}, false, err
		}
		return meta{}, false, nil
	}
	return m, true, nil
}

func (s *Store) scan(prefix []byte, fn func(key, value []byte) error) error {
	it, err := s.db.NewIterator(knocidb.IteratorOptions{Prefix: prefix})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) scanKeys(prefix []byte) ([][]byte, error) {
	var keys [][]byte
	err := s.scan(prefix, func(key, _ []byte) error {
		keys = append(keys, key)
		return nil
	})
	return keys, err
}

func (s *Store) dataKeys(key []byte, m meta) ([][]byte, error) {
	switch m.typ {
	case typeString:
		return [][]byte{stringKey(key)}, nil
	case typeHash:
		return s.scanKeys(hashPrefix(key))
	case typeList:
		return s.scanKeys(listPrefix(key))
	case typeSet:
		return s.scanKeys(setPrefix(key))
	case typeZSet:
		scoreKeys, err := s.scanKeys(zsetScorePrefix(key))
		if err != nil {
			return nil, err
		}
		memberKeys, err := s.scanKeys(zsetMemberPrefix(key))
		if err != nil {
			return nil, err
		}
		return append(scoreKeys, memberKeys...), nil
	default:
		return nil, nil
	}
}

func (s *Store) removeWithMeta(key []byte, m meta) error {
	keys, err := s.dataKeys(key, m)
	if err != nil {
		return err
	}
	keys = append(keys, metaKey(key))
	b := s.newBatch()
	defer b.Close()
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return b.Commit()
}

// Set 设置 string；expireAt 为 0 表示不过期。
func (s *Store) Set(key, value []byte, expireAt int64, nx, xx bool) (bool, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return false, err
	}
	if nx && exists {
		return false, nil
	}
	if xx && !exists {
		return false, nil
	}

	var oldKeys [][]byte
	if exists && m.typ != typeString {
		oldKeys, err = s.dataKeys(key, m)
		if err != nil {
			return false, err
		}
	}

	b := s.newBatch()
	defer b.Close()
	for _, k := range oldKeys {
		if err := b.Delete(k); err != nil {
			return false, err
		}
	}
	if err := b.Put(stringKey(key), value); err != nil {
		return false, err
	}
	if err := s.putMeta(b, key, meta{typ: typeString, expireAt: expireAt, size: 1}); err != nil {
		return false, err
	}
	return true, b.Commit()
}

func (s *Store) Get(key []byte) ([]byte, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, knocidb.ErrKeyNotFound
	}
	if m.typ != typeString {
		return nil, ErrWrongType
	}
	value, err := s.db.Get(stringKey(key))
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return nil, knocidb.ErrKeyNotFound
	}
	return value, err
}

func (s *Store) Del(keys ...[]byte) (int64, error) {
	var allKeys [][]byte
	var count int64
	seenKeys := make(map[string]struct{})
	for _, key := range keys {
		if _, ok := seenKeys[string(key)]; ok {
			continue
		}
		seenKeys[string(key)] = struct{}{}
		m, exists, err := s.getMeta(key)
		if err != nil {
			return 0, err
		}
		if !exists {
			continue
		}
		dataKeys, err := s.dataKeys(key, m)
		if err != nil {
			return 0, err
		}
		allKeys = append(allKeys, dataKeys...)
		allKeys = append(allKeys, metaKey(key))
		count++
	}
	if count == 0 {
		return 0, nil
	}
	b := s.newBatch()
	defer b.Close()
	for _, key := range allKeys {
		if err := b.Delete(key); err != nil {
			return 0, err
		}
	}
	return count, b.Commit()
}

func (s *Store) Exists(keys ...[]byte) (int64, error) {
	var count int64
	for _, key := range keys {
		_, exists, err := s.getMeta(key)
		if err != nil {
			return 0, err
		}
		if exists {
			count++
		}
	}
	return count, nil
}

func (s *Store) Type(key []byte) (string, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return "", err
	}
	if !exists {
		return "none", nil
	}
	return typeName(m.typ), nil
}

// ExpireAt 设置绝对过期时间（毫秒）；expireAt <= now 时立即删除。
func (s *Store) ExpireAt(key []byte, expireAt int64) (bool, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if expireAt <= nowMillis() {
		if err := s.removeWithMeta(key, m); err != nil {
			return false, err
		}
		return true, nil
	}
	b := s.newBatch()
	defer b.Close()
	m.expireAt = expireAt
	if err := s.putMeta(b, key, m); err != nil {
		return false, err
	}
	return true, b.Commit()
}

// TTLms 返回剩余毫秒：-2 不存在，-1 不过期。
func (s *Store) TTLms(key []byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return -2, nil
	}
	if m.expireAt == 0 {
		return -1, nil
	}
	left := m.expireAt - nowMillis()
	if left < 0 {
		return -2, nil
	}
	return left, nil
}

func (s *Store) Persist(key []byte) (bool, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return false, err
	}
	if !exists || m.expireAt == 0 {
		return false, nil
	}
	b := s.newBatch()
	defer b.Close()
	m.expireAt = 0
	if err := s.putMeta(b, key, m); err != nil {
		return false, err
	}
	return true, b.Commit()
}

func (s *Store) IncrBy(key []byte, delta int64) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	var current int64
	if exists {
		if m.typ != typeString {
			return 0, ErrWrongType
		}
		raw, err := s.db.Get(stringKey(key))
		if err != nil && !errors.Is(err, knocidb.ErrKeyNotFound) {
			return 0, err
		}
		if len(raw) > 0 {
			current, err = strconv.ParseInt(string(raw), 10, 64)
			if err != nil {
				return 0, ErrNotInteger
			}
		}
	}
	if (delta > 0 && current > math.MaxInt64-delta) ||
		(delta < 0 && current < math.MinInt64-delta) {
		return 0, ErrNotInteger
	}
	next := current + delta

	b := s.newBatch()
	defer b.Close()
	if err := b.Put(stringKey(key), []byte(strconv.FormatInt(next, 10))); err != nil {
		return 0, err
	}
	if err := s.putMeta(b, key, meta{typ: typeString, size: 1}); err != nil {
		return 0, err
	}
	return next, b.Commit()
}

func (s *Store) Append(key, suffix []byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	var value []byte
	if exists {
		if m.typ != typeString {
			return 0, ErrWrongType
		}
		value, err = s.db.Get(stringKey(key))
		if err != nil && !errors.Is(err, knocidb.ErrKeyNotFound) {
			return 0, err
		}
	}
	value = append(value, suffix...)
	b := s.newBatch()
	defer b.Close()
	if err := b.Put(stringKey(key), value); err != nil {
		return 0, err
	}
	if err := s.putMeta(b, key, meta{typ: typeString, size: 1}); err != nil {
		return 0, err
	}
	return int64(len(value)), b.Commit()
}

// ---- Hash ----

func (s *Store) HSet(key []byte, pairs ...[]byte) (int64, error) {
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return 0, fmt.Errorf("ERR wrong number of arguments for 'hset' command")
	}
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if exists && m.typ != typeHash {
		return 0, ErrWrongType
	}
	if !exists {
		m = meta{typ: typeHash}
	}

	var added int64
	fields := make([][]byte, 0, len(pairs)/2)
	values := make([][]byte, 0, len(pairs)/2)
	seen := make(map[string]int)
	for i := 0; i < len(pairs); i += 2 {
		field, value := pairs[i], pairs[i+1]
		fk := hashFieldKey(key, field)
		if idx, ok := seen[string(fk)]; ok {
			values[idx] = value
			continue
		}
		_, err := s.db.Get(fk)
		if errors.Is(err, knocidb.ErrKeyNotFound) {
			added++
			m.size++
		} else if err != nil {
			return 0, err
		}
		seen[string(fk)] = len(fields)
		fields = append(fields, fk)
		values = append(values, value)
	}
	b := s.newBatch()
	defer b.Close()
	for i, fk := range fields {
		if err := b.Put(fk, values[i]); err != nil {
			return 0, err
		}
	}
	if err := s.putMeta(b, key, m); err != nil {
		return 0, err
	}
	return added, b.Commit()
}

func (s *Store) HGet(key, field []byte) ([]byte, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, knocidb.ErrKeyNotFound
	}
	if m.typ != typeHash {
		return nil, ErrWrongType
	}
	return s.db.Get(hashFieldKey(key, field))
}

func (s *Store) HDel(key []byte, fields ...[]byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if m.typ != typeHash {
		return 0, ErrWrongType
	}
	var keys [][]byte
	var removed int64
	seenFields := make(map[string]struct{})
	for _, field := range fields {
		fk := hashFieldKey(key, field)
		if _, ok := seenFields[string(fk)]; ok {
			continue
		}
		seenFields[string(fk)] = struct{}{}
		if _, err := s.db.Get(fk); err == nil {
			keys = append(keys, fk)
			removed++
		} else if !errors.Is(err, knocidb.ErrKeyNotFound) {
			return 0, err
		}
	}
	if removed == 0 {
		return 0, nil
	}
	b := s.newBatch()
	defer b.Close()
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return 0, err
		}
	}
	m.size -= removed
	if m.size <= 0 {
		if err := b.Delete(metaKey(key)); err != nil {
			return 0, err
		}
	} else if err := s.putMeta(b, key, m); err != nil {
		return 0, err
	}
	return removed, b.Commit()
}

func (s *Store) HGetAll(key []byte) (map[string][]byte, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return map[string][]byte{}, nil
	}
	if m.typ != typeHash {
		return nil, ErrWrongType
	}
	out := make(map[string][]byte)
	prefix := hashPrefix(key)
	err = s.scan(prefix, func(k, v []byte) error {
		field, err := decodeB64(strings.TrimPrefix(string(k), string(prefix)))
		if err != nil {
			return err
		}
		out[string(field)] = append([]byte(nil), v...)
		return nil
	})
	return out, err
}

func (s *Store) HLen(key []byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if m.typ != typeHash {
		return 0, ErrWrongType
	}
	return m.size, nil
}

func (s *Store) HExists(key, field []byte) (bool, error) {
	_, err := s.HGet(key, field)
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// ---- Set ----

func (s *Store) SAdd(key []byte, members ...[]byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if exists && m.typ != typeSet {
		return 0, ErrWrongType
	}
	if !exists {
		m = meta{typ: typeSet}
	}
	var added int64
	keys := make([][]byte, 0, len(members))
	seen := make(map[string]struct{})
	for _, member := range members {
		mk := setMemberKey(key, member)
		if _, ok := seen[string(mk)]; ok {
			continue
		}
		seen[string(mk)] = struct{}{}
		if _, err := s.db.Get(mk); errors.Is(err, knocidb.ErrKeyNotFound) {
			added++
			m.size++
		} else if err != nil {
			return 0, err
		}
		keys = append(keys, mk)
	}
	b := s.newBatch()
	defer b.Close()
	for _, mk := range keys {
		if err := b.Put(mk, []byte{1}); err != nil {
			return 0, err
		}
	}
	if err := s.putMeta(b, key, m); err != nil {
		return 0, err
	}
	return added, b.Commit()
}

func (s *Store) SRem(key []byte, members ...[]byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if m.typ != typeSet {
		return 0, ErrWrongType
	}
	var removed int64
	var keys [][]byte
	seenMembers := make(map[string]struct{})
	for _, member := range members {
		mk := setMemberKey(key, member)
		if _, ok := seenMembers[string(mk)]; ok {
			continue
		}
		seenMembers[string(mk)] = struct{}{}
		if _, err := s.db.Get(mk); err == nil {
			removed++
			keys = append(keys, mk)
		} else if !errors.Is(err, knocidb.ErrKeyNotFound) {
			return 0, err
		}
	}
	if removed == 0 {
		return 0, nil
	}
	b := s.newBatch()
	defer b.Close()
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return 0, err
		}
	}
	m.size -= removed
	if m.size <= 0 {
		if err := b.Delete(metaKey(key)); err != nil {
			return 0, err
		}
	} else if err := s.putMeta(b, key, m); err != nil {
		return 0, err
	}
	return removed, b.Commit()
}

func (s *Store) SMembers(key []byte) ([][]byte, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return [][]byte{}, nil
	}
	if m.typ != typeSet {
		return nil, ErrWrongType
	}
	prefix := setPrefix(key)
	var out [][]byte
	err = s.scan(prefix, func(k, _ []byte) error {
		member, err := decodeB64(strings.TrimPrefix(string(k), string(prefix)))
		if err != nil {
			return err
		}
		out = append(out, member)
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
	return out, err
}

func (s *Store) SIsMember(key, member []byte) (bool, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if m.typ != typeSet {
		return false, ErrWrongType
	}
	_, err = s.db.Get(setMemberKey(key, member))
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) SCard(key []byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if m.typ != typeSet {
		return 0, ErrWrongType
	}
	return m.size, nil
}

// ---- List ----

func (s *Store) LPush(key []byte, values ...[]byte) (int64, error) {
	return s.push(key, values, true)
}

func (s *Store) RPush(key []byte, values ...[]byte) (int64, error) {
	return s.push(key, values, false)
}

func (s *Store) push(key []byte, values [][]byte, left bool) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if exists && m.typ != typeList {
		return 0, ErrWrongType
	}
	if !exists {
		m = meta{typ: typeList, head: 0, tail: -1}
	}
	b := s.newBatch()
	defer b.Close()
	for _, value := range values {
		var seq int64
		if left {
			seq = m.head - 1
			m.head = seq
		} else {
			seq = m.tail + 1
			m.tail = seq
		}
		if err := b.Put(listEntryKey(key, seq), value); err != nil {
			return 0, err
		}
		m.size++
	}
	if err := s.putMeta(b, key, m); err != nil {
		return 0, err
	}
	return m.size, b.Commit()
}

func (s *Store) LPop(key []byte) ([]byte, error) { return s.pop(key, true) }
func (s *Store) RPop(key []byte) ([]byte, error) { return s.pop(key, false) }

func (s *Store) pop(key []byte, left bool) ([]byte, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !exists || m.size == 0 {
		return nil, knocidb.ErrKeyNotFound
	}
	if m.typ != typeList {
		return nil, ErrWrongType
	}
	var seq int64
	if left {
		seq = m.head
	} else {
		seq = m.tail
	}
	value, err := s.db.Get(listEntryKey(key, seq))
	if err != nil {
		return nil, err
	}
	b := s.newBatch()
	defer b.Close()
	if err := b.Delete(listEntryKey(key, seq)); err != nil {
		return nil, err
	}
	if left {
		m.head++
	} else {
		m.tail--
	}
	m.size--
	if m.size <= 0 {
		if err := b.Delete(metaKey(key)); err != nil {
			return nil, err
		}
	} else if err := s.putMeta(b, key, m); err != nil {
		return nil, err
	}
	return value, b.Commit()
}

type listEntry struct {
	seq   int64
	value []byte
}

func (s *Store) listEntries(key []byte) ([]listEntry, error) {
	prefix := listPrefix(key)
	var out []listEntry
	err := s.scan(prefix, func(k, v []byte) error {
		seq, err := decodeListSeq(k, prefix)
		if err != nil {
			return err
		}
		out = append(out, listEntry{seq: seq, value: append([]byte(nil), v...)})
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out, err
}

func (s *Store) LLen(key []byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if m.typ != typeList {
		return 0, ErrWrongType
	}
	return m.size, nil
}

func (s *Store) LRange(key []byte, start, stop int64) ([][]byte, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return [][]byte{}, nil
	}
	if m.typ != typeList {
		return nil, ErrWrongType
	}
	entries, err := s.listEntries(key)
	if err != nil {
		return nil, err
	}
	n := int64(len(entries))
	if n == 0 {
		return [][]byte{}, nil
	}
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 || start >= n || start > stop {
		return [][]byte{}, nil
	}
	if stop >= n {
		stop = n - 1
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, entries[i].value)
	}
	return out, nil
}

type zEntry struct {
	member []byte
	score  float64
}

type scoreBound struct {
	value     float64
	exclusive bool
}

func (b scoreBound) minOK(score float64) bool {
	if b.exclusive {
		return score > b.value
	}
	return score >= b.value
}

func (b scoreBound) maxOK(score float64) bool {
	if b.exclusive {
		return score < b.value
	}
	return score <= b.value
}

// ZAdd 添加/更新有序集合成员。
func (s *Store) ZAdd(key []byte, entries []zEntry) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if exists && m.typ != typeZSet {
		return 0, ErrWrongType
	}
	if !exists {
		m = meta{typ: typeZSet}
	}

	order := make([]string, 0, len(entries))
	scores := make(map[string]float64, len(entries))
	members := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		name := string(entry.member)
		if _, ok := scores[name]; !ok {
			order = append(order, name)
		}
		scores[name] = entry.score
		members[name] = entry.member
	}

	var added int64
	type write struct {
		deleteKey []byte
		scoreKey  []byte
		memberKey []byte
		score     float64
	}
	var writes []write
	for _, name := range order {
		member := members[name]
		score := scores[name]
		mk := zsetMemberKey(key, member)
		old, err := s.db.Get(mk)
		if err == nil {
			oldScore, decErr := decodeSortableScore(old)
			if decErr != nil {
				return 0, decErr
			}
			writes = append(writes, write{deleteKey: zsetScoreKey(key, oldScore, member)})
		} else if errors.Is(err, knocidb.ErrKeyNotFound) {
			m.size++
			added++
		} else {
			return 0, err
		}
		writes = append(writes, write{
			scoreKey:  zsetScoreKey(key, score, member),
			memberKey: mk,
			score:     score,
		})
	}

	b := s.newBatch()
	defer b.Close()
	for _, w := range writes {
		if w.deleteKey != nil {
			if err := b.Delete(w.deleteKey); err != nil {
				return 0, err
			}
		}
		if w.scoreKey != nil {
			if err := b.Put(w.scoreKey, []byte{}); err != nil {
				return 0, err
			}
			if err := b.Put(w.memberKey, encodeScoreValue(w.score)); err != nil {
				return 0, err
			}
		}
	}
	if err := s.putMeta(b, key, m); err != nil {
		return 0, err
	}
	return added, b.Commit()
}

func (s *Store) ZCard(key []byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if m.typ != typeZSet {
		return 0, ErrWrongType
	}
	return m.size, nil
}

func (s *Store) ZScore(key, member []byte) (float64, bool, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return 0, false, nil
	}
	if m.typ != typeZSet {
		return 0, false, ErrWrongType
	}
	raw, err := s.db.Get(zsetMemberKey(key, member))
	if errors.Is(err, knocidb.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	score, err := decodeSortableScore(raw)
	return score, err == nil, err
}

func (s *Store) ZRem(key []byte, members ...[]byte) (int64, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if m.typ != typeZSet {
		return 0, ErrWrongType
	}
	seen := make(map[string]struct{})
	var scoreKeys [][]byte
	var memberKeys [][]byte
	for _, member := range members {
		mk := zsetMemberKey(key, member)
		if _, ok := seen[string(mk)]; ok {
			continue
		}
		seen[string(mk)] = struct{}{}
		raw, err := s.db.Get(mk)
		if errors.Is(err, knocidb.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return 0, err
		}
		score, err := decodeSortableScore(raw)
		if err != nil {
			return 0, err
		}
		scoreKeys = append(scoreKeys, zsetScoreKey(key, score, member))
		memberKeys = append(memberKeys, mk)
	}
	if len(memberKeys) == 0 {
		return 0, nil
	}
	b := s.newBatch()
	defer b.Close()
	for _, k := range scoreKeys {
		if err := b.Delete(k); err != nil {
			return 0, err
		}
	}
	for _, k := range memberKeys {
		if err := b.Delete(k); err != nil {
			return 0, err
		}
	}
	m.size -= int64(len(memberKeys))
	if m.size <= 0 {
		if err := b.Delete(metaKey(key)); err != nil {
			return 0, err
		}
	} else if err := s.putMeta(b, key, m); err != nil {
		return 0, err
	}
	if err := b.Commit(); err != nil {
		return 0, err
	}
	return int64(len(memberKeys)), nil
}

func (s *Store) zEntries(key []byte) ([]zEntry, error) {
	prefix := zsetScorePrefix(key)
	var entries []zEntry
	err := s.scan(prefix, func(k, _ []byte) error {
		if len(k) < len(prefix)+8 {
			return fmt.Errorf("invalid zset score key")
		}
		score, err := decodeSortableScore(k[len(prefix):])
		if err != nil {
			return err
		}
		member := append([]byte(nil), k[len(prefix)+8:]...)
		entries = append(entries, zEntry{member: member, score: score})
		return nil
	})
	return entries, err
}

func (s *Store) ZRange(key []byte, start, stop int64, reverse bool) ([]zEntry, error) {
	m, exists, err := s.getMeta(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []zEntry{}, nil
	}
	if m.typ != typeZSet {
		return nil, ErrWrongType
	}
	entries, err := s.zEntries(key)
	if err != nil {
		return nil, err
	}
	if reverse {
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
	n := int64(len(entries))
	if n == 0 {
		return []zEntry{}, nil
	}
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 || start >= n || start > stop {
		return []zEntry{}, nil
	}
	if stop >= n {
		stop = n - 1
	}
	return entries[start : stop+1], nil
}

func (s *Store) ZRank(key, member []byte, reverse bool) (int64, bool, error) {
	entries, err := s.ZRange(key, 0, -1, reverse)
	if err != nil {
		return 0, false, err
	}
	for i, entry := range entries {
		if bytes.Equal(entry.member, member) {
			return int64(i), true, nil
		}
	}
	return 0, false, nil
}

func (s *Store) ZIncrBy(key, member []byte, delta float64) (float64, error) {
	score, exists, err := s.ZScore(key, member)
	if err != nil {
		return 0, err
	}
	if !exists {
		score = 0
	}
	score += delta
	if _, err := s.ZAdd(key, []zEntry{{member: member, score: score}}); err != nil {
		return 0, err
	}
	return score, nil
}

func (s *Store) ZCount(key []byte, min, max scoreBound) (int64, error) {
	entries, err := s.ZRange(key, 0, -1, false)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, entry := range entries {
		if min.minOK(entry.score) && max.maxOK(entry.score) {
			count++
		}
	}
	return count, nil
}

func (s *Store) metaKeys() ([][]byte, error) {
	prefix := []byte(namespace + "m:")
	var keys [][]byte
	err := s.scan(prefix, func(k, _ []byte) error {
		out, err := decodeB64(string(k[len(prefix):]))
		if err != nil {
			return err
		}
		keys = append(keys, out)
		return nil
	})
	return keys, err
}

// Keys 返回匹配 glob 的 key，已过期 key 会被惰性清理。
func (s *Store) Keys(pattern string) ([][]byte, error) {
	if pattern == "" {
		pattern = "*"
	}
	all, err := s.metaKeys()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(all))
	for _, key := range all {
		_, exists, err := s.getMeta(key)
		if err != nil {
			return nil, err
		}
		if exists && matchGlob([]byte(pattern), key) {
			out = append(out, key)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
	return out, nil
}

// Scan 返回 (nextCursor, keys)。cursor 是序号而不是 Redis 哈希游标，
// 但语义上满足“0 开始、0 结束”的遍历。
func (s *Store) Scan(cursor uint64, pattern string, count int, typeFilter string) (uint64, [][]byte, error) {
	keys, err := s.Keys(pattern)
	if err != nil {
		return 0, nil, err
	}
	if typeFilter != "" {
		filtered := keys[:0]
		for _, key := range keys {
			typ, err := s.Type(key)
			if err != nil {
				return 0, nil, err
			}
			if typ == typeFilter {
				filtered = append(filtered, key)
			}
		}
		keys = filtered
	}
	if count <= 0 {
		count = 10
	}
	if cursor >= uint64(len(keys)) {
		return 0, [][]byte{}, nil
	}
	end := int(cursor) + count
	if end > len(keys) {
		end = len(keys)
	}
	next := uint64(end)
	if end >= len(keys) {
		next = 0
	}
	return next, keys[cursor:end], nil
}

// ExpireDue 扫描元数据并删除所有已过期 key，返回删除数量。
func (s *Store) ExpireDue(now int64) (int, error) {
	keys, err := s.metaKeys()
	if err != nil {
		return 0, err
	}
	type expired struct {
		key []byte
		m   meta
	}
	var due []expired
	for _, key := range keys {
		m, exists, err := s.readMeta(key)
		if err != nil {
			return 0, err
		}
		if exists && m.expireAt > 0 && m.expireAt <= now {
			due = append(due, expired{key: key, m: m})
		}
	}
	for _, item := range due {
		if err := s.removeWithMeta(item.key, item.m); err != nil {
			return 0, err
		}
	}
	return len(due), nil
}

// MSet 在一个 Batch 中设置多个 string key。
func (s *Store) MSet(pairs ...[]byte) error {
	if len(pairs)%2 != 0 {
		return fmt.Errorf("ERR wrong number of arguments for 'mset' command")
	}
	var oldKeys [][]byte
	for i := 0; i < len(pairs); i += 2 {
		m, exists, err := s.getMeta(pairs[i])
		if err != nil {
			return err
		}
		if exists && m.typ != typeString {
			keys, err := s.dataKeys(pairs[i], m)
			if err != nil {
				return err
			}
			oldKeys = append(oldKeys, keys...)
		}
	}
	b := s.newBatch()
	defer b.Close()
	for _, key := range oldKeys {
		if err := b.Delete(key); err != nil {
			return err
		}
	}
	for i := 0; i < len(pairs); i += 2 {
		if err := b.Put(stringKey(pairs[i]), pairs[i+1]); err != nil {
			return err
		}
		if err := s.putMeta(b, pairs[i], meta{typ: typeString, size: 1}); err != nil {
			return err
		}
	}
	return b.Commit()
}
