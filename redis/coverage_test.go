package redis

import (
	"testing"

	"github.com/knoci/knocidb"
	"github.com/stretchr/testify/require"
)

func TestStoreCoverageExtras(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	// String / TTL branches.
	ok, err := store.Set([]byte("k"), []byte("v"), 0, false, true)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = store.Set([]byte("k"), []byte("v"), 0, false, false)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = store.ExpireAt([]byte("missing"), nowMillis()+1000)
	require.NoError(t, err)
	require.False(t, ok)
	ms, err := store.TTLms([]byte("missing"))
	require.NoError(t, err)
	require.Equal(t, int64(-2), ms)
	ok, err = store.Persist([]byte("missing"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, int64(1), mustInt(store.Exists([]byte("k"), []byte("missing"))))
	require.Equal(t, "string", mustString(store.Type([]byte("k"))))
	require.Equal(t, "none", mustString(store.Type([]byte("missing"))))
	n, err := store.Append([]byte("k"), []byte("2"))
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	n, err = store.Del([]byte("missing"))
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	// Hash branches.
	_, err = store.HSet([]byte("h"), []byte("a"))
	require.Error(t, err)
	_, err = store.HGet([]byte("h"), []byte("a"))
	require.ErrorIs(t, err, knocidb.ErrKeyNotFound)
	_, err = store.HSet([]byte("h"), []byte("a"), []byte("1"))
	require.NoError(t, err)
	require.Equal(t, int64(1), mustInt(store.HLen([]byte("h"))))
	ok, err = store.HExists([]byte("h"), []byte("a"))
	require.NoError(t, err)
	require.True(t, ok)
	_, err = store.HDel([]byte("h"), []byte("missing"))
	require.NoError(t, err)
	all, err := store.HGetAll([]byte("missing"))
	require.NoError(t, err)
	require.Empty(t, all)

	// List branches.
	_, err = store.LPop([]byte("missing"))
	require.ErrorIs(t, err, knocidb.ErrKeyNotFound)
	_, err = store.RPop([]byte("missing"))
	require.ErrorIs(t, err, knocidb.ErrKeyNotFound)
	require.Equal(t, int64(0), mustInt(store.LLen([]byte("missing"))))
	vals, err := store.LRange([]byte("missing"), 0, -1)
	require.NoError(t, err)
	require.Empty(t, vals)
	_, err = store.LPush([]byte("h"), []byte("x"))
	require.ErrorIs(t, err, ErrWrongType)

	// Set branches.
	ok, err = store.SIsMember([]byte("missing"), []byte("m"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, int64(0), mustInt(store.SCard([]byte("missing"))))
	members, err := store.SMembers([]byte("missing"))
	require.NoError(t, err)
	require.Empty(t, members)
	_, err = store.SAdd([]byte("h"), []byte("x"))
	require.ErrorIs(t, err, ErrWrongType)
	n, err = store.SRem([]byte("missing"), []byte("x"))
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	// ZSet branches.
	entries, err := store.ZRange([]byte("missing"), 0, -1, false)
	require.NoError(t, err)
	require.Empty(t, entries)
	score, ok, err := store.ZScore([]byte("missing"), []byte("m"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, score)
	rank, ok, err := store.ZRank([]byte("missing"), []byte("m"), false)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, rank)
	require.Equal(t, int64(0), mustInt(store.ZCard([]byte("missing"))))
	_, err = store.ZAdd([]byte("missing"), nil)
	require.NoError(t, err)
	_, err = store.ZRem([]byte("missing"), []byte("m"))
	require.NoError(t, err)
	_, err = store.ZIncrBy([]byte("z"), []byte("m"), 1.5)
	require.NoError(t, err)
	score, ok, err = store.ZScore([]byte("z"), []byte("m"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1.5, score)
	n, err = store.ZCount([]byte("z"), scoreBound{value: 1}, scoreBound{value: 2})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	n, err = store.ZRem([]byte("z"), []byte("m"))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// Scan / keys / expire.
	keys, err := store.Keys("*")
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	cursor, scanKeys, err := store.Scan(0, "*", 10, "string")
	require.NoError(t, err)
	require.Zero(t, cursor)
	require.NotEmpty(t, scanKeys)
	_, _, err = store.Scan(0, "*", 10, "hash")
	require.NoError(t, err)
	expired, err := store.ExpireDue(nowMillis() + 100000)
	require.NoError(t, err)
	require.GreaterOrEqual(t, expired, 0)
}

func mustInt(v int64, err error) int64 {
	if err != nil {
		panic(err)
	}
	return v
}

func mustString(v string, err error) string {
	if err != nil {
		panic(err)
	}
	return v
}
