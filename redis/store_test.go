package redis

import (
	"testing"

	"github.com/knoci/knocidb"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) (*Store, *knocidb.DB) {
	t.Helper()
	options := knocidb.DefaultOptions
	options.DirPath = t.TempDir()
	db, err := knocidb.Open(options)
	require.NoError(t, err)
	store, err := NewStore(db)
	require.NoError(t, err)
	return store, db
}

func TestStoreStringAndTTL(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	ok, err := store.Set([]byte("k"), []byte("v"), 0, false, false)
	require.NoError(t, err)
	require.True(t, ok)
	value, err := store.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), value)

	ok, err = store.Set([]byte("k"), []byte("v2"), 0, true, false)
	require.NoError(t, err)
	require.False(t, ok)
	value, _ = store.Get([]byte("k"))
	require.Equal(t, []byte("v"), value)

	ok, err = store.ExpireAt([]byte("k"), nowMillis()+5000)
	require.NoError(t, err)
	require.True(t, ok)
	ms, err := store.TTLms([]byte("k"))
	require.NoError(t, err)
	require.Greater(t, ms, int64(0))
	ok, err = store.Persist([]byte("k"))
	require.NoError(t, err)
	require.True(t, ok)
	ms, _ = store.TTLms([]byte("k"))
	require.Equal(t, int64(-1), ms)

	n, err := store.IncrBy([]byte("counter"), 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
	n, err = store.IncrBy([]byte("counter"), -1)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
}

func TestStoreHash(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	n, err := store.HSet([]byte("h"), []byte("a"), []byte("1"), []byte("b"), []byte("2"))
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	n, err = store.HSet([]byte("h"), []byte("a"), []byte("3"))
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	n, err = store.HSet([]byte("h"), []byte("c"), []byte("4"), []byte("c"), []byte("5"))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	value, err := store.HGet([]byte("h"), []byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("3"), value)
	all, err := store.HGetAll([]byte("h"))
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"a": []byte("3"), "b": []byte("2"), "c": []byte("5")}, all)
	n, err = store.HDel([]byte("h"), []byte("a"), []byte("a"))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	_, err = store.HGet([]byte("h"), []byte("a"))
	require.ErrorIs(t, err, knocidb.ErrKeyNotFound)
}

func TestStoreList(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	n, err := store.RPush([]byte("l"), []byte("1"), []byte("2"), []byte("3"))
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
	n, err = store.LPush([]byte("l"), []byte("0"))
	require.NoError(t, err)
	require.Equal(t, int64(4), n)
	values, err := store.LRange([]byte("l"), 0, -1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("0"), []byte("1"), []byte("2"), []byte("3")}, values)
	value, err := store.LPop([]byte("l"))
	require.NoError(t, err)
	require.Equal(t, []byte("0"), value)
	value, err = store.RPop([]byte("l"))
	require.NoError(t, err)
	require.Equal(t, []byte("3"), value)
	values, err = store.LRange([]byte("l"), 0, -1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("1"), []byte("2")}, values)
}

func TestStoreSet(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	n, err := store.SAdd([]byte("s"), []byte("a"), []byte("b"), []byte("a"))
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	members, err := store.SMembers([]byte("s"))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, members)
	ok, err := store.SIsMember([]byte("s"), []byte("a"))
	require.NoError(t, err)
	require.True(t, ok)
	n, err = store.SRem([]byte("s"), []byte("a"), []byte("a"))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	members, _ = store.SMembers([]byte("s"))
	require.Equal(t, [][]byte{[]byte("b")}, members)
}

func TestStorePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	options := knocidb.DefaultOptions
	options.DirPath = dir

	db, err := knocidb.Open(options)
	require.NoError(t, err)
	store, err := NewStore(db)
	require.NoError(t, err)
	_, err = store.HSet([]byte("h"), []byte("f"), []byte("v"))
	require.NoError(t, err)
	_, err = store.RPush([]byte("l"), []byte("a"), []byte("b"))
	require.NoError(t, err)
	_, err = store.SAdd([]byte("s"), []byte("m"))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = knocidb.Open(options)
	require.NoError(t, err)
	defer db.Close()
	store, err = NewStore(db)
	require.NoError(t, err)
	value, err := store.HGet([]byte("h"), []byte("f"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), value)
	values, err := store.LRange([]byte("l"), 0, -1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, values)
	ok, err := store.SIsMember([]byte("s"), []byte("m"))
	require.NoError(t, err)
	require.True(t, ok)
}

func TestStoreWrongTypeAndDelete(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	_, err := store.Set([]byte("x"), []byte("v"), 0, false, false)
	require.NoError(t, err)
	_, err = store.HGet([]byte("x"), []byte("f"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.LPush([]byte("x"), []byte("v"))
	require.ErrorIs(t, err, ErrWrongType)

	require.NoError(t, store.MSet([]byte("m1"), []byte("v1"), []byte("m2"), []byte("v2")))
	value, err := store.Get([]byte("m2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), value)

	n, err := store.Del([]byte("x"), []byte("x"))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	_, err = store.Get([]byte("x"))
	require.ErrorIs(t, err, knocidb.ErrKeyNotFound)
}
