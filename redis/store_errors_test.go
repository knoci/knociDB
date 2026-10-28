package redis

import (
	"errors"
	"testing"

	"github.com/knoci/knocidb"
	"github.com/stretchr/testify/require"
)

func TestStoreTypeErrorsAndEdges(t *testing.T) {
	store, db := newTestStore(t)
	defer db.Close()

	require.NoError(t, store.MSet([]byte("s"), []byte("1")))
	_, err := store.IncrBy([]byte("s"), 1) // "1" is a numeric string, so this succeeds.
	require.NoError(t, err)
	_, err = store.Set([]byte("s"), []byte("not-number"), 0, false, false)
	require.NoError(t, err)
	if _, err := store.IncrBy([]byte("s"), 1); err == nil {
		t.Fatal("non-numeric incr should fail")
	}
	_, err = store.HSet([]byte("hash"), []byte("f"), []byte("v"))
	require.NoError(t, err)
	_, err = store.Append([]byte("hash"), []byte("x"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.HSet([]byte("s"), []byte("f"), []byte("v"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.HDel([]byte("s"), []byte("f"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.HGetAll([]byte("s"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.SAdd([]byte("s"), []byte("m"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.SRem([]byte("s"), []byte("m"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.SMembers([]byte("s"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.SCard([]byte("s"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.LPush([]byte("s"), []byte("m"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.RPush([]byte("s"), []byte("m"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.LLen([]byte("s"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.LRange([]byte("s"), 0, -1)
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.ZAdd([]byte("s"), []zEntry{{member: []byte("m"), score: 1}})
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.ZRange([]byte("s"), 0, -1, false)
	require.ErrorIs(t, err, ErrWrongType)
	_, _, err = store.ZScore([]byte("s"), []byte("m"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.ZRem([]byte("s"), []byte("m"))
	require.ErrorIs(t, err, ErrWrongType)
	_, err = store.ZIncrBy([]byte("s"), []byte("m"), 1)
	require.ErrorIs(t, err, ErrWrongType)

	err = store.MSet([]byte("odd"))
	require.Error(t, err)
	_, _ = store.Set(nil, []byte("v"), 0, false, false)
	_, _ = store.Get(nil)
	_, _ = store.Del(nil)
	_, _ = store.ExpireAt(nil, nowMillis()+1)
	_, err = store.ExpireAt([]byte("missing"), nowMillis()+1)
	require.NoError(t, err)
	_, _ = store.HSet(nil, nil, nil)
	_, _ = store.ZAdd(nil, nil)
	_, err = store.Keys("[")
	require.NoError(t, err)
	_, _, err = store.Scan(0, "[", 10, "")
	require.NoError(t, err)
	_, _, err = store.Scan(0, "*", 0, "")
	require.NoError(t, err)

	if err := store.MSet([]byte("exp"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ExpireAt([]byte("exp"), nowMillis()-1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get([]byte("exp")); !errors.Is(err, knocidb.ErrKeyNotFound) {
		t.Fatalf("expired key: %v", err)
	}
}
