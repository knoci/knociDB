package knocidb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTTLDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.TTLScanInterval = 10 * time.Millisecond
	db, err := Open(options)
	require.NoError(t, err)
	return db, dir
}

func TestCoreTTLPutGetAndExpire(t *testing.T) {
	db, _ := newTTLDB(t)
	defer db.Close()

	require.NoError(t, db.PutWithTTL([]byte("ttl"), []byte("v"), 40*time.Millisecond))
	value, err := db.Get([]byte("ttl"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), value)
	left, ok, err := db.GetTTL([]byte("ttl"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, left, time.Duration(0))

	time.Sleep(120 * time.Millisecond)
	_, err = db.Get([]byte("ttl"))
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, ok, err = db.GetTTL([]byte("ttl"))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCoreTTLSetPersistAndReopen(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.TTLScanInterval = 10 * time.Millisecond

	db, err := Open(options)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("k"), []byte("v")))
	ok, err := db.SetTTL([]byte("k"), 200*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, db.Close())

	db, err = Open(options)
	require.NoError(t, err)
	left, ok, err := db.GetTTL([]byte("k"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, left, time.Duration(0))
	ok, err = db.Persist([]byte("k"))
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = db.GetTTL([]byte("k"))
	require.NoError(t, err)
	require.False(t, ok)
	value, err := db.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), value)
	require.NoError(t, db.Close())
}

func TestCoreTTLDeleteAndIterator(t *testing.T) {
	db, _ := newTTLDB(t)
	defer db.Close()

	require.NoError(t, db.Put([]byte("normal"), []byte("n")))
	require.NoError(t, db.PutWithTTL([]byte("temp"), []byte("t"), 30*time.Millisecond))
	require.NoError(t, db.PutWithTTL([]byte("deleted"), []byte("d"), time.Second))
	require.NoError(t, db.Delete([]byte("deleted")))

	time.Sleep(100 * time.Millisecond)
	it, err := db.NewIterator(IteratorOptions{})
	require.NoError(t, err)
	defer it.Close()
	var keys []string
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.Equal(t, []string{"normal"}, keys)
}

func TestCoreTTLSetTTLMissing(t *testing.T) {
	db, _ := newTTLDB(t)
	defer db.Close()
	ok, err := db.SetTTL([]byte("missing"), time.Second)
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = db.GetTTL([]byte("missing"))
	require.NoError(t, err)
	require.False(t, ok)
}
