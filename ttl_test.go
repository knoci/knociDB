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
func newTTLDBNoSweeper(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.TTLScanInterval = 0
	db, err := Open(options)
	require.NoError(t, err)
	return db, dir
}

func TestCoreTTLAPIErrorPaths(t *testing.T) {
	db, _ := newTTLDB(t)
	defer db.Close()

	require.Error(t, db.PutWithTTL(nil, []byte("v"), time.Second))
	require.Error(t, db.PutWithTTL([]byte("k"), []byte("v"), 0))
	require.Error(t, db.PutWithTTL([]byte("k"), []byte("v"), -time.Second))

	_, err := db.SetTTL(nil, time.Second)
	require.Error(t, err)
	_, err = db.SetTTL([]byte("k"), 0)
	require.Error(t, err)

	_, _, err = db.GetTTL(nil)
	require.Error(t, err)

	_, err = db.Persist(nil)
	require.Error(t, err)

	require.NoError(t, db.Put([]byte("plain"), []byte("v")))
	_, ok, err := db.GetTTL([]byte("plain"))
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = db.Persist([]byte("plain"))
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = db.Persist([]byte("missing"))
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, db.Sync())
}

func TestBatchTTLPaths(t *testing.T) {
	db, _ := newTTLDBNoSweeper(t)
	defer db.Close()

	batch := db.NewBatch(DefaultBatchOptions)
	require.Error(t, batch.PutWithTTL(nil, []byte("v"), time.Second))
	require.Error(t, batch.PutWithTTL([]byte("k"), []byte("v"), 0))
	require.Error(t, batch.SetTTL(nil, time.Second))
	require.Error(t, batch.SetTTL([]byte("k"), 0))
	require.Error(t, batch.Persist(nil))
	batch.Close()

	ro := db.NewBatch(BatchOptions{ReadOnly: true})
	require.ErrorIs(t, ro.SetTTL([]byte("k"), time.Second), ErrReadOnlyBatch)
	require.ErrorIs(t, ro.Persist([]byte("k")), ErrReadOnlyBatch)
	ro.Close()

	require.NoError(t, db.Put([]byte("k"), []byte("v")))

	// pending TTL expiry without a pending write
	batch = db.NewBatch(DefaultBatchOptions)
	require.NoError(t, batch.SetTTL([]byte("k"), 5*time.Millisecond))
	time.Sleep(20 * time.Millisecond)
	_, err := batch.Get([]byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
	batch.Close()

	// TTL-only commit
	batch = db.NewBatch(DefaultBatchOptions)
	require.NoError(t, batch.SetTTL([]byte("k"), time.Second))
	require.NoError(t, batch.Commit())
	_, ok, err := db.GetTTL([]byte("k"))
	require.NoError(t, err)
	require.True(t, ok)

	// delete a key with TTL
	batch = db.NewBatch(DefaultBatchOptions)
	require.NoError(t, batch.Delete([]byte("k")))
	require.NoError(t, batch.Commit())
	_, ok, err = db.GetTTL([]byte("k"))
	require.NoError(t, err)
	require.False(t, ok)

	// Persist-only commit
	require.NoError(t, db.PutWithTTL([]byte("k2"), []byte("v"), time.Second))
	batch = db.NewBatch(DefaultBatchOptions)
	require.NoError(t, batch.Persist([]byte("k2")))
	require.NoError(t, batch.Commit())
	_, ok, err = db.GetTTL([]byte("k2"))
	require.NoError(t, err)
	require.False(t, ok)

	// delete a key without TTL
	batch = db.NewBatch(DefaultBatchOptions)
	require.NoError(t, batch.Delete([]byte("k2")))
	require.NoError(t, batch.Commit())
	exists, err := db.Exist([]byte("k2"))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestTTLStoreDirect(t *testing.T) {
	ttl, err := openTTLStore(t.TempDir())
	require.NoError(t, err)
	defer ttl.close()

	now := time.Now().UnixMilli()
	require.NoError(t, ttl.apply(map[string]int64{
		"a": now + 1000,
		"b": now - 1000,
	}))
	expireAt, ok, err := ttl.get([]byte("a"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, expireAt, now)
	require.True(t, ttl.has([]byte("a")))
	require.False(t, ttl.has([]byte("missing")))

	expired, err := ttl.expired(now)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("b")}, expired)

	expiredFlag, err := ttl.isExpired([]byte("b"), now)
	require.NoError(t, err)
	require.True(t, expiredFlag)

	require.NoError(t, ttl.apply(map[string]int64{"a": 0}))
	_, ok, err = ttl.get([]byte("a"))
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, ttl.sync())
}

func TestCoreTTLIteratorFilterWithoutSweeper(t *testing.T) {
	db, _ := newTTLDBNoSweeper(t)
	defer db.Close()

	require.NoError(t, db.PutWithTTL([]byte("a-expired"), []byte("v"), 20*time.Millisecond))
	require.NoError(t, db.Put([]byte("z-normal"), []byte("v")))
	time.Sleep(60 * time.Millisecond)

	it, err := db.NewIterator(IteratorOptions{})
	require.NoError(t, err)
	var keys []string
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.Equal(t, []string{"z-normal"}, keys)
	require.NoError(t, it.Close())

	require.NoError(t, db.Delete([]byte("z-normal")))
	require.NoError(t, db.PutWithTTL([]byte("only-expired"), []byte("v"), 20*time.Millisecond))
	time.Sleep(60 * time.Millisecond)
	it2, err := db.NewIterator(IteratorOptions{})
	require.NoError(t, err)
	defer it2.Close()
	it2.Rewind()
	require.False(t, it2.Valid())
}

func TestCoreTTLNoSweeperLifecycle(t *testing.T) {
	db, _ := newTTLDBNoSweeper(t)
	require.NoError(t, db.PutWithTTL([]byte("k"), []byte("v"), time.Second))
	require.NoError(t, db.Sync())
	require.NoError(t, db.Close())
}
