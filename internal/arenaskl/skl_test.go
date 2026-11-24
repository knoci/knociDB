package arenaskl

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSkiplistMVCCBasicAndReverse(t *testing.T) {
	s := NewSkiplist(64 * 1024)
	s.Put([]byte("a"), 1, ValueStruct{Value: []byte("a1"), Meta: 0})
	s.Put([]byte("b"), 2, ValueStruct{Value: []byte("b1"), Meta: 0})
	s.Put([]byte("a"), 3, ValueStruct{Value: []byte("a2"), Meta: 0, ExpireAt: 12345})
	s.Put([]byte("c"), 4, ValueStruct{Value: []byte("c1"), Meta: 0})
	s.Put([]byte("b"), 5, ValueStruct{Value: nil, Meta: 1})

	// Latest view
	v, ok := s.Get([]byte("a"), 0)
	require.True(t, ok)
	require.Equal(t, []byte("a2"), v.Value)
	require.Equal(t, int64(12345), v.ExpireAt)

	// Snapshot at seq=2 sees old a1, b1, and does not see c1 or b delete.
	v, ok = s.Get([]byte("a"), 2)
	require.True(t, ok)
	require.Equal(t, []byte("a1"), v.Value)
	v, ok = s.Get([]byte("b"), 2)
	require.True(t, ok)
	require.Equal(t, []byte("b1"), v.Value)
	_, ok = s.Get([]byte("c"), 2)
	require.False(t, ok)

	// Forward iterator at seq=2
	it := s.NewIterator(false, 2)
	var keys, vals []string
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value().Value))
	}
	require.Equal(t, []string{"a", "b"}, keys)
	require.Equal(t, []string{"a1", "b1"}, vals)

	// Reverse iterator at seq=3 sees c invisible, b=b1, a=a2
	rit := s.NewIterator(true, 3)
	keys, vals = nil, nil
	for rit.Rewind(); rit.Valid(); rit.Next() {
		keys = append(keys, string(rit.Key()))
		vals = append(vals, string(rit.Value().Value))
	}
	require.Equal(t, []string{"b", "a"}, keys)
	require.Equal(t, []string{"b1", "a2"}, vals)

	// Reverse Seek past max key returns largest visible key <= target
	rit.Seek([]byte("z"))
	require.True(t, rit.Valid())
	require.Equal(t, []byte("b"), rit.Key())
	rit.Seek([]byte("a"))
	require.True(t, rit.Valid())
	require.Equal(t, []byte("a"), rit.Key())
	require.Equal(t, []byte("a2"), rit.Value().Value)
}

func TestSkiplistArenaOverflowNoPanic(t *testing.T) {
	// Tiny 4KB arena must safely overflow on a 32KB value without panicking.
	s := NewSkiplist(4096)
	big := make([]byte, 32*1024)
	for i := range big {
		big[i] = byte(i)
	}
	s.Put([]byte("big"), 1, ValueStruct{Value: big})
	got, ok := s.Get([]byte("big"), 1)
	require.True(t, ok)
	require.Equal(t, big, got.Value)
	require.Greater(t, s.MemSize(), int64(32*1024))
}

func TestSkiplistConcurrentPutGetIterate(t *testing.T) {
	s := NewSkiplist(256 * 1024)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 1; i <= 500; i++ {
			k := []byte(fmt.Sprintf("k-%04d", i%100))
			s.Put(k, uint64(i), ValueStruct{Value: []byte(fmt.Sprintf("v-%d", i))})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, _ = s.Get([]byte(fmt.Sprintf("k-%04d", i%100)), uint64(i+1))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			it := s.NewIterator(i%2 == 1, uint64(i*10+1))
			for it.Rewind(); it.Valid(); it.Next() {
				_ = it.Key()
				_ = it.Value()
			}
			_ = it.Close()
		}
	}()
	wg.Wait()
}

func TestSkiplistPutOrReplaceAndMultiChunk(t *testing.T) {
	s := NewSkiplist(64 * 1024)
	s.PutOrReplace([]byte("k1"), 1, ValueStruct{Value: []byte("v1"), Meta: 0}, true)
	s.PutOrReplace([]byte("k1"), 2, ValueStruct{Value: []byte("v2"), Meta: 0, ExpireAt: 999}, true)

	// In-place replace keeps latest value visible to latest & newer seq snapshots
	got, ok := s.Get([]byte("k1"), 0)
	require.True(t, ok)
	require.Equal(t, []byte("v2"), got.Value)
	require.Equal(t, int64(999), got.ExpireAt)

	// With replace=false (active iterator mode), a new MVCC node is inserted before the old one
	s.PutOrReplace([]byte("k1"), 5, ValueStruct{Value: []byte("v5"), Meta: 0}, false)
	gotOld, ok := s.Get([]byte("k1"), 3)
	require.True(t, ok)
	require.Equal(t, []byte("v2"), gotOld.Value)
	gotNew, ok := s.Get([]byte("k1"), 5)
	require.True(t, ok)
	require.Equal(t, []byte("v5"), gotNew.Value)

	// Cross multiple 2 MiB chunks + oversized > 1 MiB blob
	payload := make([]byte, 16*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	for i := 0; i < 200; i++ { // > 3.2 MiB
		k := []byte(fmt.Sprintf("chunk-key-%04d", i))
		s.PutOrReplace(k, uint64(10+i), ValueStruct{Value: payload}, true)
	}
	huge := make([]byte, 2*1024*1024+123)
	huge[0] = 0xAB
	huge[len(huge)-1] = 0xCD
	s.PutOrReplace([]byte("huge-blob"), 999, ValueStruct{Value: huge}, true)

	gotHuge, ok := s.Get([]byte("huge-blob"), 0)
	require.True(t, ok)
	require.Equal(t, huge, gotHuge.Value)

	gotChunk, ok := s.Get([]byte("chunk-key-0199"), 0)
	require.True(t, ok)
	require.Equal(t, payload, gotChunk.Value)
}
