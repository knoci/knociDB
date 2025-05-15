package knocidb

import (
	"bytes"
	"testing"
)

type mockIterator struct {
	keys   [][]byte
	values []any
	index  int
	valid  bool
}

func newMockIterator(keys [][]byte, values []any) *mockIterator {
	return &mockIterator{
		keys:   keys,
		values: values,
		index:  0,
		valid:  len(keys) > 0,
	}
}

func (mi *mockIterator) Rewind() {
	mi.index = 0
	mi.valid = len(mi.keys) > 0
}

func (mi *mockIterator) Seek(key []byte) {
	for i, k := range mi.keys {
		if bytes.Compare(k, key) >= 0 {
			mi.index = i
			mi.valid = true
			return
		}
	}
	mi.valid = false
}

func (mi *mockIterator) Next() {
	if mi.index < len(mi.keys)-1 {
		mi.index++
	} else {
		mi.valid = false
	}
}

func (mi *mockIterator) Key() []byte {
	if !mi.valid {
		return nil
	}
	return mi.keys[mi.index]
}

func (mi *mockIterator) Value() any {
	if !mi.valid {
		return nil
	}
	return mi.values[mi.index]
}

func (mi *mockIterator) Valid() bool {
	return mi.valid
}

func (mi *mockIterator) Close() error {
	return nil
}

func TestIterHeapOperations(t *testing.T) {
	// 创建一些模拟迭代器
	iter1 := newMockIterator(
		[][]byte{[]byte("a"), []byte("c"), []byte("e")},
		[]any{"value1", "value3", "value5"},
	)
	iter2 := newMockIterator(
		[][]byte{[]byte("b"), []byte("d"), []byte("f")},
		[]any{"value2", "value4", "value6"},
	)

	sIter1 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 1,
		idx:     0,
		iter:    iter1,
	}
	sIter2 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 0,
		idx:     1,
		iter:    iter2,
	}

	h := iterHeap{sIter1, sIter2}

	if h.Len() != 2 {
		t.Errorf("Expected heap length 2, got %d", h.Len())
	}

	if !h.Less(0, 1) {
		t.Errorf("Expected 'a' to be less than 'b'")
	}

	h.Swap(0, 1)
	if !bytes.Equal(h[0].iter.Key(), []byte("b")) || !bytes.Equal(h[1].iter.Key(), []byte("a")) {
		t.Errorf("Swap failed, expected 'b' and 'a', got '%s' and '%s'", h[0].iter.Key(), h[1].iter.Key())
	}

	iter3 := newMockIterator(
		[][]byte{[]byte("g")},
		[]any{"value7"},
	)
	sIter3 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 2,
		idx:     2,
		iter:    iter3,
	}
	h.Push(sIter3)
	if h.Len() != 3 {
		t.Errorf("Expected heap length 3 after push, got %d", h.Len())
	}

	popped := h.Pop().(*singleIter)
	if !bytes.Equal(popped.iter.Key(), []byte("g")) {
		t.Errorf("Expected popped value 'g', got '%s'", popped.iter.Key())
	}
	if h.Len() != 2 {
		t.Errorf("Expected heap length 2 after pop, got %d", h.Len())
	}
}

func TestIteratorRewind(t *testing.T) {
	// 创建模拟迭代器
	iter1 := newMockIterator(
		[][]byte{[]byte("a"), []byte("c")},
		[]any{"value1", "value3"},
	)
	iter2 := newMockIterator(
		[][]byte{[]byte("b"), []byte("d")},
		[]any{"value2", "value4"},
	)

	sIter1 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 1,
		idx:     0,
		iter:    iter1,
	}
	sIter2 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 0,
		idx:     1,
		iter:    iter2,
	}

	iterator := &Iterator{
		itrs:       []*singleIter{sIter1, sIter2},
		versionMap: map[int]*singleIter{0: sIter2, 1: sIter1},
	}

	iterator.Rewind()

	if iterator.h.Len() != 2 {
		t.Errorf("Expected heap length 2, got %d", iterator.h.Len())
	}

	if !bytes.Equal(iterator.h[0].iter.Key(), []byte("a")) {
		t.Errorf("Expected top key 'a', got '%s'", iterator.h[0].iter.Key())
	}
}

func TestIteratorSeek(t *testing.T) {
	iter1 := newMockIterator(
		[][]byte{[]byte("a"), []byte("c"), []byte("e")},
		[]any{"value1", "value3", "value5"},
	)
	iter2 := newMockIterator(
		[][]byte{[]byte("b"), []byte("d"), []byte("f")},
		[]any{"value2", "value4", "value6"},
	)

	sIter1 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 1,
		idx:     0,
		iter:    iter1,
	}
	sIter2 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 0,
		idx:     1,
		iter:    iter2,
	}

	iterator := &Iterator{
		itrs:       []*singleIter{sIter1, sIter2},
		versionMap: map[int]*singleIter{0: sIter2, 1: sIter1},
	}

	iterator.Seek([]byte("c"))

	if iterator.h.Len() != 2 {
		t.Errorf("Expected heap length 2, got %d", iterator.h.Len())
	}

	if !bytes.Equal(iterator.h[0].iter.Key(), []byte("c")) {
		t.Errorf("Expected top key 'c', got '%s'", iterator.h[0].iter.Key())
	}

	iterator.Seek([]byte("z"))

	if iterator.h.Len() != 0 {
		t.Errorf("Expected empty heap, got length %d", iterator.h.Len())
	}
}

func TestIteratorNext(t *testing.T) {
	iter1 := newMockIterator(
		[][]byte{[]byte("a"), []byte("c"), []byte("e")},
		[]any{"value1", "value3", "value5"},
	)
	iter2 := newMockIterator(
		[][]byte{[]byte("b"), []byte("d"), []byte("f")},
		[]any{"value2", "value4", "value6"},
	)

	sIter1 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 1,
		idx:     0,
		iter:    iter1,
	}
	sIter2 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 0,
		idx:     1,
		iter:    iter2,
	}

	iterator := &Iterator{
		itrs:       []*singleIter{sIter1, sIter2},
		versionMap: map[int]*singleIter{0: sIter2, 1: sIter1},
		db:         &DB{},
	}

	iterator.Rewind()

	if !bytes.Equal(iterator.h[0].iter.Key(), []byte("a")) {
		t.Errorf("Expected initial key 'a', got '%s'", iterator.h[0].iter.Key())
	}

	iterator.Next()

	if !bytes.Equal(iterator.h[0].iter.Key(), []byte("b")) {
		t.Errorf("Expected next key 'b', got '%s'", iterator.h[0].iter.Key())
	}

	iterator.Next()
	iterator.Next()
	iterator.Next()

	if !bytes.Equal(iterator.h[0].iter.Key(), []byte("e")) {
		t.Errorf("Expected fifth key 'e', got '%s'", iterator.h[0].iter.Key())
	}

	iterator.Next()
	iterator.Next()

	if iterator.h.Len() != 0 {
		t.Errorf("Expected empty heap after iteration, got length %d", iterator.h.Len())
	}
}

func TestIteratorClose(t *testing.T) {
	iter1 := newMockIterator(
		[][]byte{[]byte("a")},
		[]any{"value1"},
	)
	iter2 := newMockIterator(
		[][]byte{[]byte("b")},
		[]any{"value2"},
	)

	sIter1 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 1,
		idx:     0,
		iter:    iter1,
	}
	sIter2 := &singleIter{
		iType:   MemItr,
		options: IteratorOptions{},
		version: 0,
		idx:     1,
		iter:    iter2,
	}

	iterator := &Iterator{
		itrs:       []*singleIter{sIter1, sIter2},
		versionMap: map[int]*singleIter{0: sIter2, 1: sIter1},
		db:         &DB{},
	}

	err := iterator.Close()
	if err != nil {
		t.Errorf("Expected no error on close, got %v", err)
	}
}
