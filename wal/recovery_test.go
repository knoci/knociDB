package wal

import (
	"bytes"
	"os"
	"testing"
)

func TestRecoverTailTruncatesPartialChunk(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions
	opts.DirPath = dir
	opts.SegmentSize = 1 << 20
	opts.SegmentFileExt = ".SEG"
	opts.InitialSegmentID = 1

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	w.PendingWrites([]byte("hello"))
	if _, err := w.WriteAll(); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟崩溃：在文件尾部追加半个 chunk 头。
	path := SegmentFileName(dir, ".SEG", 1)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(opts)
	if err != nil {
		t.Fatalf("open after partial write: %v", err)
	}
	defer w2.Close()
	reader := w2.NewReader()
	data, _, err := reader.Next()
	if err != nil {
		t.Fatalf("read recovered record: %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("unexpected data: %q", data)
	}
	if _, _, err := reader.Next(); err == nil {
		t.Fatal("expected EOF after recovered record")
	}
}
