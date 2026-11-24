package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP0_WALRecoverTailRejectsMidSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions
	opts.DirPath = dir
	opts.SegmentSize = 1 << 20
	opts.SegmentFileExt = ".SEG"
	opts.InitialSegmentID = 1

	w, err := Open(opts)
	require.NoError(t, err)
	_, err = w.Write([]byte("record-one-payload"))
	require.NoError(t, err)
	_, err = w.Write([]byte("record-two-payload"))
	require.NoError(t, err)
	_, err = w.Write([]byte("record-three-payload"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Flip a byte inside record 1 (mid-segment, with valid records 2 and 3 following).
	path := SegmentFileName(dir, ".SEG", 1)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[chunkHeaderSize+2] ^= 0xff
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	// Open must fail fast instead of silently truncating records 2 and 3.
	_, err = Open(opts)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidCRC)
}

func TestP0_WALNewReaderWithStartNoDeadlockAndLexicographic(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions
	opts.DirPath = dir
	opts.SegmentSize = 4 << 20
	opts.SegmentFileExt = ".SEG"

	w, err := Open(opts)
	require.NoError(t, err)
	defer w.Close()

	// Write enough records to span multiple 32KB blocks.
	payload := bytes.Repeat([]byte("a"), 1000)
	var targetPos *ChunkPosition
	for i := 0; i < 80; i++ {
		pos, err := w.Write(payload)
		require.NoError(t, err)
		if i == 35 {
			targetPos = pos
		}
	}
	require.NotNil(t, targetPos)
	require.Greater(t, targetPos.BlockNumber, uint32(0))

	// Concurrent writers + NewReaderWithStart must never deadlock on recursive RLock.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = w.Write([]byte("concurrent-write"))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			r, err := w.NewReaderWithStart(targetPos)
			require.NoError(t, err)
			data, pos, err := r.Next()
			require.NoError(t, err)
			require.Equal(t, targetPos.BlockNumber, pos.BlockNumber)
			require.Equal(t, targetPos.ChunkOffset, pos.ChunkOffset)
			require.Equal(t, payload, data)
		}
	}()
	wg.Wait()

	// Out-of-range startPos beyond all segments must not panic.
	r, err := w.NewReaderWithStart(&ChunkPosition{SegmentId: 999, BlockNumber: 0, ChunkOffset: 0})
	require.NoError(t, err)
	_, _, err = r.Next()
	require.Error(t, err)
}

func TestP0_WALDecodeChunkPositionMalformed(t *testing.T) {
	// Overflowing varint (>10 continuation bytes) previously caused negative index panic.
	overflow := bytes.Repeat([]byte{0xff}, 16)
	require.Nil(t, DecodeChunkPosition(overflow))
	// Truncated buffer must return nil instead of zeroed fields.
	var partial [3]byte
	binary.PutUvarint(partial[:], 1)
	require.Nil(t, DecodeChunkPosition(partial[:1]))
}

func TestP0_WALWriteBatchConcurrent(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions
	opts.DirPath = dir
	opts.SegmentSize = 4 << 20
	opts.SegmentFileExt = ".SEG"

	w, err := Open(opts)
	require.NoError(t, err)
	defer w.Close()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				positions, err := w.WriteBatch([][]byte{[]byte("one"), []byte("two")})
				require.NoError(t, err)
				require.Len(t, positions, 2)
				v, err := w.Read(positions[1])
				require.NoError(t, err)
				require.Equal(t, []byte("two"), v)
			}
		}()
	}
	wg.Wait()
}
