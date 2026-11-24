package knocidb

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestP0_DSCMETAEmptyFileRecovery(t *testing.T) {
	dir := t.TempDir()
	// Simulate crash before first Close() leaving a 0-byte DSCMETA file.
	metaPath := filepath.Join(dir, discardMetaName)
	require.NoError(t, os.WriteFile(metaPath, nil, 0o600))

	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err, "Open should self-heal 0-byte DSCMETA instead of failing with EOF")
	require.NoError(t, db.Put([]byte("k"), []byte("v")))
	got, err := db.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	require.NoError(t, db.Close())

	info, err := os.Stat(metaPath)
	require.NoError(t, err)
	require.Equal(t, int64(16), info.Size())
}

func TestP0_OrderedPerShardFlushAndStartupImmu(t *testing.T) {
	dir := t.TempDir()
	// Create 3 WAL tables on shard 0: M1 sets k=v1, M2 sets k=v2 & d=alive, M3 deletes d and sets k=v3.
	for seq := uint32(1); seq <= 3; seq++ {
		mt, err := openMemtable(memtableOptions{
			dirPath: dir,
			tableID: makeTableID(0, seq),
			shardID: 0,
			memSize: 1 * MB,
		})
		require.NoError(t, err)
		switch seq {
		case 1:
			require.NoError(t, mt.prepareRecord(&LogRecord{Key: []byte("k"), Value: []byte("v1"), Type: LogRecordNormal}, nextBatchID(), 1, DefaultWriteOptions))
		case 2:
			require.NoError(t, mt.prepareRecord(&LogRecord{Key: []byte("k"), Value: []byte("v2"), Type: LogRecordNormal}, nextBatchID(), 1, DefaultWriteOptions))
			require.NoError(t, mt.prepareRecord(&LogRecord{Key: []byte("d"), Value: []byte("alive"), Type: LogRecordNormal}, nextBatchID(), 1, DefaultWriteOptions))
		case 3:
			require.NoError(t, mt.prepareRecord(&LogRecord{Key: []byte("k"), Value: []byte("v3"), Type: LogRecordNormal}, nextBatchID(), 1, DefaultWriteOptions))
			require.NoError(t, mt.prepareRecord(&LogRecord{Key: []byte("d"), Type: LogRecordDeleted}, nextBatchID(), 1, DefaultWriteOptions))
		}
		require.NoError(t, mt.close())
	}
	// Also create a 4th empty active table so M1, M2, M3 are ALL in s.immu on Open!
	mt4, err := openMemtable(memtableOptions{
		dirPath: dir,
		tableID: makeTableID(0, 4),
		shardID: 0,
		memSize: 1 * MB,
	})
	require.NoError(t, err)
	require.NoError(t, mt4.close())

	opts := DefaultOptions
	opts.DirPath = dir
	opts.PartitionNum = 2
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.flushError())
	val, err := db.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), val)
	_, err = db.Get([]byte("d"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestP0_TableIDNoShardWraparound(t *testing.T) {
	id := makeTableID(0, 0xffff)
	next := nextTableID(0, id)
	require.Equal(t, uint32(0), tableShardID(next))
	require.Equal(t, uint32(0x10000), tableLocalID(next))

	parsed, ok := parseMemtableWALName("000000001.SEG.0.65536")
	require.True(t, ok)
	require.Equal(t, uint32(0), parsed.shardID)
	require.Equal(t, next, parsed.tableID)
	require.False(t, parsed.legacyExt)

	legacyParsed, ok := parseMemtableWALName("000000001.SEG.65537")
	require.True(t, ok)
	require.Equal(t, uint32(1), legacyParsed.shardID)
	require.Equal(t, uint32(1), tableLocalID(legacyParsed.tableID))
	require.True(t, legacyParsed.legacyExt)
}

func TestP0_BloomFilterGetStatsNoDeadlock(t *testing.T) {
	bf := NewBloomFilter(BloomFilterOptions{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
		DirPath:           t.TempDir(),
	}, 0)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			bf.Add([]byte{byte(i), byte(i >> 8)})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			stats := bf.GetStats()
			require.Contains(t, stats, "false_positive_rate")
		}
	}()
	wg.Wait()
}

func TestP0_SnapshotLatestIgnoresRecoverFilesAndValidatesPath(t *testing.T) {
	snapDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "20260101000000.000000000-000001.ZST"), []byte("good"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "recover-99999.ZST"), []byte("temp"), 0o600))

	latest, err := latestSnapshotFile(snapDir)
	require.NoError(t, err)
	require.Equal(t, "20260101000000.000000000-000001.ZST", filepath.Base(latest))

	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.SnapshotPath = ""
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	require.Error(t, db.ExportSnapshot())
	// DB must still be open because ExportSnapshot validated SnapshotPath before Close().
	require.NoError(t, db.Put([]byte("still-open"), []byte("yes")))
}

func TestP0_RaftConfigSnapshotEntriesAndOrderedConfigChange(t *testing.T) {
	cfg := DefaultClusterOptions(1, 1, "127.0.0.1:20001", t.TempDir())
	rc := cfg.GetRaftConfig()
	require.Equal(t, uint64(100000), rc.SnapshotEntries)
	require.Equal(t, uint64(10000), rc.CompactionOverhead)
	require.True(t, rc.OrderedConfigChange)

	// Small custom SnapshotEntries clamps CompactionOverhead cleanly.
	cfg.SnapshotEntries = 10
	rc2 := cfg.GetRaftConfig()
	require.Equal(t, uint64(10), rc2.SnapshotEntries)
	require.Less(t, rc2.CompactionOverhead, rc2.SnapshotEntries)
}

func TestP0_ShardMigrationAbortClearsPending(t *testing.T) {
	var m shardMigrations
	move1 := ShardMove{ShardID: 0, FromNode: 1, ToNode: 4, ToAddress: "127.0.0.1:10004"}
	res, completed := m.apply(migrationChange{Move: move1, CanStart: true})
	require.Equal(t, migrationPending, res.Value)
	require.False(t, completed)

	// Different move conflicts while pending.
	move2 := ShardMove{ShardID: 0, FromNode: 1, ToNode: 5, ToAddress: "127.0.0.1:10005"}
	res, _ = m.apply(migrationChange{Move: move2, CanStart: true})
	require.ErrorIs(t, raftResultError(res), ErrShardMigrationConflict)

	// Abort clears pending move.
	res, completed = m.apply(migrationChange{Move: move1, Abort: true})
	require.Equal(t, migrationAborted, res.Value)
	require.False(t, completed)
	require.Empty(t, m.Pending)

	// New move succeeds cleanly.
	res, _ = m.apply(migrationChange{Move: move2, CanStart: true})
	require.Equal(t, migrationPending, res.Value)
}

func TestP0_KVStateMachineRejectsOversizedSnapshotMetadata(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.SnapshotPath = t.TempDir()
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	sm := NewKVStateMachine(1, 1, db)
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(0xffffffff)))

	err = sm.RecoverFromSnapshot(&buf, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid snapshot metadata length")

	// Also verify done cancellation returns ErrSnapshotStopped immediately.
	done := make(chan struct{})
	close(done)
	require.ErrorIs(t, sm.SaveSnapshot(bytes.NewBuffer(nil), nil, done), statemachine.ErrSnapshotStopped)
	require.ErrorIs(t, sm.RecoverFromSnapshot(bytes.NewReader(nil), nil, done), statemachine.ErrSnapshotStopped)

	// Verify GetHash is race-safe under concurrent calls.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = sm.GetHash()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = sm.AppliedIndex()
		}
	}()
	wg.Wait()
	_ = time.Millisecond
}
