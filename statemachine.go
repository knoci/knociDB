package knocidb

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/knoci/knocidb/util"
	"github.com/lni/dragonboat/v4/statemachine"
)

// 操作类型常量
const (
	OpPut    uint32 = 1
	OpDelete uint32 = 2
	OpBatch  uint32 = 3
)

const (
	raftMetaFileName      = "RAFTMETA"
	raftSnapshotV2Magic   = "KRS2"
	maxAppliedBatchWindow = 16384
)

// Command 表示要通过Raft共识执行的命令
type Command struct {
	Op       uint32 `json:"op"`                  // 操作类型：1=Put, 2=Delete, 3=Batch
	Key      []byte `json:"key"`                 // 键
	Value    []byte `json:"value"`               // 值
	BatchID  uint64 `json:"batch_id,omitempty"`  // 批处理ID（非零时启用幂等去重）
	ExpireAt int64  `json:"expire_at,omitempty"` // 绝对过期时间戳 UnixMilli（Raft 确定性 TTL）
}

// BatchCommand 表示批量操作命令
type BatchCommand struct {
	Op       uint32    `json:"op"` // 操作类型，固定为OpBatch
	Commands []Command `json:"commands"`
	BatchID  uint64    `json:"batch_id"` // 批处理ID
}

type raftWriteCommand struct {
	Command
	Commands  []Command        `json:"commands"`
	Migration *migrationChange `json:"migration,omitempty"`
}

func validateRaftWrite(cmd Command) error {
	if cmd.Op != OpPut && cmd.Op != OpDelete {
		return ErrInvalidRaftCommand
	}
	if len(cmd.Key) == 0 {
		return ErrKeyIsEmpty
	}
	return nil
}

// Update reserves its error return for unrecoverable storage failures.
func rejectedRaftResult(err error) statemachine.Result {
	return statemachine.Result{Data: []byte(err.Error())}
}

func raftResultError(result statemachine.Result) error {
	if len(result.Data) == 0 {
		return nil
	}
	return decodeRaftError(string(result.Data))
}

func decodeRaftError(message string) error {
	switch message {
	case ErrInvalidRaftCommand.Error():
		return ErrInvalidRaftCommand
	case ErrKeyIsEmpty.Error():
		return ErrKeyIsEmpty
	case ErrStaleShardMapEpoch.Error():
		return ErrStaleShardMapEpoch
	case ErrShardMigrationConflict.Error():
		return ErrShardMigrationConflict
	default:
		return errors.New(message)
	}
}

// KVStateMachine 将 KnociDB 引擎与 Dragonboat Multi-Raft 共识集成。
// 内部支持：
// - IOnDiskStateMachine（通过 asOnDisk() 适配器）：重启直接恢复 appliedIndex 无需全量日志回放；
// - 紧凑二进制命令编解码（兼容旧版 JSON）；
// - 非关闭在线 MVCC 流式快照（KRS2 格式，零停服、零临时大文件）；
// - BatchID 幂等去重与 Raft 确定性绝对时间戳 TTL。
type KVStateMachine struct {
	clusterID    uint64
	nodeID       uint64
	db           KVEngine // 数据库核心接口
	appliedIndex uint64
	migrations   shardMigrations
	mu           sync.RWMutex

	appliedBatchSet  map[uint64]struct{}
	appliedBatchRing []uint64
}

type kvSnapshotMetadata struct {
	AppliedIndex uint64           `json:"applied_index"`
	ClusterID    uint64           `json:"cluster_id"`
	NodeID       uint64           `json:"node_id"`
	Timestamp    uint64           `json:"timestamp"`
	Migrations   *shardMigrations `json:"migrations,omitempty"`
}

type raftPersistedMeta struct {
	AppliedIndex uint64           `json:"applied_index"`
	Migrations   *shardMigrations `json:"migrations,omitempty"`
}

// NewKVStateMachine 创建一个新的KV状态机实例
func NewKVStateMachine(clusterID, nodeID uint64, db KVEngine) *KVStateMachine {
	return &KVStateMachine{
		clusterID:       clusterID,
		nodeID:          nodeID,
		db:              db,
		appliedIndex:    0,
		appliedBatchSet: make(map[uint64]struct{}),
	}
}

// onDiskKVStateMachine 适配 Dragonboat statemachine.IOnDiskStateMachine 接口。
type onDiskKVStateMachine struct {
	*KVStateMachine
}

func (s *KVStateMachine) asOnDisk() statemachine.IOnDiskStateMachine {
	return &onDiskKVStateMachine{KVStateMachine: s}
}

func (s *KVStateMachine) raftMetaPath() string {
	if s.db == nil {
		return ""
	}
	dir := s.db.GetOptions().DirPath
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, raftMetaFileName)
}

func (s *KVStateMachine) loadPersistedMetaLocked() (uint64, error) {
	path := s.raftMetaPath()
	if path == "" {
		return s.appliedIndex, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s.appliedIndex, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return s.appliedIndex, nil
	}
	var meta raftPersistedMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0, fmt.Errorf("decode %s: %w", raftMetaFileName, err)
	}
	s.appliedIndex = meta.AppliedIndex
	if meta.Migrations != nil {
		s.migrations = *meta.Migrations
	}
	return s.appliedIndex, nil
}

func (s *KVStateMachine) savePersistedMetaLocked() error {
	path := s.raftMetaPath()
	if path == "" {
		return nil
	}
	meta := raftPersistedMeta{
		AppliedIndex: s.appliedIndex,
	}
	if !s.migrations.empty() {
		m := s.migrations
		meta.Migrations = &m
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := util.RenameDurable(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return util.SyncDir(filepath.Dir(path))
}

func (s *KVStateMachine) isBatchAppliedLocked(batchID uint64) bool {
	if batchID == 0 || s.appliedBatchSet == nil {
		return false
	}
	_, ok := s.appliedBatchSet[batchID]
	return ok
}

func (s *KVStateMachine) recordBatchAppliedLocked(batchID uint64) {
	if batchID == 0 {
		return
	}
	if s.appliedBatchSet == nil {
		s.appliedBatchSet = make(map[uint64]struct{})
	}
	if _, exists := s.appliedBatchSet[batchID]; exists {
		return
	}
	if len(s.appliedBatchRing) >= maxAppliedBatchWindow {
		oldest := s.appliedBatchRing[0]
		s.appliedBatchRing = s.appliedBatchRing[1:]
		delete(s.appliedBatchSet, oldest)
	}
	s.appliedBatchSet[batchID] = struct{}{}
	s.appliedBatchRing = append(s.appliedBatchRing, batchID)
}

// Open 实现 IOnDiskStateMachine.Open，从磁盘加载持久化 appliedIndex。
func (o *onDiskKVStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	if err := checkSnapshotDone(stopc); err != nil {
		return 0, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.loadPersistedMetaLocked()
}

// Sync 实现 IOnDiskStateMachine.Sync，刷盘底层存储与 Raft 元数据。
func (o *onDiskKVStateMachine) Sync() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if syncer, ok := o.db.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return err
		}
	}
	return o.savePersistedMetaLocked()
}

// Lookup 实现了只读查询接口：优先零拷贝直读 raftLookupKey，同时兼容旧版 JSON []byte 查询。
func (s *KVStateMachine) Lookup(query interface{}) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	switch q := query.(type) {
	case raftLookupKey:
		return s.db.Get([]byte(q))
	case []byte:
		var cmd Command
		if err := json.Unmarshal(q, &cmd); err != nil {
			return nil, err
		}
		return s.db.Get(cmd.Key)
	default:
		return nil, errors.New("query args must be []byte or raftLookupKey")
	}
}

// AppliedIndex 返回最后处理的应用日志索引。
func (s *KVStateMachine) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedIndex
}

// Update 处理单条日志（兼容旧版 IStateMachine 与现有单元测试）。
func (s *KVStateMachine) Update(entry statemachine.Entry) (statemachine.Result, error) {
	entries := []statemachine.Entry{entry}
	out, err := s.UpdateBatch(entries)
	if err != nil {
		return statemachine.Result{}, err
	}
	return out[0].Result, nil
}

// Update 实现 IOnDiskStateMachine.Update，批量应用 Dragonboat 日志切片。
func (o *onDiskKVStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	return o.KVStateMachine.UpdateBatch(entries)
}

// UpdateBatch 批量应用 Raft 日志条目：合并同批普通写为单次 Batch 提交，支持 BatchID 幂等去重与确定性 TTL。
func (s *KVStateMachine) UpdateBatch(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		pendingBatch      *Batch
		pendingAppliedIdx uint64
		pendingBatchIDs   []uint64
	)
	commitPendingBatch := func() error {
		if pendingBatch == nil {
			return nil
		}
		err := pendingBatch.Commit()
		pendingBatch.Close()
		pendingBatch = nil
		if err != nil {
			pendingAppliedIdx = 0
			pendingBatchIDs = nil
			return err
		}
		if pendingAppliedIdx > s.appliedIndex {
			s.appliedIndex = pendingAppliedIdx
		}
		for _, id := range pendingBatchIDs {
			s.recordBatchAppliedLocked(id)
		}
		pendingAppliedIdx = 0
		pendingBatchIDs = nil
		return nil
	}
	ensureBatch := func() *Batch {
		if pendingBatch == nil {
			pendingBatch = s.db.NewBatch(DefaultBatchOptions)
		}
		return pendingBatch
	}
	defer func() {
		if pendingBatch != nil {
			pendingBatch.Close()
		}
	}()

	for i := range entries {
		entry := &entries[i]
		effectiveApplied := s.appliedIndex
		if pendingAppliedIdx > effectiveApplied {
			effectiveApplied = pendingAppliedIdx
		}
		if entry.Index <= effectiveApplied {
			entry.Result = statemachine.Result{}
			continue
		}
		write, err := decodeRaftWriteCommand(entry.Cmd)
		if err != nil {
			if ferr := commitPendingBatch(); ferr != nil {
				return nil, fmt.Errorf("batch commit before rejected command failed: %w", ferr)
			}
			s.appliedIndex = entry.Index
			entry.Result = rejectedRaftResult(ErrInvalidRaftCommand)
			continue
		}
		isDupBatch := write.BatchID != 0 && s.isBatchAppliedLocked(write.BatchID)
		if !isDupBatch && write.BatchID != 0 {
			for _, id := range pendingBatchIDs {
				if id == write.BatchID {
					isDupBatch = true
					break
				}
			}
		}
		if isDupBatch {
			if ferr := commitPendingBatch(); ferr != nil {
				return nil, fmt.Errorf("batch commit before duplicate batch failed: %w", ferr)
			}
			s.appliedIndex = entry.Index
			entry.Result = statemachine.Result{Value: 0}
			continue
		}

		switch write.Op {
		case OpPut, OpDelete:
			if err := validateRaftWrite(write.Command); err != nil {
				if ferr := commitPendingBatch(); ferr != nil {
					return nil, fmt.Errorf("batch commit before rejected write failed: %w", ferr)
				}
				s.appliedIndex = entry.Index
				entry.Result = rejectedRaftResult(err)
				continue
			}
			b := ensureBatch()
			if write.Op == OpPut {
				if write.ExpireAt > 0 {
					if err := b.PutWithExpireAt(write.Key, write.Value, write.ExpireAt); err != nil {
						return nil, fmt.Errorf("put with expireAt failed: %w", err)
					}
				} else {
					if err := b.Put(write.Key, write.Value); err != nil {
						return nil, fmt.Errorf("put failed: %w", err)
					}
				}
			} else {
				if err := b.Delete(write.Key); err != nil {
					return nil, fmt.Errorf("delete failed: %w", err)
				}
			}
			if write.BatchID != 0 {
				pendingBatchIDs = append(pendingBatchIDs, write.BatchID)
			}
			pendingAppliedIdx = entry.Index
			entry.Result = statemachine.Result{Value: 1}

		case OpBatch:
			var invalidErr error
			for _, cmd := range write.Commands {
				if err := validateRaftWrite(cmd); err != nil {
					invalidErr = err
					break
				}
			}
			if invalidErr != nil {
				if ferr := commitPendingBatch(); ferr != nil {
					return nil, fmt.Errorf("batch commit before rejected batch failed: %w", ferr)
				}
				s.appliedIndex = entry.Index
				entry.Result = rejectedRaftResult(invalidErr)
				continue
			}
			b := ensureBatch()
			for _, cmd := range write.Commands {
				if cmd.Op == OpPut {
					if cmd.ExpireAt > 0 {
						if err := b.PutWithExpireAt(cmd.Key, cmd.Value, cmd.ExpireAt); err != nil {
							return nil, fmt.Errorf("batch put with expireAt failed: %w", err)
						}
					} else {
						if err := b.Put(cmd.Key, cmd.Value); err != nil {
							return nil, fmt.Errorf("batch put failed: %w", err)
						}
					}
				} else {
					if err := b.Delete(cmd.Key); err != nil {
						return nil, fmt.Errorf("batch delete failed: %w", err)
					}
				}
			}
			if write.BatchID != 0 {
				pendingBatchIDs = append(pendingBatchIDs, write.BatchID)
			}
			pendingAppliedIdx = entry.Index
			entry.Result = statemachine.Result{Value: uint64(len(write.Commands))}

		case opShardMigration:
			if ferr := commitPendingBatch(); ferr != nil {
				return nil, fmt.Errorf("batch commit before migration failed: %w", ferr)
			}
			if write.Migration == nil || !write.Migration.valid(1) {
				s.appliedIndex = entry.Index
				entry.Result = rejectedRaftResult(ErrInvalidRaftCommand)
				continue
			}
			res, _ := s.migrations.apply(*write.Migration)
			s.appliedIndex = entry.Index
			entry.Result = res

		default:
			if ferr := commitPendingBatch(); ferr != nil {
				return nil, fmt.Errorf("batch commit before unknown op failed: %w", ferr)
			}
			s.appliedIndex = entry.Index
			entry.Result = rejectedRaftResult(ErrInvalidRaftCommand)
		}
	}

	if err := commitPendingBatch(); err != nil {
		return nil, fmt.Errorf("batch commit failed: %w", err)
	}
	return entries, nil
}

const (
	maxSnapshotMetadataSize = 16 << 20 // 16 MiB 上限，防止恶意/损坏快照头 OOM
)

func checkSnapshotDone(done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return statemachine.ErrSnapshotStopped
	default:
		return nil
	}
}

func copyWithDone(w io.Writer, r io.Reader, done <-chan struct{}) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	for {
		if err := checkSnapshotDone(done); err != nil {
			return written, err
		}
		nr, rerr := r.Read(buf)
		if nr > 0 {
			nw, werr := w.Write(buf[:nr])
			written += int64(nw)
			if werr != nil {
				return written, werr
			}
			if nw != nr {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return written, nil
			}
			return written, rerr
		}
	}
}

type kvSnapshotContext struct {
	appliedIndex uint64
	migrations   shardMigrations
	iter         *Iterator
	localDB      *DB
}

// PrepareSnapshot 捕获一致点元数据与零冻结 MVCC 迭代器，全程不关闭数据库、不阻塞线上读写。
func (o *onDiskKVStateMachine) PrepareSnapshot() (interface{}, error) {
	return o.KVStateMachine.prepareSnapshot()
}

func (s *KVStateMachine) prepareSnapshot() (*kvSnapshotContext, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	local, ok := s.db.(*DB)
	if !ok {
		return nil, ErrUnsupportedIndex
	}
	it, err := local.NewIterator(IteratorOptions{})
	if err != nil {
		return nil, fmt.Errorf("open mvcc snapshot iterator failed: %w", err)
	}
	return &kvSnapshotContext{
		appliedIndex: s.appliedIndex,
		migrations:   s.migrations,
		iter:         it,
		localDB:      local,
	}, nil
}

// SaveSnapshot 实现 IOnDiskStateMachine.SaveSnapshot：在线流式编码 MVCC 快照至 w，全程零停服。
func (o *onDiskKVStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	snapCtx, ok := ctx.(*kvSnapshotContext)
	if !ok || snapCtx == nil {
		return errors.New("invalid snapshot context")
	}
	return o.KVStateMachine.saveSnapshotFromContext(snapCtx, w, done)
}

// SaveSnapshot 保留旧版 IStateMachine 签名（供既有测试直接调用），内部走在线非关闭流式快照。
func (s *KVStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	if err := checkSnapshotDone(done); err != nil {
		return err
	}
	snapCtx, err := s.prepareSnapshot()
	if err != nil {
		return err
	}
	return s.saveSnapshotFromContext(snapCtx, w, done)
}

func (s *KVStateMachine) saveSnapshotFromContext(snapCtx *kvSnapshotContext, w io.Writer, done <-chan struct{}) error {
	defer func() {
		if snapCtx.iter != nil {
			_ = snapCtx.iter.Close()
		}
	}()
	if err := checkSnapshotDone(done); err != nil {
		return err
	}

	// 持久化当前 Raft 元数据，便于磁盘重启对齐。
	s.mu.Lock()
	_ = s.savePersistedMetaLocked()
	s.mu.Unlock()

	// 若配置了对象存储冷层，则同步触发一次在线异步快照（同样不关闭 DB）。
	if snapCtx.localDB != nil {
		opts := snapCtx.localDB.GetOptions()
		if opts.ObjectTier.Enabled {
			task, err := snapCtx.localDB.StartSnapshot()
			if err != nil {
				return fmt.Errorf("online cloud snapshot failed: %w", err)
			}
			if err := task.Wait(); err != nil {
				return fmt.Errorf("wait online cloud snapshot failed: %w", err)
			}
		}
	}

	if err := checkSnapshotDone(done); err != nil {
		return err
	}

	metadata := kvSnapshotMetadata{
		AppliedIndex: snapCtx.appliedIndex,
		ClusterID:    s.clusterID,
		NodeID:       s.nodeID,
		Timestamp:    uint64(time.Now().Unix()),
	}
	if !snapCtx.migrations.empty() {
		m := snapCtx.migrations
		metadata.Migrations = &m
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot metadata: %w", err)
	}
	checksum := crc32.ChecksumIEEE(data)

	// 写入 KRS2 魔数 + 元数据头 + CRC32
	if _, err := w.Write([]byte(raftSnapshotV2Magic)); err != nil {
		return fmt.Errorf("write snapshot magic failed: %w", err)
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write metadata length failed: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write metadata failed: %w", err)
	}
	binary.LittleEndian.PutUint32(hdr[:], checksum)
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write checksum failed: %w", err)
	}

	// ZSTD 流式编码全部存活 KV + ExpireAt
	zw, err := zstd.NewWriter(w)
	if err != nil {
		return fmt.Errorf("create zstd snapshot writer failed: %w", err)
	}
	var varBuf [3 * binary.MaxVarintLen64]byte
	for snapCtx.iter.Rewind(); snapCtx.iter.Valid(); snapCtx.iter.Next() {
		if err := checkSnapshotDone(done); err != nil {
			_ = zw.Close()
			return err
		}
		key := snapCtx.iter.Key()
		val := snapCtx.iter.Value()
		if err := snapCtx.iter.Err(); err != nil {
			_ = zw.Close()
			return fmt.Errorf("iterate snapshot kv failed: %w", err)
		}
		var expireAt int64
		if snapCtx.localDB != nil {
			if left, ok, terr := snapCtx.localDB.GetTTL(key); terr == nil && ok && left > 0 {
				expireAt = time.Now().Add(left).UnixMilli()
			}
		}
		n := binary.PutUvarint(varBuf[:], uint64(len(key)))
		n += binary.PutUvarint(varBuf[n:], uint64(len(val)))
		n += binary.PutVarint(varBuf[n:], expireAt)
		if _, err := zw.Write(varBuf[:n]); err != nil {
			_ = zw.Close()
			return fmt.Errorf("write kv header failed: %w", err)
		}
		if _, err := zw.Write(key); err != nil {
			_ = zw.Close()
			return fmt.Errorf("write kv key failed: %w", err)
		}
		if len(val) > 0 {
			if _, err := zw.Write(val); err != nil {
				_ = zw.Close()
				return fmt.Errorf("write kv value failed: %w", err)
			}
		}
	}
	if err := snapCtx.iter.Err(); err != nil {
		_ = zw.Close()
		return fmt.Errorf("snapshot iterator error: %w", err)
	}
	// 写入 keyLen=0 终止标记
	if _, err := zw.Write([]byte{0}); err != nil {
		_ = zw.Close()
		return fmt.Errorf("write snapshot eof marker failed: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("close zstd snapshot writer failed: %w", err)
	}
	return nil
}

// RecoverFromSnapshot 实现 IOnDiskStateMachine.RecoverFromSnapshot。
func (o *onDiskKVStateMachine) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	return o.KVStateMachine.RecoverFromSnapshot(r, nil, done)
}

// RecoverFromSnapshot 从流式快照（KRS2，全程不关库）或旧版 v1（.ZST）快照恢复状态机。
func (s *KVStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	if err := checkSnapshotDone(done); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkSnapshotDone(done); err != nil {
		return err
	}

	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return fmt.Errorf("read metadata length failed: %w", err)
	}

	if string(prefix[:]) == raftSnapshotV2Magic {
		return s.recoverFromStreamingV2Locked(r, done)
	}
	return s.recoverFromLegacyV1Locked(prefix[:], r, done)
}

func (s *KVStateMachine) recoverFromStreamingV2Locked(r io.Reader, done <-chan struct{}) error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return fmt.Errorf("read metadata length failed: %w", err)
	}
	metadataLen := binary.LittleEndian.Uint32(lenBuf[:])
	if metadataLen == 0 || metadataLen > maxSnapshotMetadataSize {
		return fmt.Errorf("invalid snapshot metadata length: %d", metadataLen)
	}
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(r, metadataBytes); err != nil {
		return fmt.Errorf("read metadata failed: %w", err)
	}
	var checksumBuf [4]byte
	if _, err := io.ReadFull(r, checksumBuf[:]); err != nil {
		return fmt.Errorf("read checksum failed: %w", err)
	}
	expectedChecksum := binary.LittleEndian.Uint32(checksumBuf[:])
	if actual := crc32.ChecksumIEEE(metadataBytes); actual != expectedChecksum {
		return fmt.Errorf("checksum invalid: expected %d, actual %d", expectedChecksum, actual)
	}
	var metadata kvSnapshotMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return fmt.Errorf("unmarshal metadata failed: %w", err)
	}

	// 清空当前本地库中的旧键（不关闭 DB、无句柄重开风险）。
	if local, ok := s.db.(*DB); ok {
		it, err := local.NewIterator(IteratorOptions{})
		if err != nil {
			return fmt.Errorf("open iterator to clear stale state: %w", err)
		}
		var staleKeys [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			staleKeys = append(staleKeys, append([]byte(nil), it.Key()...))
		}
		_ = it.Close()
		const clearBatchSize = 512
		for start := 0; start < len(staleKeys); start += clearBatchSize {
			if err := checkSnapshotDone(done); err != nil {
				return err
			}
			end := min(start+clearBatchSize, len(staleKeys))
			b := s.db.NewBatch(DefaultBatchOptions)
			for _, k := range staleKeys[start:end] {
				if err := b.Delete(k); err != nil {
					b.Close()
					return fmt.Errorf("clear stale key failed: %w", err)
				}
			}
			if err := b.Commit(); err != nil {
				b.Close()
				return fmt.Errorf("commit clear stale keys failed: %w", err)
			}
			b.Close()
		}
	}

	zr, err := zstd.NewReader(r)
	if err != nil {
		return fmt.Errorf("create zstd snapshot reader failed: %w", err)
	}
	defer zr.Close()
	br := bufio.NewReaderSize(zr, 64*1024)

	const restoreBatchSize = 512
	batch := s.db.NewBatch(DefaultBatchOptions)
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()
	inBatch := 0

	for {
		if err := checkSnapshotDone(done); err != nil {
			return err
		}
		keyLen, err := binary.ReadUvarint(br)
		if err != nil {
			return fmt.Errorf("read snapshot key len failed: %w", err)
		}
		if keyLen == 0 {
			break
		}
		if keyLen > maxRPCValueSize {
			return fmt.Errorf("invalid snapshot key len: %d", keyLen)
		}
		valLen, err := binary.ReadUvarint(br)
		if err != nil {
			return fmt.Errorf("read snapshot val len failed: %w", err)
		}
		if valLen > maxRPCValueSize {
			return fmt.Errorf("invalid snapshot val len: %d", valLen)
		}
		expireAt, err := binary.ReadVarint(br)
		if err != nil {
			return fmt.Errorf("read snapshot expireAt failed: %w", err)
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(br, key); err != nil {
			return fmt.Errorf("read snapshot key failed: %w", err)
		}
		val := make([]byte, valLen)
		if valLen > 0 {
			if _, err := io.ReadFull(br, val); err != nil {
				return fmt.Errorf("read snapshot val failed: %w", err)
			}
		}
		if expireAt > 0 {
			if err := batch.PutWithExpireAt(key, val, expireAt); err != nil {
				return fmt.Errorf("restore kv with ttl failed: %w", err)
			}
		} else {
			if err := batch.Put(key, val); err != nil {
				return fmt.Errorf("restore kv failed: %w", err)
			}
		}
		inBatch++
		if inBatch >= restoreBatchSize {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("commit restore batch failed: %w", err)
			}
			batch.Close()
			batch = s.db.NewBatch(DefaultBatchOptions)
			inBatch = 0
		}
	}
	if inBatch > 0 {
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("commit final restore batch failed: %w", err)
		}
	}
	if syncer, ok := s.db.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return fmt.Errorf("sync restored db failed: %w", err)
		}
	}

	s.appliedIndex = metadata.AppliedIndex
	s.migrations = shardMigrations{}
	if metadata.Migrations != nil {
		s.migrations = *metadata.Migrations
	}
	s.appliedBatchSet = make(map[uint64]struct{})
	s.appliedBatchRing = nil
	return s.savePersistedMetaLocked()
}

func (s *KVStateMachine) recoverFromLegacyV1Locked(lenBuf []byte, r io.Reader, done <-chan struct{}) error {
	metadataLen := binary.LittleEndian.Uint32(lenBuf)
	if metadataLen == 0 || metadataLen > maxSnapshotMetadataSize {
		return fmt.Errorf("invalid snapshot metadata length: %d", metadataLen)
	}

	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(r, metadataBytes); err != nil {
		return fmt.Errorf("read metadata failed: %w", err)
	}

	checksumBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, checksumBuf); err != nil {
		return fmt.Errorf("read checksum failed: %w", err)
	}
	expectedChecksum := binary.LittleEndian.Uint32(checksumBuf)
	actualChecksum := crc32.ChecksumIEEE(metadataBytes)
	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum invalid: expected %d, actual %d", expectedChecksum, actualChecksum)
	}

	var metadata kvSnapshotMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return fmt.Errorf("unmarshal matadata failed: %w", err)
	}

	options := s.db.GetOptions()
	snapDir := options.SnapshotPath
	if snapDir == "" {
		return fmt.Errorf("snapshotpath empty")
	}
	if err := os.MkdirAll(snapDir, os.ModePerm); err != nil {
		return fmt.Errorf("create snapshot dir failed: %w", err)
	}

	var err error
	var importPath string
	var dataSize int64
	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(r, sizeBuf); err == nil {
		dataSize = int64(binary.LittleEndian.Uint64(sizeBuf))
		if dataSize < 0 {
			return fmt.Errorf("invalid snapshot data size: %d", dataSize)
		}
	} else if err != io.EOF {
		return fmt.Errorf("read snapshot data size failed: %w", err)
	}

	if dataSize > 0 {
		tmp, err := os.CreateTemp(snapDir, "recover-*.ZST")
		if err != nil {
			return fmt.Errorf("create recover snapshot failed: %w", err)
		}
		tmpPath := tmp.Name()
		if n, err := copyWithDone(tmp, io.LimitReader(r, dataSize), done); err != nil || n != dataSize {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
			if errors.Is(err, statemachine.ErrSnapshotStopped) {
				return err
			}
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("read snapshot data failed: %w", err)
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("close recover snapshot failed: %w", err)
		}
		importPath = tmpPath
		defer os.Remove(importPath)
	} else {
		importPath, err = latestSnapshotFile(snapDir)
		if err != nil {
			return fmt.Errorf("can not find snapshot file: %w", err)
		}
	}

	if err := checkSnapshotDone(done); err != nil {
		return err
	}

	if err = s.db.ImportSnapshot(importPath); err != nil {
		if local, ok := s.db.(*DB); ok && !local.isClosing() {
			return fmt.Errorf("import snapshot failed: %w", err)
		}
		db, openErr := Open(options)
		if openErr != nil {
			return fmt.Errorf("reopen database failed: %v, import snapshot failed: %w", openErr, err)
		}
		s.db = db
		return fmt.Errorf("import snapshot failed: %w", err)
	}

	db, err := Open(options)
	if err != nil {
		return fmt.Errorf("reopen database failed after import snapshot: %w", err)
	}
	s.db = db

	s.appliedIndex = metadata.AppliedIndex
	s.migrations = shardMigrations{}
	if metadata.Migrations != nil {
		s.migrations = *metadata.Migrations
	}
	return s.savePersistedMetaLocked()
}

// Close 实现了statemachine.IStateMachine/IOnDiskStateMachine接口的Close方法
func (s *KVStateMachine) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.savePersistedMetaLocked()
	return s.db.Close()
}

// GetHash 实现了statemachine.IStateMachine接口的GetHash方法
func (s *KVStateMachine) GetHash() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedIndex, nil
}
