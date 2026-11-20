package knocidb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4/statemachine"
)

// 操作类型常量
const (
	OpPut    uint32 = 1
	OpDelete uint32 = 2
	OpBatch  uint32 = 3
)

// Command 表示要通过Raft共识执行的命令
type Command struct {
	Op      uint32 `json:"op"`                 // 操作类型：1=Put, 2=Delete, 3=Batch
	Key     []byte `json:"key"`                // 键
	Value   []byte `json:"value"`              // 值
	BatchID uint64 `json:"batch_id,omitempty"` // 批处理ID
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

// KVStateMachine 实现了Dragonboat的statemachine.IStateMachine接口
// 它将数据库操作与Raft共识机制集成
type KVStateMachine struct {
	clusterID    uint64
	nodeID       uint64
	db           KVEngine // 数据库核心接口
	appliedIndex uint64
	migrations   shardMigrations
	mu           sync.RWMutex
}

type kvSnapshotMetadata struct {
	AppliedIndex uint64           `json:"applied_index"`
	ClusterID    uint64           `json:"cluster_id"`
	NodeID       uint64           `json:"node_id"`
	Timestamp    uint64           `json:"timestamp"`
	Migrations   *shardMigrations `json:"migrations,omitempty"`
}

// NewKVStateMachine 创建一个新的KV状态机实例
func NewKVStateMachine(clusterID, nodeID uint64, db KVEngine) *KVStateMachine {
	return &KVStateMachine{
		clusterID:    clusterID,
		nodeID:       nodeID,
		db:           db,
		appliedIndex: 0,
	}
}

// Lookup 实现了statemachine.IStateMachine接口的Lookup方法
// 用于处理只读查询请求，在Leader节点上执行
func (s *KVStateMachine) Lookup(query interface{}) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := query.([]byte)
	if !ok {
		return nil, errors.New("query args must be []byte")
	}

	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}

	// 在Leader节点上直接从数据库读取数据, 这里的调用最终会到达Leader节点的本地数据库
	return s.db.Get(cmd.Key)
}

// AppliedIndex 返回最后处理的应用日志索引。
func (s *KVStateMachine) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedIndex
}

func (s *KVStateMachine) Update(entry statemachine.Entry) (statemachine.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.Index <= s.appliedIndex {
		return statemachine.Result{}, nil
	}
	reject := func(err error) (statemachine.Result, error) {
		s.appliedIndex = entry.Index
		return rejectedRaftResult(err), nil
	}
	var write raftWriteCommand
	if err := json.Unmarshal(entry.Cmd, &write); err != nil {
		return reject(ErrInvalidRaftCommand)
	}
	var processedCount uint64
	switch write.Op {
	case OpPut, OpDelete:
		if err := validateRaftWrite(write.Command); err != nil {
			return reject(err)
		}
		if write.Op == OpPut {
			if err := s.db.Put(write.Key, write.Value); err != nil {
				return statemachine.Result{}, fmt.Errorf("put failed: %w", err)
			}
		} else {
			if err := s.db.Delete(write.Key); err != nil {
				return statemachine.Result{}, fmt.Errorf("delete failed: %w", err)
			}
		}
		processedCount = 1
	case OpBatch:
		for _, cmd := range write.Commands {
			if err := validateRaftWrite(cmd); err != nil {
				return reject(err)
			}
		}
		batch := s.db.NewBatch(DefaultBatchOptions)
		defer batch.Close()
		for _, cmd := range write.Commands {
			if cmd.Op == OpPut {
				if err := batch.Put(cmd.Key, cmd.Value); err != nil {
					return statemachine.Result{}, fmt.Errorf("batch put failed: %w", err)
				}
			} else {
				if err := batch.Delete(cmd.Key); err != nil {
					return statemachine.Result{}, fmt.Errorf("batch delete failed: %w", err)
				}
			}
		}
		if err := batch.Commit(); err != nil {
			return statemachine.Result{}, fmt.Errorf("batch commit failed: %w", err)
		}
		processedCount = uint64(len(write.Commands))
	case opShardMigration:
		if write.Migration == nil || !write.Migration.valid(1) {
			return reject(ErrInvalidRaftCommand)
		}
		result, _ := s.migrations.apply(*write.Migration)
		s.appliedIndex = entry.Index
		return result, nil
	default:
		return reject(ErrInvalidRaftCommand)
	}
	s.appliedIndex = entry.Index
	return statemachine.Result{Value: processedCount}, nil
}

// SaveSnapshot 实现了statemachine.IStateMachine接口的SaveSnapshot方法
// 用于创建状态机的快照
func (s *KVStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	options := s.db.GetOptions()

	// Use KnociDB's snapshot feature to export database state
	// ExportSnapshot will automatically close the database
	if err := s.db.ExportSnapshot(); err != nil {
		// Export failed, try to reopen the database
		db, openErr := Open(options)
		if openErr != nil {
			return fmt.Errorf("reopen database failed : %v, export error: %w", openErr, err)
		}
		s.db = db
		return fmt.Errorf("database export snapshot failed: %w", err)
	}

	// Reopen the database after successful export
	db, err := Open(options)
	if err != nil {
		return fmt.Errorf("reopen database failed after export snapshot : %w", err)
	}
	s.db = db

	// Write appliedIndex and other metadata to snapshot
	metadata := kvSnapshotMetadata{
		AppliedIndex: s.appliedIndex,
		ClusterID:    s.clusterID,
		NodeID:       s.nodeID,
		Timestamp:    uint64(time.Now().Unix()),
	}
	if !s.migrations.empty() {
		metadata.Migrations = &s.migrations
	}

	// Serialize metadata
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot metadata: %w", err)
	}

	// Calculate checksum
	checksum := crc32.ChecksumIEEE(data)

	// Write metadata length
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("write matadata length failed: %w", err)
	}

	// Write metadata
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write metadata failed: %w", err)
	}

	// Write checksum
	checksumBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBuf, checksum)
	if _, err := w.Write(checksumBuf); err != nil {
		return fmt.Errorf("write checksum failed: %w", err)
	}

	// 把导出的 .ZST 文件内容也写入快照流，使快照自包含、可跨节点传输。
	snapPath, err := latestSnapshotFile(options.SnapshotPath)
	if err != nil {
		return fmt.Errorf("find exported snapshot failed: %w", err)
	}
	file, err := os.Open(snapPath)
	if err != nil {
		return fmt.Errorf("open exported snapshot failed: %w", err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat exported snapshot failed: %w", err)
	}
	sizeBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(sizeBuf, uint64(stat.Size()))
	if _, err := w.Write(sizeBuf); err != nil {
		return fmt.Errorf("write snapshot data size failed: %w", err)
	}
	if _, err := io.Copy(w, file); err != nil {
		return fmt.Errorf("write snapshot data failed: %w", err)
	}

	return nil
}

// RecoverFromSnapshot 实现了statemachine.IStateMachine接口的RecoverFromSnapshot方法
// 用于从快照恢复状态机
func (s *KVStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 读取元数据长度
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return fmt.Errorf("read metadata length failed: %w", err)
	}
	metadataLen := binary.LittleEndian.Uint32(lenBuf)

	// 读取元数据
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(r, metadataBytes); err != nil {
		return fmt.Errorf("read metadata failed: %w", err)
	}

	// 读取校验和
	checksumBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, checksumBuf); err != nil {
		return fmt.Errorf("read checksum failed: %w", err)
	}
	expectedChecksum := binary.LittleEndian.Uint32(checksumBuf)

	// 验证校验和
	actualChecksum := crc32.ChecksumIEEE(metadataBytes)
	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum invalid: expected %d, actual %d", expectedChecksum, actualChecksum)
	}

	// 解析元数据
	var metadata kvSnapshotMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return fmt.Errorf("unmarshal matadata failed: %w", err)
	}

	// 保存当前数据库选项，用于后续重新打开数据库
	options := s.db.GetOptions()

	snapDir := options.SnapshotPath
	if snapDir == "" {
		return fmt.Errorf("snapshotpath empty")
	}
	if err := os.MkdirAll(snapDir, os.ModePerm); err != nil {
		return fmt.Errorf("create snapshot dir failed: %w", err)
	}

	// 新格式：元数据/CRC 之后是 int64 文件大小 + .ZST 文件内容。
	// 兼容旧格式：没有文件内容时，退回本地最新 .ZST。
	var err error
	var importPath string
	var dataSize int64
	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(r, sizeBuf); err == nil {
		dataSize = int64(binary.LittleEndian.Uint64(sizeBuf))
	} else if err != io.EOF {
		return fmt.Errorf("read snapshot data size failed: %w", err)
	}

	if dataSize > 0 {
		tmp, err := os.CreateTemp(snapDir, "recover-*.ZST")
		if err != nil {
			return fmt.Errorf("create recover snapshot failed: %w", err)
		}
		tmpPath := tmp.Name()
		if _, err := io.CopyN(tmp, r, dataSize); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
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

	// 导入在快照校验通过后才关闭数据库。
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

	// 导入成功后重新打开数据库
	db, err := Open(options)
	if err != nil {
		return fmt.Errorf("reopen database failed after import snapshot: %w", err)
	}
	s.db = db

	// 恢复appliedIndex
	s.appliedIndex = metadata.AppliedIndex
	s.migrations = shardMigrations{}
	if metadata.Migrations != nil {
		s.migrations = *metadata.Migrations
	}

	return nil
}

// Close 实现了statemachine.IStateMachine接口的Close方法
func (s *KVStateMachine) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// GetHash 实现了statemachine.IStateMachine接口的GetHash方法
func (s *KVStateMachine) GetHash() (uint64, error) {
	// 简化实现，返回appliedIndex作为哈希值
	return s.appliedIndex, nil
}
