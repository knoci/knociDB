package knocidb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// KVStateMachine 实现了Dragonboat的statemachine.IStateMachine接口
// 它将数据库操作与Raft共识机制集成
type KVStateMachine struct {
	clusterID    uint64
	nodeID       uint64
	db           *DB // 数据库
	appliedIndex uint64
	mu           sync.RWMutex
}

// NewKVStateMachine 创建一个新的KV状态机实例
func NewKVStateMachine(clusterID, nodeID uint64, db *DB) *KVStateMachine {
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

// Update 实现了statemachine.IStateMachine接口的Update方法
// 用于应用Raft日志条目到状态机
func (s *KVStateMachine) Update(entry statemachine.Entry) (statemachine.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var processedCount uint64 = 0 // 统计处理的命令数量

	// 检查是否已经应用过该条目
	if entry.Index <= s.appliedIndex {
		return statemachine.Result{Value: processedCount}, nil
	}

	var cmd Command
	if err := json.Unmarshal(entry.Cmd, &cmd); err != nil {
		// 尝试解析为批处理命令
		var batchCmd BatchCommand
		if err := json.Unmarshal(entry.Cmd, &batchCmd); err != nil {
			return statemachine.Result{}, fmt.Errorf("failed to unmarshal command: %v", err)
		}

		batch := s.db.NewBatch(DefaultBatchOptions)
		for _, command := range batchCmd.Commands {
			switch command.Op {
			case OpPut:
				if err := batch.Put(command.Key, command.Value); err != nil {
					return statemachine.Result{}, fmt.Errorf("batch put failed: %v", err)
				}
			case OpDelete:
				if err := batch.Delete(command.Key); err != nil {
					return statemachine.Result{}, fmt.Errorf("batch delete failed: %v", err)
				}
			default:
				return statemachine.Result{}, fmt.Errorf("unknown batch operation: %v", command.Op)
			}
			processedCount++
		}
		if err := batch.Commit(); err != nil {
			return statemachine.Result{}, fmt.Errorf("batch commit failed: %v", err)
		}
	} else {
		// 处理单个命令
		switch cmd.Op {
		case OpPut:
			if err := s.db.Put(cmd.Key, cmd.Value); err != nil {
				return statemachine.Result{}, fmt.Errorf("put failed: %v", err)
			}
		case OpDelete:
			if err := s.db.Delete(cmd.Key); err != nil {
				return statemachine.Result{}, fmt.Errorf("delete failed: %v", err)
			}
		default:
			return statemachine.Result{}, fmt.Errorf("unknown operation: %v", cmd.Op)
		}
		processedCount++
	}

	// 更新已应用的索引
	s.appliedIndex = entry.Index
	return statemachine.Result{Value: processedCount}, nil
}

// SaveSnapshot 实现了statemachine.IStateMachine接口的SaveSnapshot方法
// 用于创建状态机的快照
func (s *KVStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	options := s.db.GetOptions()

	// 使用KnociDB的快照功能导出数据库状态
	// ExportSnapshot会自动关闭数据库
	if err := s.db.ExportSnapshot(); err != nil {
		// 导出失败，尝试重新打开数据库
		db, openErr := Open(options)
		if openErr != nil {
			return fmt.Errorf("reopen database failed : %v, export error: %w", openErr, err)
		}
		s.db = db
		return fmt.Errorf("database export snashot failed: %w", err)
	}

	// 导出成功后重新打开数据库
	db, err := Open(options)
	if err != nil {
		return fmt.Errorf("reopen database failed after export snapshot : %w", err)
	}
	s.db = db

	// 将appliedIndex和其他元数据写入快照
	metadata := map[string]uint64{
		"applied_index": s.appliedIndex,
		"cluster_id":    s.clusterID,
		"node_id":       s.nodeID,
		"timestamp":     uint64(time.Now().Unix()),
	}

	// 序列化元数据
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("序列化快照元数据失败: %w", err)
	}

	// 计算校验和
	checksum := crc32.ChecksumIEEE(data)

	// 写入元数据长度
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("write matadata length failed: %w", err)
	}

	// 写入元数据
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write matadate failed: %w", err)
	}

	// 写入校验和
	checksumBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBuf, checksum)
	if _, err := w.Write(checksumBuf); err != nil {
		return fmt.Errorf("write checksum faied: %w", err)
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
		return fmt.Errorf("read matadata length failed: %w", err)
	}
	metadataLen := binary.LittleEndian.Uint32(lenBuf)

	// 读取元数据
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(r, metadataBytes); err != nil {
		return fmt.Errorf("read matadate failed: %w", err)
	}

	// 读取校验和
	checksumBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, checksumBuf); err != nil {
		return fmt.Errorf("read checksum faied: %w", err)
	}
	expectedChecksum := binary.LittleEndian.Uint32(checksumBuf)

	// 验证校验和
	actualChecksum := crc32.ChecksumIEEE(metadataBytes)
	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum invalid: expected %d, actual %d", expectedChecksum, actualChecksum)
	}

	// 解析元数据
	var metadata map[string]uint64
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return fmt.Errorf("unmarshal matadata failed: %w", err)
	}

	// 保存当前数据库选项，用于后续重新打开数据库
	options := s.db.GetOptions()

	// 获取最新的快照文件路径
	snapDir := options.SnapshotPath
	if snapDir == "" {
		return fmt.Errorf("snapshotpath empty")
	}

	file, err := os.ReadDir(snapDir)
	if err != nil {
		return fmt.Errorf(": %w", err)
	}

	var snapFiles []string
	for _, f := range file {
		if strings.HasSuffix(f.Name(), snapshotExt) {
			snapFiles = append(snapFiles, filepath.Join(snapDir, f.Name()))
		}
	}

	if len(snapFiles) == 0 {
		return fmt.Errorf("can not find snapshot file")
	}

	// 按文件名排序（文件名包含时间戳）
	sort.Strings(snapFiles)
	// 获取最新的快照文件
	latestSnapshot := snapFiles[len(snapFiles)-1]

	// 使用KnociDB的导入功能恢复数据库状态 ImportSnapshot会自动关闭数据库
	if err := s.db.ImportSnapshot(latestSnapshot); err != nil {
		// 导入失败，尝试重新打开数据库
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
	if index, ok := metadata["applied_index"]; ok {
		s.appliedIndex = index
	}

	return nil
}

// Close 实现了statemachine.IStateMachine接口的Close方法
func (s *KVStateMachine) Close() error {
	return s.db.Close()
}

// GetHash 实现了statemachine.IStateMachine接口的GetHash方法
func (s *KVStateMachine) GetHash() (uint64, error) {
	// 简化实现，返回appliedIndex作为哈希值
	return s.appliedIndex, nil
}
