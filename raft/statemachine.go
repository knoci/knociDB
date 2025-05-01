package raft

import (
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/lni/dragonboat/v4/statemachine"
	"knocidb/wal"
)

// 操作类型常量
const (
	OpPut    uint32 = 1
	OpDelete uint32 = 2
	OpBatch  uint32 = 3
)

// Command 表示要通过Raft共识执行的命令
type Command struct {
	Op    uint32 `json:"op"`    // 操作类型：1=Put, 2=Delete, 3=Batch
	Key   []byte `json:"key"`   // 键
	Value []byte `json:"value"` // 值
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
	clusterID   uint64
	nodeID      uint64
	db          DBOperator // 数据库操作接口
	appliedIndex uint64
	mu          sync.RWMutex
}

// DBOperator 定义了状态机需要的数据库操作接口
type DBOperator interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	BatchWrite(commands []Command, batchID uint64) error
}

// NewKVStateMachine 创建一个新的KV状态机实例
func NewKVStateMachine(clusterID, nodeID uint64, db DBOperator) *KVStateMachine {
	return &KVStateMachine{
		clusterID:   clusterID,
		nodeID:      nodeID,
		db:          db,
		appliedIndex: 0,
	}
}

// Lookup 实现了statemachine.IStateMachine接口的Lookup方法
// 用于处理只读查询请求
func (s *KVStateMachine) Lookup(query interface{}) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := query.([]byte)
	if !ok {
		return nil, errors.New("查询参数必须是字节切片")
	}

	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}

	// 只支持Get操作
	return s.db.Get(cmd.Key)
}

// Update 实现了statemachine.IStateMachine接口的Update方法
// 用于应用Raft日志条目到状态机
func (s *KVStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]statemachine.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Index <= s.appliedIndex {
			panic("不应用已经应用过的条目")
		}

		var cmd Command
		if err := json.Unmarshal(entry.Cmd, &cmd); err != nil {
			// 尝试解析为批处理命令
			var batchCmd BatchCommand
			if err := json.Unmarshal(entry.Cmd, &batchCmd); err != nil {
				return nil, err
			}
			
			// 执行批处理命令
			if err := s.db.BatchWrite(batchCmd.Commands, batchCmd.BatchID); err != nil {
				return nil, err
			}
		} else {
			// 执行单个命令
			switch cmd.Op {
			case OpPut:
				if err := s.db.Put(cmd.Key, cmd.Value); err != nil {
					return nil, err
				}
			case OpDelete:
				if err := s.db.Delete(cmd.Key); err != nil {
					return nil, err
				}
			default:
				return nil, errors.New("未知的操作类型")
			}
		}

		// 更新已应用的索引
		s.appliedIndex = entry.Index
		result = append(result, entry)
	}

	return result, nil
}

// SaveSnapshot 实现了statemachine.IStateMachine接口的SaveSnapshot方法
// 用于创建状态机的快照
func (s *KVStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 在实际实现中，这里应该将数据库的当前状态序列化到快照中
	// 由于我们使用的是外部数据库，可以考虑创建数据库的一致性快照
	// 这里简化处理，只保存appliedIndex
	data, err := json.Marshal(map[string]uint64{"applied_index": s.appliedIndex})
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// RecoverFromSnapshot 实现了statemachine.IStateMachine接口的RecoverFromSnapshot方法
// 用于从快照恢复状态机
func (s *KVStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 读取快照数据
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	// 解析快照数据
	var snapshot map[string]uint64
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	// 恢复appliedIndex
	if index, ok := snapshot["applied_index"]; ok {
		s.appliedIndex = index
	}

	return nil
}

// Close 实现了statemachine.IStateMachine接口的Close方法
func (s *KVStateMachine) Close() error {
	// 这里不需要关闭数据库，因为数据库的生命周期由外部管理
	return nil
}

// GetHash 实现了statemachine.IStateMachine接口的GetHash方法
func (s *KVStateMachine) GetHash() (uint64, error) {
	// 简化实现，返回appliedIndex作为哈希值
	return s.appliedIndex, nil
}