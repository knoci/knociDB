package knocidb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/lni/dragonboat/v4/statemachine"
)

// ShardMapStateMachine 保存固定分片元数据（分片数与 epoch）。
// 它由 metadata Raft group 复制；当前版本分片表不可变，更新只推进 epoch。
type ShardMapStateMachine struct {
	mu         sync.RWMutex
	epoch      uint64
	shardCount int
}

// ShardMapMeta 是 ShardMapStateMachine 使用的 JSON 载荷。
type ShardMapMeta struct {
	Epoch      uint64 `json:"epoch"`
	ShardCount int    `json:"shard_count"`
}

func NewShardMapStateMachine(epoch uint64, shardCount int) *ShardMapStateMachine {
	if epoch == 0 {
		epoch = 1
	}
	return &ShardMapStateMachine{epoch: epoch, shardCount: shardCount}
}

func (s *ShardMapStateMachine) Lookup(query interface{}) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(ShardMapMeta{Epoch: s.epoch, ShardCount: s.shardCount})
}

func (s *ShardMapStateMachine) Update(entry statemachine.Entry) (statemachine.Result, error) {
	var meta ShardMapMeta
	if err := json.Unmarshal(entry.Cmd, &meta); err != nil {
		return statemachine.Result{}, err
	}
	if meta.ShardCount <= 0 {
		return statemachine.Result{}, fmt.Errorf("invalid shard count")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if meta.Epoch < s.epoch {
		return statemachine.Result{}, fmt.Errorf("stale shard map epoch")
	}
	s.epoch = meta.Epoch
	s.shardCount = meta.ShardCount
	return statemachine.Result{Value: s.epoch}, nil
}

func (s *ShardMapStateMachine) SaveSnapshot(w io.Writer, _ statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	select {
	case <-done:
		return statemachine.ErrSnapshotStopped
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], s.epoch)
	binary.BigEndian.PutUint64(buf[8:16], uint64(s.shardCount))
	_, err := w.Write(buf[:])
	return err
}

func (s *ShardMapStateMachine) RecoverFromSnapshot(r io.Reader, _ []statemachine.SnapshotFile, done <-chan struct{}) error {
	select {
	case <-done:
		return statemachine.ErrSnapshotStopped
	default:
	}
	var buf [16]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	epoch := binary.BigEndian.Uint64(buf[0:8])
	count := binary.BigEndian.Uint64(buf[8:16])
	if epoch == 0 || count == 0 {
		return fmt.Errorf("invalid shard map snapshot")
	}
	s.mu.Lock()
	s.epoch = epoch
	s.shardCount = int(count)
	s.mu.Unlock()
	return nil
}

func (s *ShardMapStateMachine) Close() error { return nil }

func (s *ShardMapStateMachine) GetHash() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], s.epoch)
	binary.BigEndian.PutUint64(buf[8:16], uint64(s.shardCount))
	return xxhash.Sum64(buf[:]), nil
}

func (s *ShardMapStateMachine) Meta() ShardMapMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return ShardMapMeta{Epoch: s.epoch, ShardCount: s.shardCount}
}
