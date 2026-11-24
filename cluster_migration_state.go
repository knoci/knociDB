package knocidb

import (
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/statemachine"
)

const (
	opShardMigration uint32 = 10
	migrationPending uint64 = 1
	migrationDone    uint64 = 2
	migrationAborted uint64 = 3
)

type migrationChange struct {
	Move     ShardMove `json:"move"`
	CanStart bool      `json:"can_start,omitempty"`
	Complete bool      `json:"complete,omitempty"`
	Abort    bool      `json:"abort,omitempty"`
}

func (c migrationChange) valid(shardCount int) bool {
	move := c.Move
	flags := 0
	if c.CanStart {
		flags++
	}
	if c.Complete {
		flags++
	}
	if c.Abort {
		flags++
	}
	if flags > 1 {
		return false
	}
	if c.Abort {
		return shardCount > 0 && uint64(move.ShardID) < uint64(shardCount) && move.FromNode != 0 &&
			move.ToNode != 0 && move.FromNode != move.ToNode
	}
	return shardCount > 0 && uint64(move.ShardID) < uint64(shardCount) && move.FromNode != 0 &&
		move.ToNode != 0 && move.FromNode != move.ToNode && config.IsValidAddress(move.ToAddress)
}

// A removed Raft replica ID cannot be reused within the same shard.
// Keep its completed move so an ambiguous response can be retried.
type shardMigrations struct {
	Pending   map[uint32]ShardMove            `json:"pending,omitempty"`
	Completed map[uint32]map[uint64]ShardMove `json:"completed,omitempty"`
}

func (s *shardMigrations) empty() bool {
	return len(s.Pending) == 0 && len(s.Completed) == 0
}

func (s *shardMigrations) apply(change migrationChange) (statemachine.Result, bool) {
	move := change.Move
	if change.Abort {
		pending, ok := s.Pending[move.ShardID]
		if !ok {
			return statemachine.Result{Value: migrationAborted}, false
		}
		if pending.FromNode != move.FromNode || pending.ToNode != move.ToNode {
			return rejectedRaftResult(ErrShardMigrationConflict), false
		}
		delete(s.Pending, move.ShardID)
		return statemachine.Result{Value: migrationAborted}, false
	}
	if done, ok := s.Completed[move.ShardID][move.FromNode]; ok {
		if done != move {
			return rejectedRaftResult(ErrShardMigrationConflict), false
		}
		return statemachine.Result{Value: migrationDone}, false
	}
	pending, ok := s.Pending[move.ShardID]
	if ok && pending != move {
		return rejectedRaftResult(ErrShardMigrationConflict), false
	}
	if !ok {
		if !change.CanStart {
			return rejectedRaftResult(ErrShardMigrationConflict), false
		}
		if s.Pending == nil {
			s.Pending = make(map[uint32]ShardMove)
		}
		s.Pending[move.ShardID] = move
	}
	if !change.Complete {
		return statemachine.Result{Value: migrationPending}, false
	}
	if s.Completed == nil {
		s.Completed = make(map[uint32]map[uint64]ShardMove)
	}
	if s.Completed[move.ShardID] == nil {
		s.Completed[move.ShardID] = make(map[uint64]ShardMove)
	}
	s.Completed[move.ShardID][move.FromNode] = move
	delete(s.Pending, move.ShardID)
	return statemachine.Result{Value: migrationDone}, true
}
