package knocidb

import "fmt"

// ShardMove describes one replica migration produced by PlanRebalance.
type ShardMove struct {
	ShardID   uint32
	FromNode  uint64
	ToNode    uint64
	ToAddress string
}

// PlanRebalance returns at most one move that reduces the maximum voter
// imbalance across shards. It does not change membership.
func (c *ClusterDB) PlanRebalance() ([]ShardMove, error) {
	if c.config.ShardCount <= 0 {
		return nil, ErrNodeNotStarted
	}
	counts := make(map[uint64]int)
	addresses := make(map[uint64]string)
	for id, addr := range c.config.InitialMembers {
		addresses[id] = addr
	}
	memberships := make([]*ShardMembership, 0, c.config.ShardCount)
	for i := 0; i < c.config.ShardCount; i++ {
		m, err := c.ShardMembership(uint32(i))
		if err != nil {
			return nil, err
		}
		memberships = append(memberships, m)
		for _, id := range m.Voters {
			counts[id]++
			if addr := m.Addresses[id]; addr != "" {
				addresses[id] = addr
			}
		}
	}

	var best *ShardMove
	bestDiff := 0
	for _, m := range memberships {
		voters := make(map[uint64]struct{}, len(m.Voters))
		for _, id := range m.Voters {
			voters[id] = struct{}{}
		}
		for _, from := range m.Voters {
			for to, addr := range addresses {
				if to == from || addr == "" {
					continue
				}
				if _, ok := voters[to]; ok {
					continue
				}
				if _, removed := m.Removed[to]; removed {
					continue
				}
				diff := counts[from] - counts[to]
				if diff > bestDiff {
					move := ShardMove{ShardID: m.ShardID, FromNode: from, ToNode: to, ToAddress: addr}
					best = &move
					bestDiff = diff
				}
			}
		}
	}
	if best == nil {
		return nil, nil
	}
	return []ShardMove{*best}, nil
}

// RebalanceAll executes up to maxMoves planned moves. It stops when no move
// remains or a move fails. A non-positive limit defaults to 8.
func (c *ClusterDB) RebalanceAll(maxMoves int) ([]ShardMove, error) {
	if maxMoves <= 0 {
		maxMoves = 8
	}
	var applied []ShardMove
	for i := 0; i < maxMoves; i++ {
		moves, err := c.PlanRebalance()
		if err != nil {
			return applied, err
		}
		if len(moves) == 0 {
			return applied, nil
		}
		if err := c.ExecuteShardMove(moves[0]); err != nil {
			return applied, err
		}
		applied = append(applied, moves[0])
	}
	return applied, nil
}

// Rebalance executes one planned move on the current node. The caller must be
// the leader of the selected shard; repeated calls can be used to converge.
// ExecuteShardMove runs a previously planned move on this node.
func (c *ClusterDB) ExecuteShardMove(move ShardMove) error {
	leader, err := c.leaderForShard(move.ShardID)
	if err != nil {
		return err
	}
	if leader != c.config.NodeID {
		return fmt.Errorf("shard %d leader is node %d, not local node", move.ShardID, leader)
	}
	return c.MigrateShard(move.ShardID, move.FromNode, move.ToNode, move.ToAddress)
}

func (c *ClusterDB) Rebalance() ([]ShardMove, error) {
	moves, err := c.PlanRebalance()
	if err != nil {
		return nil, err
	}
	if len(moves) == 0 {
		return nil, nil
	}
	move := moves[0]
	if err := c.ExecuteShardMove(move); err != nil {
		return nil, err
	}
	return moves, nil
}
