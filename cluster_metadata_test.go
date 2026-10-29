package knocidb

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"
)

func TestShardMapStateMachine(t *testing.T) {
	sm := NewShardMapStateMachine(1, 2)
	require.Equal(t, ShardMapMeta{Epoch: 1, ShardCount: 2}, sm.Meta())

	raw, err := sm.Lookup(nil)
	require.NoError(t, err)
	var meta ShardMapMeta
	require.NoError(t, json.Unmarshal(raw.([]byte), &meta))
	require.Equal(t, ShardMapMeta{Epoch: 1, ShardCount: 2}, meta)

	cmd, err := json.Marshal(ShardMapMeta{Epoch: 2, ShardCount: 3})
	require.NoError(t, err)
	res, err := sm.Update(statemachine.Entry{Cmd: cmd})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res.Value)
	require.Equal(t, ShardMapMeta{Epoch: 2, ShardCount: 3}, sm.Meta())

	// Stale and invalid updates are rejected.
	stale, _ := json.Marshal(ShardMapMeta{Epoch: 1, ShardCount: 3})
	_, err = sm.Update(statemachine.Entry{Cmd: stale})
	require.Error(t, err)
	_, err = sm.Update(statemachine.Entry{Cmd: []byte("{bad")})
	require.Error(t, err)
	badCount, _ := json.Marshal(ShardMapMeta{Epoch: 3, ShardCount: 0})
	_, err = sm.Update(statemachine.Entry{Cmd: badCount})
	require.Error(t, err)

	var buf bytes.Buffer
	require.NoError(t, sm.SaveSnapshot(&buf, nil, make(chan struct{})))
	restored := NewShardMapStateMachine(1, 1)
	require.NoError(t, restored.RecoverFromSnapshot(&buf, nil, make(chan struct{})))
	require.Equal(t, ShardMapMeta{Epoch: 2, ShardCount: 3}, restored.Meta())

	closed := make(chan struct{})
	close(closed)
	require.ErrorIs(t, sm.SaveSnapshot(&buf, nil, closed), statemachine.ErrSnapshotStopped)
	require.ErrorIs(t, restored.RecoverFromSnapshot(&buf, nil, closed), statemachine.ErrSnapshotStopped)

	hash, err := sm.GetHash()
	require.NoError(t, err)
	require.NotZero(t, hash)
	require.NoError(t, sm.Close())
}
