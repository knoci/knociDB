package knocidb

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaftSnapshotObjectStoreFailure(t *testing.T) {
	base := t.TempDir()
	fake := newFakeObjectStore()
	fake.failUpload = true

	config := createTestRaftConfig(1, 402, "localhost:10302", base)
	options := createTestDBOptions(t, base)
	options.ObjectTier = ObjectTierOptions{
		Enabled: true,
		Store:   fake,
		Prefix:  "raft-fail",
	}

	rdb, err := OpenRaft(config, options)
	require.NoError(t, err)
	defer rdb.Close()
	require.Eventually(t, func() bool { return rdb.IsLeader() }, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, rdb.Put([]byte("failure-key"), []byte("value")))
	err = rdb.stateMachine.SaveSnapshot(io.Discard, nil, nil)
	require.Error(t, err)
	// 快照上传失败后，状态机应重新打开 DB 并可继续服务。
	require.NoError(t, rdb.Put([]byte("after-failure"), []byte("ok")))
}
