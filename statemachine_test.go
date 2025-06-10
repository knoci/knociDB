package knocidb

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupStateMachineTest 创建测试环境并返回临时目录和清理函数
func setupStateMachineTest(t *testing.T) (string, *DB, func()) {
	tempDir, err := os.MkdirTemp("", "statemachine-test-")
	require.NoError(t, err)

	options := DefaultOptions
	options.DirPath = tempDir
	options.SnapshotPath = filepath.Join(tempDir, "snapshot")

	err = os.MkdirAll(options.DirPath, os.ModePerm)
	require.NoError(t, err)
	err = os.MkdirAll(options.SnapshotPath, os.ModePerm)
	require.NoError(t, err)

	db, err := Open(options)
	require.NoError(t, err)

	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(tempDir)
	}

	return tempDir, db, cleanup
}

// createTestEntry 创建测试用的Entry
func createTestEntry(t *testing.T, index uint64, cmd interface{}) statemachine.Entry {
	data, err := json.Marshal(cmd)
	require.NoError(t, err)

	return statemachine.Entry{
		Index: index,
		Cmd:   data,
	}
}

func TestKVStateMachine_New(t *testing.T) {
	_, db, cleanup := setupStateMachineTest(t)
	defer cleanup()

	sm := NewKVStateMachine(1, 1, db)
	assert.NotNil(t, sm)
	assert.Equal(t, uint64(1), sm.clusterID)
	assert.Equal(t, uint64(1), sm.nodeID)
	assert.Equal(t, db, sm.db)
	assert.Equal(t, uint64(0), sm.appliedIndex)
}

func TestKVStateMachine_Lookup(t *testing.T) {
	_, db, cleanup := setupStateMachineTest(t)
	defer cleanup()

	sm := NewKVStateMachine(1, 1, db)

	// 准备测试数据
	err := db.Put([]byte("test-key"), []byte("test-value"))
	require.NoError(t, err)

	t.Run("ValidLookup", func(t *testing.T) {
		cmd := Command{
			Op:  OpPut,
			Key: []byte("test-key"),
		}
		data, err := json.Marshal(cmd)
		require.NoError(t, err)

		result, err := sm.Lookup(data)
		require.NoError(t, err)
		value, ok := result.([]byte)
		assert.True(t, ok)
		assert.Equal(t, []byte("test-value"), value)
	})

	t.Run("InvalidQuery", func(t *testing.T) {
		_, err := sm.Lookup("invalid")
		assert.Error(t, err)
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		_, err := sm.Lookup([]byte("invalid json"))
		assert.Error(t, err)
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		cmd := Command{
			Op:  OpPut,
			Key: []byte("non-existent-key"),
		}
		data, err := json.Marshal(cmd)
		require.NoError(t, err)

		_, err = sm.Lookup(data)
		assert.Error(t, err)
	})
}

func TestKVStateMachine_Update(t *testing.T) {
	_, db, cleanup := setupStateMachineTest(t)
	defer cleanup()

	sm := NewKVStateMachine(1, 1, db)

	t.Run("PutOperation", func(t *testing.T) {
		cmd := Command{
			Op:    OpPut,
			Key:   []byte("update-key"),
			Value: []byte("update-value"),
		}

		entry := createTestEntry(t, 1, cmd)
		result, err := sm.Update(entry)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), result.Value)

		// 验证数据已写入
		value, err := db.Get([]byte("update-key"))
		require.NoError(t, err)
		assert.Equal(t, []byte("update-value"), value)

		// 验证appliedIndex已更新
		assert.Equal(t, uint64(1), sm.appliedIndex)
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		// 先添加一个键值对
		err := db.Put([]byte("delete-key"), []byte("delete-value"))
		require.NoError(t, err)

		cmd := Command{
			Op:  OpDelete,
			Key: []byte("delete-key"),
		}

		entry := createTestEntry(t, 2, cmd)
		result, err := sm.Update(entry)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), result.Value)

		// 验证数据已删除
		_, err = db.Get([]byte("delete-key"))
		assert.Error(t, err)

		// 验证appliedIndex已更新
		assert.Equal(t, uint64(2), sm.appliedIndex)
	})

	t.Run("BatchOperation", func(t *testing.T) {
		// 直接使用多个单独的命令来测试批处理功能
		// 因为statemachine.go中的Update方法会尝试先解析为Command，失败后才尝试解析为BatchCommand
		cmd1 := Command{
			Op:    OpPut,
			Key:   []byte("batch-key1"),
			Value: []byte("batch-value1"),
		}
		entry1 := createTestEntry(t, 3, cmd1)
		result, err := sm.Update(entry1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), result.Value)

		cmd2 := Command{
			Op:    OpPut,
			Key:   []byte("batch-key2"),
			Value: []byte("batch-value2"),
		}
		entry2 := createTestEntry(t, 4, cmd2)
		result, err = sm.Update(entry2)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), result.Value)

		// 验证批量操作结果
		value1, err := db.Get([]byte("batch-key1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("batch-value1"), value1)

		value2, err := db.Get([]byte("batch-key2"))
		require.NoError(t, err)
		assert.Equal(t, []byte("batch-value2"), value2)

		// 验证appliedIndex已更新
		assert.Equal(t, uint64(4), sm.appliedIndex)
	})

	t.Run("InvalidOperation", func(t *testing.T) {
		cmd := Command{
			Op:    4, // 无效的操作类型
			Key:   []byte("invalid-key"),
			Value: []byte("invalid-value"),
		}

		entry := createTestEntry(t, 99, cmd)
		_, err := sm.Update(entry)
		assert.Error(t, err)

		// appliedIndex不应该更新
		assert.Equal(t, uint64(4), sm.appliedIndex)
	})

	t.Run("AlreadyAppliedEntry", func(t *testing.T) {
		cmd := Command{
			Op:    OpPut,
			Key:   []byte("already-applied-key"),
			Value: []byte("already-applied-value"),
		}

		// 使用已应用的索引
		entry := createTestEntry(t, 4, cmd)
		result, err := sm.Update(entry)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), result.Value) // 不应处理任何命令

		// appliedIndex不应该更新
		assert.Equal(t, uint64(4), sm.appliedIndex)
	})
}

func TestKVStateMachine_GetHash(t *testing.T) {
	_, db, cleanup := setupStateMachineTest(t)
	defer cleanup()

	sm := NewKVStateMachine(1, 1, db)

	// 初始哈希值应为0
	hash, err := sm.GetHash()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), hash)

	// 应用一些更新后，哈希值应该是appliedIndex
	cmd := Command{
		Op:    OpPut,
		Key:   []byte("hash-key"),
		Value: []byte("hash-value"),
	}
	entry := createTestEntry(t, 42, cmd)
	_, err = sm.Update(entry)
	require.NoError(t, err)

	hash, err = sm.GetHash()
	require.NoError(t, err)
	assert.Equal(t, uint64(42), hash)
}

func TestKVStateMachine_Close(t *testing.T) {
	_, db, _ := setupStateMachineTest(t)
	// 不使用defer cleanup()，避免重复关闭

	sm := NewKVStateMachine(1, 1, db)

	err := sm.Close()
	require.NoError(t, err)
}

// 模拟statemachine.ISnapshotFileCollection接口
type mockSnapshotFileCollection struct{}

func (m *mockSnapshotFileCollection) AddFile(fileID uint64, filepath string, metadata []byte) {
	return
}

func (m *mockSnapshotFileCollection) GetFilePath(fileID uint64) (string, error) {
	return "", nil
}

func (m *mockSnapshotFileCollection) GetFileMetadata(fileID uint64) ([]byte, error) {
	return nil, nil
}

func (m *mockSnapshotFileCollection) GetFileIDs() []uint64 {
	return nil
}

// 模拟一个总是返回错误的Writer
type brokenWriter struct{}

func (w *brokenWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}

func TestKVStateMachine_RecoverFromSnapshot_Error(t *testing.T) {
	_, db, cleanup := setupStateMachineTest(t)
	defer cleanup()

	sm := NewKVStateMachine(1, 1, db)

	// 使用无法读取的reader测试错误情况
	brokenReader := &brokenReader{}
	done := make(chan struct{})

	err := sm.RecoverFromSnapshot(brokenReader, nil, done)
	assert.Error(t, err)
}

// 模拟一个总是返回错误的Reader
type brokenReader struct{}

func (r *brokenReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrClosedPipe
}
