package knocidb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "bloom-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建布隆过滤器
	options := BloomFilterOptions{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionID:       1,
	}

	filter := NewBloomFilter(options)
	assert.NotNil(t, filter)

	// 测试添加和检查
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, key := range keys {
		filter.Add(key)
	}

	// 测试存在的键
	for _, key := range keys {
		assert.True(t, filter.Test(key))
	}

	// 测试不存在的键
	notExistKeys := [][]byte{
		[]byte("notexist1"),
		[]byte("notexist2"),
	}

	falsePositives := 0
	for _, key := range notExistKeys {
		if filter.Test(key) {
			falsePositives++
		}
	}

	// 检查假阳性率是否在可接受范围内
	actualFPRate := float64(falsePositives) / float64(len(notExistKeys))
	assert.LessOrEqual(t, actualFPRate, options.FalsePositiveRate*2) // 允许一定误差

	// 测试保存和加载
	err = filter.Save()
	assert.NoError(t, err)

	// 加载已保存的过滤器
	loadedFilter, err := LoadBloomFilter(tempDir, options.PartitionID)
	assert.NoError(t, err)
	assert.NotNil(t, loadedFilter)

	// 验证加载的过滤器包含相同的键
	for _, key := range keys {
		assert.True(t, loadedFilter.Test(key))
	}
}

func TestBloomFilterManager(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "bloom-manager-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建布隆过滤器管理器
	options := BloomFilterOptions{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
	}

	manager := NewBloomFilterManager(options)
	assert.NotNil(t, manager)

	// 测试多个分区
	partitions := []int{0, 1, 2}
	keysPerPartition := 100

	// 向每个分区添加键
	for _, partition := range partitions {
		for i := 0; i < keysPerPartition; i++ {
			key := []byte(fmt.Sprintf("key-%d-%d", partition, i))
			manager.AddKey(partition, key)
		}
	}

	// 测试每个分区中的键
	for _, partition := range partitions {
		for i := 0; i < keysPerPartition; i++ {
			key := []byte(fmt.Sprintf("key-%d-%d", partition, i))
			assert.True(t, manager.TestKey(partition, key))
		}
	}

	// 测试不存在的键
	for _, partition := range partitions {
		key := []byte(fmt.Sprintf("notexist-%d", partition))
		assert.False(t, manager.TestKey(partition, key))
	}

	// 测试保存所有过滤器
	err = manager.SaveAll()
	assert.NoError(t, err)

	// 创建新的管理器并加载已保存的过滤器
	newManager := NewBloomFilterManager(options)
	for _, partition := range partitions {
		filter := newManager.GetFilter(partition)
		assert.NotNil(t, filter)

		// 验证已加载的过滤器包含相同的键
		for i := 0; i < keysPerPartition; i++ {
			key := []byte(fmt.Sprintf("key-%d-%d", partition, i))
			assert.True(t, filter.Test(key))
		}
	}
}

func TestBloomFilterConcurrency(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "bloom-concurrency-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建布隆过滤器
	options := BloomFilterOptions{
		ExpectedElements:  10000,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionID:       1,
	}

	filter := NewBloomFilter(options)
	assert.NotNil(t, filter)

	// 并发添加和测试
	var wg sync.WaitGroup
	workers := 10
	keysPerWorker := 1000

	// 并发添加
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < keysPerWorker; j++ {
				key := []byte(fmt.Sprintf("worker-%d-key-%d", workerID, j))
				filter.Add(key)
			}
		}(i)
	}

	wg.Wait()

	// 并发测试
	var testWg sync.WaitGroup
	for i := 0; i < workers; i++ {
		testWg.Add(1)
		go func(workerID int) {
			defer testWg.Done()
			for j := 0; j < keysPerWorker; j++ {
				key := []byte(fmt.Sprintf("worker-%d-key-%d", workerID, j))
				assert.True(t, filter.Test(key))
			}
		}(i)
	}

	testWg.Wait()
}

func TestBloomFilterEdgeCases(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "bloom-edge-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 测试空键
	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionID:       1,
	}

	filter := NewBloomFilter(options)
	filter.Add([]byte{})
	assert.True(t, filter.Test([]byte{}))

	// 测试大键
	bigKey := make([]byte, 1024*1024) // 1MB
	filter.Add(bigKey)
	assert.True(t, filter.Test(bigKey))

	// 测试默认假阳性率
	filter = NewBloomFilter(BloomFilterOptions{
		ExpectedElements: 100,
		DirPath:          tempDir,
		PartitionID:      2,
	})
	assert.Equal(t, defaultFalsePositiveRate, filter.EstimateFalsePositiveRate())

	// 测试文件不存在的情况
	nonExistentFilter, err := LoadBloomFilter(tempDir, 999)
	assert.Nil(t, err)
	assert.Nil(t, nonExistentFilter)

	// 测试文件损坏的情况
	corruptedPath := filepath.Join(tempDir, fmt.Sprintf("partition_%d%s", 3, bloomFileExt))
	err = os.WriteFile(corruptedPath, []byte("corrupted data"), 0644)
	assert.NoError(t, err)

	_, err = LoadBloomFilter(tempDir, 3)
	assert.Error(t, err)
}
