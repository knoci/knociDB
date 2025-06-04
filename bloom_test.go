package knocidb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
)

// TestNewBloomFilter 测试创建新的布隆过滤器
func TestNewBloomFilter(t *testing.T) {
	tempDir := t.TempDir()

	// 测试默认参数
	options := BloomFilterOptions{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 1)
	if bf == nil {
		t.Fatal("NewBloomFilter returned nil")
	}

	if bf.Size() != 0 {
		t.Errorf("Expected size 0, got %d", bf.Size())
	}

	if bf.IsEmpty() != true {
		t.Error("Expected bloom filter to be empty")
	}

	if bf.GetCapacity() == 0 {
		t.Error("Expected capacity > 0")
	}

	if bf.GetHashFunctions() == 0 {
		t.Error("Expected hash functions > 0")
	}

	// 测试零值参数的默认处理
	optionsZero := BloomFilterOptions{
		DirPath:      tempDir,
		PartitionNum: 2,
	}

	bf2 := NewBloomFilter(optionsZero, 2)
	if bf2.GetCapacity() == 0 {
		t.Error("Expected default capacity > 0")
	}
}

// TestBloomFilterAddAndTest 测试添加和查询功能
func TestBloomFilterAddAndTest(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 3)

	// 测试添加和查询
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	// 添加键
	for _, key := range keys {
		bf.Add(key)
	}

	if bf.Size() != uint32(len(keys)) {
		t.Errorf("Expected size %d, got %d", len(keys), bf.Size())
	}

	if bf.IsEmpty() {
		t.Error("Expected bloom filter to not be empty")
	}

	// 测试存在的键
	for _, key := range keys {
		if !bf.Test(key) {
			t.Errorf("Expected key %s to exist", string(key))
		}
	}

	// 测试不存在的键（可能有假阳性）
	nonExistentKey := []byte("nonexistent")
	result := bf.Test(nonExistentKey)
	// 注意：这里不能断言结果为false，因为布隆过滤器可能有假阳性
	_ = result
}

// TestBloomFilterStringMethods 测试字符串方法
func TestBloomFilterStringMethods(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 4)

	// 测试字符串方法
	keys := []string{"hello", "world", "test"}

	for _, key := range keys {
		bf.AddString(key)
	}

	for _, key := range keys {
		if !bf.TestString(key) {
			t.Errorf("Expected string key %s to exist", key)
		}
	}
}

// TestBloomFilterBatchOperations 测试批量操作
func TestBloomFilterBatchOperations(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 5)

	// 测试批量添加
	keys := [][]byte{
		[]byte("batch1"),
		[]byte("batch2"),
		[]byte("batch3"),
		[]byte("batch4"),
	}

	bf.AddMultiple(keys)

	if bf.Size() != uint32(len(keys)) {
		t.Errorf("Expected size %d, got %d", len(keys), bf.Size())
	}

	// 测试批量查询
	results := bf.TestMultiple(keys)
	if len(results) != len(keys) {
		t.Errorf("Expected %d results, got %d", len(keys), len(results))
	}

	for i, result := range results {
		if !result {
			t.Errorf("Expected key %s to exist", string(keys[i]))
		}
	}

	// 测试混合键的批量查询
	mixedKeys := [][]byte{
		[]byte("batch1"),       // 存在
		[]byte("nonexistent1"), // 不存在
		[]byte("batch2"),       // 存在
		[]byte("nonexistent2"), // 不存在
	}

	mixedResults := bf.TestMultiple(mixedKeys)
	if len(mixedResults) != len(mixedKeys) {
		t.Errorf("Expected %d results, got %d", len(mixedKeys), len(mixedResults))
	}

	// 验证存在的键
	if !mixedResults[0] || !mixedResults[2] {
		t.Error("Expected existing keys to return true")
	}
}

// TestBloomFilterSaveAndLoad 测试保存和加载功能
func TestBloomFilterSaveAndLoad(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	// 创建并填充布隆过滤器
	bf1 := NewBloomFilter(options, 6)
	keys := [][]byte{
		[]byte("save1"),
		[]byte("save2"),
		[]byte("save3"),
	}

	for _, key := range keys {
		bf1.Add(key)
	}

	// 保存到磁盘
	if err := bf1.Save(); err != nil {
		t.Fatalf("Failed to save bloom filter: %v", err)
	}

	// 从磁盘加载
	bf2, err := LoadBloomFilter(tempDir, 6)
	if err != nil {
		t.Fatalf("Failed to load bloom filter: %v", err)
	}

	if bf2 == nil {
		t.Fatal("LoadBloomFilter returned nil")
	}

	// 验证加载的数据
	if bf2.Size() != bf1.Size() {
		t.Errorf("Expected size %d, got %d", bf1.Size(), bf2.Size())
	}

	for _, key := range keys {
		if !bf2.Test(key) {
			t.Errorf("Expected key %s to exist in loaded filter", string(key))
		}
	}

	// 测试加载不存在的文件
	bf3, err := LoadBloomFilter(tempDir, 999)
	if err != nil {
		t.Errorf("Expected no error for non-existent file, got: %v", err)
	}
	if bf3 != nil {
		t.Error("Expected nil for non-existent file")
	}
}

// TestBloomFilterClear 测试清空功能
func TestBloomFilterClear(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 7)

	// 添加一些键
	keys := [][]byte{
		[]byte("clear1"),
		[]byte("clear2"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	if bf.Size() == 0 {
		t.Error("Expected size > 0 before clear")
	}

	// 清空
	bf.Clear()

	if bf.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", bf.Size())
	}

	if !bf.IsEmpty() {
		t.Error("Expected bloom filter to be empty after clear")
	}
}

// TestBloomFilterMerge 测试合并功能
func TestBloomFilterMerge(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      2,
		keyHashFunction:   xxhash.Sum64,
	}

	bf1 := NewBloomFilter(options, 8)
	bf2 := NewBloomFilter(options, 9)

	// 向两个过滤器添加不同的键
	keys1 := [][]byte{[]byte("merge1"), []byte("merge2")}
	keys2 := [][]byte{[]byte("merge3"), []byte("merge4")}

	for _, key := range keys1 {
		bf1.Add(key)
	}

	for _, key := range keys2 {
		bf2.Add(key)
	}

	originalSize1 := bf1.Size()
	originalSize2 := bf2.Size()

	// 合并
	if err := bf1.Merge(bf2); err != nil {
		t.Fatalf("Failed to merge bloom filters: %v", err)
	}

	// 验证合并后的大小
	expectedSize := originalSize1 + originalSize2
	if bf1.Size() != expectedSize {
		t.Errorf("Expected size %d after merge, got %d", expectedSize, bf1.Size())
	}

	// 验证所有键都存在
	allKeys := append(keys1, keys2...)
	for _, key := range allKeys {
		if !bf1.Test(key) {
			t.Errorf("Expected key %s to exist after merge", string(key))
		}
	}
}

// TestBloomFilterStats 测试统计信息
func TestBloomFilterStats(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 10)

	// 获取初始统计信息
	stats := bf.GetStats()

	// 验证统计信息字段
	requiredFields := []string{"size", "created_at", "updated_at", "path", "capacity", "hash_functions", "false_positive_rate"}
	for _, field := range requiredFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Expected stats to contain field %s", field)
		}
	}

	// 验证初始值
	if stats["size"].(uint32) != 0 {
		t.Error("Expected initial size to be 0")
	}

	if stats["capacity"].(uint) == 0 {
		t.Error("Expected capacity > 0")
	}

	if stats["hash_functions"].(uint) == 0 {
		t.Error("Expected hash_functions > 0")
	}

	// 添加一些键并重新检查统计信息
	bf.Add([]byte("stats_test"))
	stats = bf.GetStats()

	if stats["size"].(uint32) != 1 {
		t.Error("Expected size to be 1 after adding one key")
	}
}

// TestBloomFilterTimeTracking 测试时间跟踪
func TestBloomFilterTimeTracking(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 11)

	createdAt := bf.GetCreatedAt()
	updatedAt := bf.GetUpdatedAt()

	// 创建时间和更新时间应该相同
	if !createdAt.Equal(updatedAt) {
		t.Error("Expected created_at and updated_at to be equal initially")
	}

	// 等待一小段时间
	time.Sleep(10 * time.Millisecond)

	// 添加键应该更新 updated_at
	bf.Add([]byte("time_test"))
	newUpdatedAt := bf.GetUpdatedAt()

	if !newUpdatedAt.After(updatedAt) {
		t.Error("Expected updated_at to be updated after adding key")
	}

	// 创建时间应该保持不变
	if !bf.GetCreatedAt().Equal(createdAt) {
		t.Error("Expected created_at to remain unchanged")
	}
}

// TestBloomFilterRemove 测试删除文件
func TestBloomFilterRemove(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 12)
	bf.Add([]byte("remove_test"))

	// 保存文件
	if err := bf.Save(); err != nil {
		t.Fatalf("Failed to save bloom filter: %v", err)
	}

	// 验证文件存在
	if _, err := os.Stat(bf.GetPath()); os.IsNotExist(err) {
		t.Error("Expected file to exist after save")
	}

	// 删除文件
	if err := bf.Remove(); err != nil {
		t.Fatalf("Failed to remove bloom filter file: %v", err)
	}

	// 验证文件不存在
	if _, err := os.Stat(bf.GetPath()); !os.IsNotExist(err) {
		t.Error("Expected file to not exist after remove")
	}

	// 再次删除不应该出错
	if err := bf.Remove(); err != nil {
		t.Errorf("Expected no error when removing non-existent file, got: %v", err)
	}
}

// TestBloomFilterManager 测试布隆过滤器管理器
func TestBloomFilterManager(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      2,
		keyHashFunction:   xxhash.Sum64,
	}

	bfm := NewBloomFilterManager(options)

	// 测试获取过滤器
	bf1 := bfm.GetFilter(1)
	if bf1 == nil {
		t.Fatal("GetFilter returned nil")
	}

	// 再次获取相同分区应该返回同一个实例
	bf1_again := bfm.GetFilter(1)
	if bf1 != bf1_again {
		t.Error("Expected same instance for same partition")
	}

	// 获取不同分区应该返回不同实例
	bf2 := bfm.GetFilter(2)
	if bf1 == bf2 {
		t.Error("Expected different instances for different partitions")
	}

	// 测试添加和查询键
	key1 := []byte("manager_test1")
	key2 := []byte("manager_test2")

	bfm.AddKey(key1)
	bfm.AddKey(key2)

	if !bfm.TestKey(key1) {
		t.Error("Expected key1 to exist in partition 1")
	}

	if !bfm.TestKey(key2) {
		t.Error("Expected key2 to exist in partition 2")
	}

	// 测试保存所有
	if err := bfm.SaveAll(); err != nil {
		t.Fatalf("Failed to save all filters: %v", err)
	}

	// 测试清空所有
	bfm.ClearAll()
	if !bf1.IsEmpty() || !bf2.IsEmpty() {
		t.Error("Expected all filters to be empty after ClearAll")
	}

	// 重新添加数据用于测试删除
	bfm.AddKey(key1)
	bfm.AddKey(key2)
	bfm.SaveAll()

	// 测试删除所有
	if err := bfm.RemoveAll(); err != nil {
		t.Fatalf("Failed to remove all filters: %v", err)
	}
}

// TestBloomFilterFalsePositiveRate 测试假阳性率
func TestBloomFilterFalsePositiveRate(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01, // 1% 假阳性率
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 13)

	// 添加一些键
	addedKeys := make(map[string]bool)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		bf.AddString(key)
		addedKeys[key] = true
	}

	// 测试假阳性率
	falsePositives := 0
	totalTests := 1000

	for i := 0; i < totalTests; i++ {
		key := fmt.Sprintf("non_existent_key_%d", i)
		if _, exists := addedKeys[key]; !exists {
			if bf.TestString(key) {
				falsePositives++
			}
		}
	}

	actualFPRate := float64(falsePositives) / float64(totalTests)
	expectedFPRate := options.FalsePositiveRate

	// 允许一定的误差范围（实际假阳性率可能略有不同）
	if actualFPRate > expectedFPRate*2 {
		t.Errorf("Actual false positive rate %.4f is much higher than expected %.4f", actualFPRate, expectedFPRate)
	}

	t.Logf("Expected FP rate: %.4f, Actual FP rate: %.4f, False positives: %d/%d", expectedFPRate, actualFPRate, falsePositives, totalTests)
}

// TestBloomFilterConcurrency 测试并发安全性
func TestBloomFilterConcurrency(t *testing.T) {
	tempDir := t.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 14)

	// 并发添加和查询
	done := make(chan bool, 2)

	// 写入协程
	go func() {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("concurrent_key_%d", i)
			bf.AddString(key)
		}
		done <- true
	}()

	// 读取协程
	go func() {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("concurrent_key_%d", i)
			bf.TestString(key)
		}
		done <- true
	}()

	// 等待两个协程完成
	<-done
	<-done

	// 验证最终状态
	if bf.Size() != 100 {
		t.Errorf("Expected size 100, got %d", bf.Size())
	}
}

// BenchmarkBloomFilterAdd 基准测试：添加操作
func BenchmarkBloomFilterAdd(b *testing.B) {
	tempDir := b.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  uint(b.N),
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 15)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark_key_%d", i)
		bf.AddString(key)
	}
}

// BenchmarkBloomFilterTest 基准测试：查询操作
func BenchmarkBloomFilterTest(b *testing.B) {
	tempDir := b.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 16)

	// 预先添加一些键
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("benchmark_key_%d", i)
		bf.AddString(key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("benchmark_key_%d", i%1000)
		bf.TestString(key)
	}
}

// BenchmarkBloomFilterBatchAdd 基准测试：批量添加操作
func BenchmarkBloomFilterBatchAdd(b *testing.B) {
	tempDir := b.TempDir()

	options := BloomFilterOptions{
		ExpectedElements:  uint(b.N),
		FalsePositiveRate: 0.01,
		DirPath:           tempDir,
		PartitionNum:      1,
		keyHashFunction:   xxhash.Sum64,
	}

	bf := NewBloomFilter(options, 17)

	// 准备批量数据
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("batch_key_%d", i))
	}

	b.ResetTimer()
	bf.AddMultiple(keys)
}
