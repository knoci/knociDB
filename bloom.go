package knocidb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

const (
	// 布隆过滤器文件扩展名
	bloomFileExt = ".BLOOM"
	// 默认假阳性率
	defaultFalsePositiveRate = 0.01
	// 布隆过滤器文件头大小
	bloomHeaderSize = 16
)

// BloomFilter 包装了第三方布隆过滤器库，提供键存在性的快速检查
// 用于在查找索引之前快速过滤不存在的键，减少磁盘I/O
type BloomFilter struct {
	mu        sync.RWMutex
	filter    *bloom.BloomFilter
	path      string
	size      uint32    // 当前过滤器中的元素数量
	createdAt time.Time // 创建时间
	updatedAt time.Time // 最后更新时间
}

// BloomFilterOptions 布隆过滤器配置选项
type BloomFilterOptions struct {
	// 预期元素数量
	ExpectedElements uint
	// 假阳性率 (0.0 到 1.0)
	FalsePositiveRate float64
	// 存储路径
	DirPath string
	// 分区数量
	PartitionNum int
	// 哈希分片函数
	keyHashFunction func([]byte) uint64
}

// NewBloomFilter 创建一个新的布隆过滤器
func NewBloomFilter(options BloomFilterOptions, partitionID int) *BloomFilter {
	if options.FalsePositiveRate <= 0 {
		options.FalsePositiveRate = defaultFalsePositiveRate
	}
	if options.ExpectedElements == 0 {
		options.ExpectedElements = 10000 // 默认预期元素数量
	}

	filter := bloom.NewWithEstimates(options.ExpectedElements, options.FalsePositiveRate)
	path := filepath.Join(options.DirPath, fmt.Sprintf("%d%s", partitionID, bloomFileExt))

	now := time.Now()
	return &BloomFilter{
		filter:    filter,
		path:      path,
		size:      0,
		createdAt: now,
		updatedAt: now,
	}
}

// LoadBloomFilter 从磁盘加载布隆过滤器
func LoadBloomFilter(dirPath string, partitionID int) (*BloomFilter, error) {
	path := filepath.Join(dirPath, fmt.Sprintf("%d%s", partitionID, bloomFileExt))

	// 检查文件是否存在
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil // 文件不存在，返回nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open bloom filter file: %w", err)
	}
	defer file.Close()

	// 读取头部信息
	header := make([]byte, bloomHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter header: %w", err)
	}

	size := binary.LittleEndian.Uint32(header[0:4])

	// 创建新的布隆过滤器并从文件读取
	filter := &bloom.BloomFilter{}
	if _, err := filter.ReadFrom(file); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter data: %w", err)
	}

	return &BloomFilter{
		filter: filter,
		path:   path,
		size:   size,
	}, nil
}

// Add 向布隆过滤器添加一个键
func (bf *BloomFilter) Add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	bf.filter.Add(key)
	bf.size++
	bf.updatedAt = time.Now()
}

// Test 检查键是否可能存在于集合中
// 返回 true 表示键可能存在（可能有假阳性）
// 返回 false 表示键绝对不存在
func (bf *BloomFilter) Test(key []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.filter == nil {
		return true // 如果没有过滤器，假设键存在
	}

	return bf.filter.Test(key)
}

// Size 返回过滤器中的元素数量
func (bf *BloomFilter) Size() uint32 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.size
}

// Save 将布隆过滤器保存到磁盘
func (bf *BloomFilter) Save() error {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.filter == nil {
		return nil // 没有过滤器需要保存
	}

	// 创建临时文件
	tempPath := bf.path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create bloom filter file: %w", err)
	}

	// 写入头部信息
	header := make([]byte, bloomHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], bf.size)
	// 预留其他头部字段用于未来扩展

	if _, err := file.Write(header); err != nil {
		file.Close()
		return fmt.Errorf("failed to write bloom filter header: %w", err)
	}

	// 写入布隆过滤器数据
	if _, err := bf.filter.WriteTo(file); err != nil {
		file.Close()
		return fmt.Errorf("failed to write bloom filter data: %w", err)
	}

	// 同步到磁盘
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync bloom filter file: %w", err)
	}

	// 关闭文件，确保在Windows系统上可以重命名
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close bloom filter file: %w", err)
	}

	// 原子性地替换原文件
	if err := os.Rename(tempPath, bf.path); err != nil {
		return fmt.Errorf("failed to rename bloom filter file: %w", err)
	}

	return nil
}

// Clear 清空布隆过滤器
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.filter != nil {
		bf.filter.ClearAll()
	}
	bf.size = 0
	bf.updatedAt = time.Now()
}

// EstimateFalsePositiveRate 估算当前的假阳性率
func (bf *BloomFilter) EstimateFalsePositiveRate() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.filter == nil || bf.size == 0 {
		return 0.0
	}

	return bloom.EstimateFalsePositiveRate(bf.filter.Cap(), bf.filter.K(), uint(bf.size))
}

// Merge 合并另一个布隆过滤器
func (bf *BloomFilter) Merge(other *BloomFilter) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	if bf.filter == nil || other.filter == nil {
		return fmt.Errorf("cannot merge nil bloom filters")
	}

	if err := bf.filter.Merge(other.filter); err != nil {
		return fmt.Errorf("failed to merge bloom filters: %w", err)
	}

	bf.size += other.size
	return nil
}

// Remove 删除布隆过滤器文件
func (bf *BloomFilter) Remove() error {
	if _, err := os.Stat(bf.path); os.IsNotExist(err) {
		return nil // 文件不存在
	}
	return os.Remove(bf.path)
}

// GetCreatedAt 获取创建时间
func (bf *BloomFilter) GetCreatedAt() time.Time {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.createdAt
}

// GetUpdatedAt 获取最后更新时间
func (bf *BloomFilter) GetUpdatedAt() time.Time {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.updatedAt
}

// GetPath 获取文件路径
func (bf *BloomFilter) GetPath() string {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.path
}

// GetCapacity 获取布隆过滤器容量
func (bf *BloomFilter) GetCapacity() uint {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	if bf.filter == nil {
		return 0
	}
	return bf.filter.Cap()
}

// GetHashFunctions 获取哈希函数数量
func (bf *BloomFilter) GetHashFunctions() uint {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	if bf.filter == nil {
		return 0
	}
	return bf.filter.K()
}

// IsEmpty 检查布隆过滤器是否为空
func (bf *BloomFilter) IsEmpty() bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.size == 0
}

// AddString 添加字符串键
func (bf *BloomFilter) AddString(key string) {
	bf.Add([]byte(key))
}

// TestString 测试字符串键
func (bf *BloomFilter) TestString(key string) bool {
	return bf.Test([]byte(key))
}

// AddMultiple 批量添加键
func (bf *BloomFilter) AddMultiple(keys [][]byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	if bf.filter == nil {
		return
	}

	for _, key := range keys {
		bf.filter.Add(key)
		bf.size++
	}
	bf.updatedAt = time.Now()
}

// TestMultiple 批量测试键
func (bf *BloomFilter) TestMultiple(keys [][]byte) []bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	results := make([]bool, len(keys))
	if bf.filter == nil {
		for i := range results {
			results[i] = true
		}
		return results
	}

	for i, key := range keys {
		results[i] = bf.filter.Test(key)
	}
	return results
}

// GetStats 获取布隆过滤器统计信息
func (bf *BloomFilter) GetStats() map[string]interface{} {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["size"] = bf.size
	stats["created_at"] = bf.createdAt
	stats["updated_at"] = bf.updatedAt
	stats["path"] = bf.path

	if bf.filter != nil {
		stats["capacity"] = bf.filter.Cap()
		stats["hash_functions"] = bf.filter.K()
		stats["false_positive_rate"] = bf.EstimateFalsePositiveRate()
	} else {
		stats["capacity"] = 0
		stats["hash_functions"] = 0
		stats["false_positive_rate"] = 0.0
	}

	return stats
}

// BloomFilterManager 管理多个分区的布隆过滤器
type BloomFilterManager struct {
	mu      sync.RWMutex
	filters map[int]*BloomFilter
	options BloomFilterOptions
}

// NewBloomFilterManager 创建布隆过滤器管理器
func NewBloomFilterManager(options BloomFilterOptions) *BloomFilterManager {
	bf := BloomFilterManager{
		filters: make(map[int]*BloomFilter),
		options: options,
	}
	for i := 0; i < options.PartitionNum; i++ {
		filter := NewBloomFilter(options, i)
		bf.filters[i] = filter
	}
	return &bf
}

// GetFilter 获取指定分区的布隆过滤器
func (bfm *BloomFilterManager) GetFilter(partitionID int) *BloomFilter {
	bfm.mu.RLock()
	filter, exists := bfm.filters[partitionID]
	bfm.mu.RUnlock()

	if exists {
		return filter
	}

	bfm.mu.Lock()
	defer bfm.mu.Unlock()

	// 双重检查
	if filter, exists := bfm.filters[partitionID]; exists {
		return filter
	}

	// 尝试从磁盘加载
	filter, err := LoadBloomFilter(bfm.options.DirPath, partitionID)
	if err != nil || filter == nil {
		// 创建新的布隆过滤器
		options := bfm.options
		filter = NewBloomFilter(options, partitionID)
	}
	bfm.filters[partitionID] = filter
	return filter
}

// AddKey 向指定分区添加键
func (bfm *BloomFilterManager) AddKey(key []byte) {
	partitionID := int(bfm.options.keyHashFunction(key) % uint64(bfm.options.PartitionNum))
	filter := bfm.GetFilter(partitionID)
	filter.Add(key)
}

// TestKey 检查键是否可能存在于指定分区
func (bfm *BloomFilterManager) TestKey(key []byte) bool {
	partitionID := int(bfm.options.keyHashFunction(key) % uint64(bfm.options.PartitionNum))
	filter := bfm.GetFilter(partitionID)
	return filter.Test(key)
}

// SaveAll 保存所有布隆过滤器
func (bfm *BloomFilterManager) SaveAll() error {
	bfm.mu.RLock()
	defer bfm.mu.RUnlock()

	for _, filter := range bfm.filters {
		if err := filter.Save(); err != nil {
			return err
		}
	}
	return nil
}

// ClearAll 清空所有布隆过滤器
func (bfm *BloomFilterManager) ClearAll() {
	bfm.mu.Lock()
	defer bfm.mu.Unlock()

	for _, filter := range bfm.filters {
		filter.Clear()
	}
}

// RemoveAll 删除所有布隆过滤器文件
func (bfm *BloomFilterManager) RemoveAll() error {
	bfm.mu.Lock()
	defer bfm.mu.Unlock()

	for _, filter := range bfm.filters {
		if err := filter.Remove(); err != nil {
			return err
		}
	}
	bfm.filters = make(map[int]*BloomFilter)
	return nil
}
