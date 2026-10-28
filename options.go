package knocidb

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/lni/dragonboat/v4/config"
)

type Options struct {
	// DirPath 指定存储数据库数据文件的目录路径
	DirPath string

	// SnapshotPath 指定存储快照文件的目录路径
	SnapshotPath string

	// ClusterPath 指定 Cluster/Raft 元数据目录路径。
	ClusterPath string

	// MemtableSize 表示内存表的最大字节大小 默认值为 64MB
	MemtableSize uint32

	// MemtableNums 表示在刷新到磁盘之前内存中保留的最大内存表数量 默认值为 15
	MemtableNums int

	// Sync 表示是否通过操作系统缓冲区缓存同步写入到实际磁盘
	Sync bool

	// BytesPerSync 指定在调用 fsync 之前要写入的字节数
	BytesPerSync uint32

	// PartitionNum 指定用于索引和值日志的分区数
	PartitionNum int

	// KeyHashFunction 指定用于分片的哈希函数 用于确定键属于哪个分区 默认值为 xxhash
	KeyHashFunction func([]byte) uint64

	// ValueLogFileSize 单个值日志文件的大小 默认值为 1GB
	ValueLogFileSize int64

	// 在读取指定内存容量的条目后将条目写入磁盘
	CompactBatchCapacity int

	// 废弃表推荐压缩率
	AdvisedCompactionRate float32

	// 废弃表强制压缩率
	ForceCompactionRate float32

	// 是否启用磁盘监控
	EnableDiskIO bool

	// 磁盘IO采样间隔，单位为毫秒
	DiskIOSamplingInterval int

	// 用于检索一段时间内IO繁忙状态的窗口
	DiskIOSamplingWindow int

	// 采样时间内IO时间的比率，用于表示IO的繁忙状态
	DiskIOBusyRate float32

	// 是否支持自动压缩
	AutoCompactSupport bool

	// WaitMemSpaceTimeout 指定等待内存表空间的超时时间
	// 当所有内存表都已满时，将由后台协程刷新到磁盘 但如果刷新速度慢于写入速度，内存表中可能没有空间
	// 因此写入操作将等待内存表中的空间，超时时间由 WaitMemSpaceTimeout 指定
	// 如果超过超时时间，写入操作将失败，默认值为 100ms
	WaitMemSpaceTimeout time.Duration

	// ObjectTier 配置对象存储冷层/备份层。零值表示关闭。
	ObjectTier ObjectTierOptions

	// TTLScanInterval 是核心 TTL 后台清理间隔；<=0 表示关闭后台清理。
	TTLScanInterval time.Duration

	// 是否启用布隆过滤器 默认值为 true
	EnableBloomFilter bool

	// 布隆过滤器预期元素数量 默认值为 100000
	BloomFilterExpectedElements uint

	// 布隆过滤器假阳性率 默认值为 0.01 (1%)
	BloomFilterFalsePositiveRate float64

	// 布隆过滤器自动保存间隔（秒），0 表示禁用自动保存
	BloomFilterSaveIntervalSeconds int

	// 布隆过滤器变更阈值，达到该新增/变更次数立即保存，0 表示禁用
	BloomFilterSaveChangeThreshold int
}

// BatchOptions 指定创建批处理的选项
type BatchOptions struct {
	WriteOptions
	// ReadOnly 指定批处理是否为只读
	ReadOnly bool
}

// WriteOptions 为 PutWithOptions 和 DeleteWithOptions 设置可选参数
// 如果使用 Put 和 Delete（不带选项），则表示使用默认值
type WriteOptions struct {
	// Sync 表示是否通过操作系统缓冲区缓存同步写入到实际磁盘
	Sync bool

	// DisableWal 如果为 true，写入将不会首先进入预写日志，在崩溃后可能会丢失写入 默认值为 false
	DisableWal bool
}

// IteratorOptions 是迭代器的选项
type IteratorOptions struct {
	// Prefix 按前缀过滤键
	Prefix []byte

	// Reverse 表示迭代器是否反向 false 是正向，true 是反向
	Reverse bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// 默认选项
var DefaultOptions = Options{
	DirPath:                        tempDBDir(),
	SnapshotPath:                   tempSnapDir(),
	ClusterPath:                    tempClusterDir(),
	MemtableSize:                   64 * MB,
	MemtableNums:                   15,
	Sync:                           false,
	BytesPerSync:                   0,
	PartitionNum:                   3,
	KeyHashFunction:                xxhash.Sum64,
	ValueLogFileSize:               1 * GB,
	CompactBatchCapacity:           1 << 30,
	AdvisedCompactionRate:          0.3,
	ForceCompactionRate:            0.5,
	EnableDiskIO:                   false,
	DiskIOSamplingInterval:         100,
	DiskIOSamplingWindow:           10,
	DiskIOBusyRate:                 0.5,
	AutoCompactSupport:             false,
	WaitMemSpaceTimeout:            100 * time.Millisecond,
	TTLScanInterval:                time.Second,
	EnableBloomFilter:              true,
	BloomFilterExpectedElements:    100000,
	BloomFilterFalsePositiveRate:   0.01,
	BloomFilterSaveIntervalSeconds: 30,
	BloomFilterSaveChangeThreshold: 1000,
}

var DefaultBatchOptions = BatchOptions{
	WriteOptions: DefaultWriteOptions,
	ReadOnly:     false,
}

var DefaultWriteOptions = WriteOptions{
	Sync:       false,
	DisableWal: false,
}

// Options 表示Raft节点的配置信息
type ClusterOptions struct {
	// NodeID 是当前节点的唯一标识符
	NodeID uint64
	// ClusterID 是Raft集群的唯一标识符
	ClusterID uint64
	// RaftAddress 是当前节点的网络地址，格式为 "host:port"
	RaftAddress string
	// DataDir 是存储Raft日志和快照的目录
	DataDir string
	// JoinCluster 表示是否作为新节点加入现有集群
	JoinCluster bool
	// InitialMembers 是初始集群成员的映射，键是NodeID，值是RaftAddress。
	// Bootstrap 节点至少包含自己；Join 节点可以为空。
	InitialMembers map[uint64]string
	// TickMs 是心跳间隔，单位为毫秒
	TickMs uint64
	// ElectionRTTMs 是选举超时，单位为心跳间隔的倍数
	ElectionRTTMs uint64
	// HeartbeatRTTMs 是心跳超时，单位为心跳间隔的倍数
	HeartbeatRTTMs uint64
	// SnapshotIntervalSeconds 是自动创建快照的间隔，单位为秒
	SnapshotIntervalSeconds uint64
	// Sync 指定节点是同步还是异步更新状态
	Sync bool

	// RPCAddress 是本节点的内部转发 RPC 地址；为空时不启动转发服务。
	RPCAddress string
	// PeerRPC 是 NodeID -> 内部转发 RPC 地址的映射。
	PeerRPC map[uint64]string
	// RPCSecret 是内部转发 RPC 的共享密钥；为空表示不鉴权。
	// 密钥以明文传输，生产环境应部署在私网/VPN 中，并等待后续 mTLS 支持。
	RPCSecret string
	// RPCTimeout 是内部转发 RPC 的超时时间，默认 5s。
	RPCTimeout time.Duration
}

// GetNodeHostOptions 返回Dragonboat节点主机配置
func (c *ClusterOptions) GetNodeHostOptions() config.NodeHostConfig {
	return config.NodeHostConfig{
		RaftAddress:    c.RaftAddress,
		NodeHostDir:    filepath.Join(c.DataDir, "nodehost"),
		RTTMillisecond: c.TickMs,
		WALDir:         filepath.Join(c.DataDir, "wal"),
		DeploymentID:   1,
	}
}

// GetRaftConfig 返回 Dragonboat 的 Raft 配置。
func (c *ClusterOptions) GetRaftConfig() config.Config {
	return config.Config{
		ReplicaID:               c.NodeID,
		ShardID:                 c.ClusterID,
		ElectionRTT:             c.ElectionRTTMs,
		HeartbeatRTT:            c.HeartbeatRTTMs,
		CheckQuorum:             true,
		SnapshotEntries:         c.SnapshotIntervalSeconds,
		CompactionOverhead:      5,
		SnapshotCompressionType: config.Snappy,
		EntryCompressionType:    config.Snappy,
		DisableAutoCompactions:  false,
		PreVote:                 true,
	}
}

// DefaultOptions 返回默认的Raft配置
func DefaultClusterOptions(nodeID, clusterID uint64, raftAddress, dataDir string) ClusterOptions {
	opt := ClusterOptions{
		NodeID:                  nodeID,
		ClusterID:               clusterID,
		RaftAddress:             raftAddress,
		DataDir:                 dataDir,
		JoinCluster:             false,
		InitialMembers:          make(map[uint64]string),
		TickMs:                  100,
		ElectionRTTMs:           10,
		HeartbeatRTTMs:          1,
		SnapshotIntervalSeconds: 3600, // 默认每小时创建一次快照
		Sync:                    true,
		RPCTimeout:              5 * time.Second,
	}
	opt.InitialMembers[nodeID] = raftAddress
	return opt
}

func (c *ClusterOptions) applyDefaults() {
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = 5 * time.Second
	}
}

// ValidateOptions 验证配置是否有效
func ValidateClusterOptions(opt *ClusterOptions) error {
	if opt.NodeID == 0 {
		return fmt.Errorf("NodeID can not be0")
	}
	if opt.ClusterID == 0 {
		return fmt.Errorf("ClusterID can not be 0")
	}
	if opt.RaftAddress == "" {
		return fmt.Errorf("RaftAddress can not be nil")
	}
	if opt.DataDir == "" {
		return fmt.Errorf("DataDir can not be nil")
	}
	if !opt.JoinCluster && len(opt.InitialMembers) == 0 {
		return fmt.Errorf("InitialMembers should have at least one member")
	}
	if opt.ElectionRTTMs == 0 {
		return fmt.Errorf("ElectionRTTMs can not be 0")
	}
	if opt.ElectionRTTMs <= 2*opt.HeartbeatRTTMs {
		return fmt.Errorf("ElectionRTTMs must greater than 2*HeartbeatRTTMs")
	}
	if opt.ElectionRTTMs < 10*opt.HeartbeatRTTMs {
		fmt.Printf("warnning: ElectionRTTMs support to greater than 10*HeartbeatRTTMs\n")
	}
	return nil
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "knocidb-data")
	return dir
}

func tempSnapDir() string {
	dir, _ := os.MkdirTemp("", "knocidb-sanpshot")
	return dir
}

func tempClusterDir() string {
	dir, _ := os.MkdirTemp("", "knocidb-cluster")
	return dir
}
