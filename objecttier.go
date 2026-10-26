package knocidb

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/knoci/knocidb/wal"
)

// ErrNoObjectSnapshot 表示对象存储中还没有可恢复的快照。
var ErrNoObjectSnapshot = errors.New("no snapshot found in object storage")

const (
	defaultSnapshotObjectPrefix = "snapshots"
	defaultSegmentObjectPrefix  = "segments"
	defaultObjectUploadInterval = 30 * time.Second
)

// ObjectTierOptions 配置对象存储冷层/备份层。
//
// 零值表示关闭对象存储。Store 可用于测试或让调用方注入自定义对象存储实现；
// 当 Store 为空时，根据 Provider/Bucket/... 创建内置的 S3/OSS 客户端。
type ObjectTierOptions struct {
	Enabled   bool
	Provider  string // s3, r2, cos, oss
	Bucket    string
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string

	// Prefix 是所有对象键的公共前缀，便于多库/多租户隔离。
	Prefix string
	// SnapshotPrefix 是快照对象的二级前缀，默认 snapshots。
	SnapshotPrefix string
	// SegmentPrefix 是冷段对象的二级前缀，默认 segments。
	SegmentPrefix string
	// UploadInterval 是冷段后台上传的扫描间隔，默认 30s。
	UploadInterval time.Duration

	// Store 允许注入自定义对象存储；注入的 Store 不由 ObjectTier 关闭。
	Store wal.ObjectStorage
}

func (o ObjectTierOptions) withDefaults() ObjectTierOptions {
	if o.Provider == "" {
		o.Provider = string(wal.S3ObjectStorage)
	}
	if o.SnapshotPrefix == "" {
		o.SnapshotPrefix = defaultSnapshotObjectPrefix
	}
	if o.SegmentPrefix == "" {
		o.SegmentPrefix = defaultSegmentObjectPrefix
	}
	if o.UploadInterval <= 0 {
		o.UploadInterval = defaultObjectUploadInterval
	}
	o.Prefix = strings.Trim(o.Prefix, "/")
	o.SnapshotPrefix = strings.Trim(o.SnapshotPrefix, "/")
	o.SegmentPrefix = strings.Trim(o.SegmentPrefix, "/")
	return o
}

type uploadedSegment struct {
	size    int64
	modTime time.Time
}

type objectTier struct {
	store wal.ObjectStorage
	owns  bool
	opts  ObjectTierOptions

	mu       sync.Mutex
	uploaded map[string]uploadedSegment

	closeOnce sync.Once
}

func newObjectTier(opts ObjectTierOptions) (*objectTier, error) {
	if !opts.Enabled {
		return nil, nil
	}
	opts = opts.withDefaults()
	if opts.Bucket == "" && opts.Store == nil {
		return nil, fmt.Errorf("object tier bucket is empty")
	}

	store := opts.Store
	owns := false
	if store == nil {
		var err error
		store, err = wal.NewObjectStorageWithConfig(wal.ObjectStorageConfig{
			Type:      wal.ObjectStorageType(opts.Provider),
			Endpoint:  opts.Endpoint,
			Region:    opts.Region,
			Bucket:    opts.Bucket,
			AccessKey: opts.AccessKey,
			SecretKey: opts.SecretKey,
		})
		if err != nil {
			return nil, err
		}
		owns = true
	}

	return &objectTier{
		store:    store,
		owns:     owns,
		opts:     opts,
		uploaded: make(map[string]uploadedSegment),
	}, nil
}

func (o *objectTier) Close() error {
	if o == nil {
		return nil
	}
	var err error
	o.closeOnce.Do(func() {
		if o.owns {
			err = o.store.Close()
		}
	})
	return err
}

func (o *objectTier) objectKey(parts ...string) string {
	all := make([]string, 0, len(parts)+1)
	if o.opts.Prefix != "" {
		all = append(all, o.opts.Prefix)
	}
	all = append(all, parts...)
	return path.Join(all...)
}

func (o *objectTier) list(prefix string) ([]string, error) {
	keys, err := o.store.List()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

// UploadSnapshot 把本地 .ZST 快照上传到对象存储，返回对象键。
func (o *objectTier) UploadSnapshot(localPath string) (string, error) {
	base := filepath.Base(localPath)
	if !strings.HasSuffix(base, snapshotExt) {
		return "", fmt.Errorf("snapshot file must end with %s: %s", snapshotExt, localPath)
	}
	key := o.objectKey(o.opts.SnapshotPrefix, base)
	if err := o.store.Upload(localPath, key); err != nil {
		return "", err
	}
	return key, nil
}

// DownloadLatestSnapshot 下载对象存储中最新的快照到 dstDir，返回本地路径。
func (o *objectTier) DownloadLatestSnapshot(dstDir string) (string, error) {
	prefix := o.objectKey(o.opts.SnapshotPrefix) + "/"
	keys, err := o.list(prefix)
	if err != nil {
		return "", err
	}
	var names []string
	for _, key := range keys {
		name := path.Base(key)
		if strings.HasSuffix(name, snapshotExt) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "", ErrNoObjectSnapshot
	}
	sort.Strings(names)
	name := names[len(names)-1]
	if err := os.MkdirAll(dstDir, os.ModePerm); err != nil {
		return "", err
	}
	localPath := filepath.Join(dstDir, name)
	if err := o.store.Download(prefix+name, localPath); err != nil {
		return "", err
	}
	return localPath, nil
}

// UploadColdSegments 扫描 dir 下的非活跃 .VLOG 段并上传。
// active 中的文件名会被跳过；同一文件大小和修改时间未变化时不会重复上传。
func (o *objectTier) UploadColdSegments(dir string, active map[string]struct{}) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	uploaded := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.Contains(name, ".VLOG.") || strings.HasSuffix(name, ".temp") {
			continue
		}
		if _, ok := active[name]; ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return uploaded, err
		}
		o.mu.Lock()
		prev, seen := o.uploaded[name]
		o.mu.Unlock()
		if seen && prev.size == info.Size() && prev.modTime.Equal(info.ModTime()) {
			continue
		}
		key := o.objectKey(o.opts.SegmentPrefix, name)
		if err := o.store.Upload(filepath.Join(dir, name), key); err != nil {
			return uploaded, err
		}
		o.mu.Lock()
		o.uploaded[name] = uploadedSegment{size: info.Size(), modTime: info.ModTime()}
		o.mu.Unlock()
		uploaded++
	}
	return uploaded, nil
}

// uploadSnapshotWithOptions 使用 Options 中的对象存储配置上传快照。
func uploadSnapshotWithOptions(options Options, localPath string) error {
	if !options.ObjectTier.Enabled {
		return nil
	}
	tier, err := newObjectTier(options.ObjectTier)
	if err != nil {
		return err
	}
	defer tier.Close()
	_, err = tier.UploadSnapshot(localPath)
	return err
}

// hasDatabaseFiles 判断目录中是否已有数据库数据文件。
// 只存在 FLOCK 或普通无关文件时仍允许从对象存储恢复。
func hasDatabaseFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == fileLockName {
			continue
		}
		if name == discardMetaName ||
			strings.Contains(name, ".VLOG.") ||
			strings.HasSuffix(name, ".SEG") ||
			strings.HasPrefix(name, "INDEX.") ||
			strings.HasPrefix(name, "BLOOM.") {
			return true
		}
	}
	return false
}

// restoreFromObjectTier 在本地目录为空时下载最新快照并导入。
func restoreFromObjectTier(options Options) error {
	if !options.ObjectTier.Enabled || hasDatabaseFiles(options.DirPath) {
		return nil
	}
	tier, err := newObjectTier(options.ObjectTier)
	if err != nil {
		return err
	}
	defer tier.Close()

	snapPath, err := tier.DownloadLatestSnapshot(options.SnapshotPath)
	if errors.Is(err, ErrNoObjectSnapshot) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("download snapshot from object storage failed: %w", err)
	}
	if err := importSnapshotToDir(snapPath, options.DirPath); err != nil {
		return fmt.Errorf("restore snapshot from object storage failed: %w", err)
	}
	log.Printf("restored database from object snapshot: %s", snapPath)
	return nil
}
