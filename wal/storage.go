package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/knoci/knocidb/util"
)

// ObjectStorageType 表示对象存储的类型
type ObjectStorageType string

const (
	// S3ObjectStorage 表示使用亚马逊S3对象存储
	S3ObjectStorage ObjectStorageType = "s3"
	// R2ObjectStorage 表示使用Cloudflare R2对象存储
	R2ObjectStorage ObjectStorageType = "r2"
	// TCOSObjectStorage 表示使用腾讯云COS
	COSObjectStorage ObjectStorageType = "cos"
	// OSSObjectStorage 表示使用阿里云OSS
	OSSObjectStorage ObjectStorageType = "oss"
)

// ObjectStorageConfig 表示对象存储的配置
type ObjectStorageConfig struct {
	// Type 表示对象存储的类型，支持S3和R2
	Type ObjectStorageType
	// Endpoint 表示对象存储的端点
	Endpoint string
	// Region 表示对象存储的区域
	Region string
	// Bucket 表示对象存储的桶名
	Bucket string
	// AccessKey 表示对象存储的访问密钥
	AccessKey string
	// SecretKey 表示对象存储的秘密密钥
	SecretKey string
}

// ObjectStorage 表示对象存储接口
type ObjectStorage interface {
	// Upload 上传文件到对象存储
	Upload(localPath, objectKey string) error
	// Download 从对象存储下载文件
	Download(objectKey, localPath string) error
	// List 列出对象存储中的文件
	List() ([]string, error)
	// Close 关闭对象存储连接
	Close() error
}

// S3Storage 表示S3对象存储
type S3Storage struct {
	client *s3.Client
	bucket string
}

// NewObjectStorage 从当前目录的 config.yaml 创建一个新的对象存储（纯标准库解析，无 viper 依赖）。
// 该入口保留给旧代码；新代码应使用 NewObjectStorageWithConfig，避免全局配置文件。
func NewObjectStorage() (ObjectStorage, error) {
	f, err := os.Open("config.yaml")
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	defer f.Close()

	var cfg ObjectStorageConfig
	inSection := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		raw := sc.Text()
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if !strings.HasPrefix(raw, " ") && !strings.HasPrefix(raw, "\t") {
			inSection = strings.HasPrefix(trimmed, "object_storage:")
			continue
		}
		if !inSection {
			continue
		}
		k, v, ok := strings.Cut(trimmed, ":")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		if idx := strings.Index(v, " #"); idx >= 0 {
			v = strings.TrimSpace(v[:idx])
		}
		v = strings.Trim(v, `"'`)
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "type":
			cfg.Type = ObjectStorageType(v)
		case "endpoint":
			cfg.Endpoint = v
		case "region":
			cfg.Region = v
		case "bucket":
			cfg.Bucket = v
		case "accesskey":
			cfg.AccessKey = v
		case "secretkey":
			cfg.SecretKey = v
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return NewObjectStorageWithConfig(cfg)
}

// NewObjectStorageWithConfig 使用显式配置创建对象存储。
func NewObjectStorageWithConfig(config ObjectStorageConfig) (ObjectStorage, error) {
	if config.Type == "" {
		config.Type = S3ObjectStorage
	}
	switch config.Type {
	case S3ObjectStorage, R2ObjectStorage, COSObjectStorage:
		return newS3Storage(config)
	case OSSObjectStorage:
		return newOSSStorage(config)
	default:
		return nil, fmt.Errorf("unsupported object storage type: %s", config.Type)
	}
}

// newS3Storage 创建一个新的S3对象存储
func newS3Storage(config ObjectStorageConfig) (*S3Storage, error) {
	ctx := context.Background()
	configOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")),
	}

	// 只有当Region不为空时才添加Region配置
	if config.Region != "" {
		configOptions = append(configOptions, awsconfig.WithRegion(config.Region))
	}

	// 仅当提供了自定义 Endpoint 时才设置 Endpoint 解析
	if config.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               config.Endpoint,
				SigningRegion:     config.Region,
				HostnameImmutable: true,
			}, nil
		})
		configOptions = append(configOptions, awsconfig.WithEndpointResolverWithOptions(customResolver))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	usePathStyle := config.Endpoint != ""
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if usePathStyle {
			o.UsePathStyle = true
		}
	})

	return &S3Storage{
		client: client,
		bucket: config.Bucket,
	}, nil
}

// Upload 上传文件到S3对象存储
func (s *S3Storage) Upload(localPath, objectKey string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", localPath, err)
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	return nil
}

// Download 从S3对象存储原子下载文件（写入临时文件并落盘后再 RenameDurable，避免中断产生半截文件）
func (s *S3Storage) Download(objectKey, localPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}
	defer resp.Body.Close()

	tmpFile, err := os.CreateTemp(dir, ".download-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file in %s: %w", dir, err)
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed to write file content: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed to sync downloaded file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close downloaded file: %w", err)
	}
	if err := util.RenameDurable(tmpPath, localPath); err != nil {
		return fmt.Errorf("failed to publish downloaded file: %w", err)
	}
	cleanup = false
	return util.SyncDir(dir)
}

// List 列出S3对象存储中的文件
func (s *S3Storage) List() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var objects []string
	var token *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in S3: %w", err)
		}
		for _, obj := range resp.Contents {
			objects = append(objects, aws.ToString(obj.Key))
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}

	return objects, nil
}

// Close 关闭S3对象存储连接
func (s *S3Storage) Close() error {
	// S3客户端不需要显式关闭
	return nil
}
