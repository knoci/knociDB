package wal

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/viper"
)

// ObjectStorageType 表示对象存储的类型
type ObjectStorageType string

const (
	// S3ObjectStorage 表示使用亚马逊S3对象存储
	S3ObjectStorage ObjectStorageType = "s3"
	// R2ObjectStorage 表示使用Cloudflare R2对象存储
	R2ObjectStorage ObjectStorageType = "r2"
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

// NewObjectStorage 创建一个新的对象存储
func NewObjectStorage() (ObjectStorage, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var storage ObjectStorage
	var err error
	config := ObjectStorageConfig{
		Type:      ObjectStorageType(viper.GetString("object_storage.type")),
		Endpoint:  viper.GetString("object_storage.endpoint"),
		Region:    viper.GetString("object_storage.region"),
		Bucket:    viper.GetString("object_storage.bucket"),
		AccessKey: viper.GetString("object_storage.accesskey"),
		SecretKey: viper.GetString("object_storage.secretkey"),
	}
	switch config.Type {
	case S3ObjectStorage, R2ObjectStorage:
		storage, err = newS3Storage(config)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported object storage type: %s", config.Type)
	}

	return storage, nil
}

// newS3Storage 创建一个新的S3对象存储
func newS3Storage(config ObjectStorageConfig) (*S3Storage, error) {
	ctx := context.Background()
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               config.Endpoint,
			SigningRegion:     config.Region,
			HostnameImmutable: true,
		}, nil
	})

	configOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithEndpointResolverWithOptions(customResolver),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, "")),
	}

	// 只有当Region不为空时才添加Region配置
	if config.Region != "" {
		configOptions = append(configOptions, awsconfig.WithRegion(config.Region))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

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

// Download 从S3对象存储下载文件
func (s *S3Storage) Download(objectKey, localPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 确保目录存在
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// 创建本地文件
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}
	defer file.Close()

	// 从S3下载对象
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {

		}
	}(resp.Body)

	// 将对象内容写入本地文件
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write file content: %w", err)
	}

	return nil
}

// List 列出S3对象存储中的文件
func (s *S3Storage) List() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in S3: %w", err)
	}

	var objects []string
	for _, obj := range resp.Contents {
		objects = append(objects, aws.ToString(obj.Key))
	}

	return objects, nil
}

// Close 关闭S3对象存储连接
func (s *S3Storage) Close() error {
	// S3客户端不需要显式关闭
	return nil
}
