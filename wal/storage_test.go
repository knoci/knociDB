package wal

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	testLocalFile  = "testdata/testfile.txt"
	testObjectKey  = "testfile.txt"
	testDownloadTo = "testdata/downloaded.txt"
	testBucket     = "misc"
)

func setupTestFile() error {
	if err := os.MkdirAll("testdata", os.ModePerm); err != nil {
		return err
	}
	return os.WriteFile(testLocalFile, []byte("Test file content"), 0644)
}

func cleanupTestFiles() {
	_ = os.Remove(testLocalFile)
	_ = os.Remove(testDownloadTo)
	_ = os.RemoveAll("testdata")
}

// cleanupS3Objects 删除测试创建的S3对象
func cleanupS3Objects(s *S3Storage, objectKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to delete test object: %w", err)
	}
	return nil
}

func ensureTestBucket(s *S3Storage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		// 忽略已存在的错误
		var bne *types.BucketAlreadyExists
		var boo *types.BucketAlreadyOwnedByYou
		if !(errors.As(err, &bne) || errors.As(err, &boo)) {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	return nil
}

func TestS3StorageOperations(t *testing.T) {
	if err := setupTestFile(); err != nil {
		t.Fatalf("Failed to setup test file: %v", err)
	}
	defer cleanupTestFiles()

	config := ObjectStorageConfig{
		Type:      R2ObjectStorage,
		Endpoint:  "your-endpoint", // 替换为你的测试S3端点
		Region:    "APAC",
		Bucket:    testBucket,
		AccessKey: "your-accesskey", // 替换为你的测试访问密钥
		SecretKey: "your-secretkey", // 替换为你的测试秘密密钥
	}

	storage, err := NewObjectStorage(config)
	if err != nil {
		t.Fatalf("Failed to create object storage: %v", err)
	}
	if storage == nil {
		t.Fatal("Expected storage instance, got nil")
	}
	defer func() {
		// 测试结束后清理S3对象
		if s3Storage, ok := storage.(*S3Storage); ok {
			if err := cleanupS3Objects(s3Storage, testObjectKey); err != nil {
				t.Logf("Warning: failed to cleanup S3 objects: %v", err)
			}
		}
		if err := storage.Close(); err != nil {
			t.Errorf("Failed to close storage: %v", err)
		}
	}()

	s3Storage, ok := storage.(*S3Storage)
	if !ok {
		t.Fatal("Expected *S3Storage type")
	}

	if err := ensureTestBucket(s3Storage); err != nil {
		t.Fatalf("Failed to ensure test bucket: %v", err)
	}

	t.Run("Upload", func(t *testing.T) {
		if err := storage.Upload(testLocalFile, testObjectKey); err != nil {
			t.Errorf("Upload failed: %v", err)
		}
	})

	t.Run("List", func(t *testing.T) {
		time.Sleep(1 * time.Second)

		objects, err := storage.List()
		if err != nil {
			t.Errorf("List failed: %v", err)
			return
		}

		found := false
		for _, obj := range objects {
			if obj == testObjectKey {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Expected to find %s in object list, got: %v", testObjectKey, objects)
		}
	})

	t.Run("Download", func(t *testing.T) {
		if err := storage.Download(testObjectKey, testDownloadTo); err != nil {
			t.Errorf("Download failed: %v", err)
		}

		if _, err := os.Stat(testDownloadTo); os.IsNotExist(err) {
			t.Errorf("Downloaded file does not exist")
		}
	})

	// 测试Close
	t.Run("Close", func(t *testing.T) {
		err := storage.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})
}

func TestNoObjectStorage(t *testing.T) {
	// 测试不使用对象存储的情况
	config := ObjectStorageConfig{
		Type: NoObjectStorage,
	}

	storage, err := NewObjectStorage(config)
	if err != nil {
		t.Errorf("Unexpected error for NoObjectStorage: %v", err)
	}
	if storage != nil {
		t.Error("Expected nil storage for NoObjectStorage")
	}
}
