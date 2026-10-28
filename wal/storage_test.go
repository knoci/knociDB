package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

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
	if os.Getenv("KNOCIDB_S3_TEST") != "1" {
		t.Skip("set KNOCIDB_S3_TEST=1 and provide wal/config.yaml credentials to run S3 integration test")
	}
	if err := setupTestFile(); err != nil {
		t.Fatalf("Failed to setup test file: %v", err)
	}
	defer cleanupTestFiles()

	storage, err := NewObjectStorage()
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

func TestNewObjectStorageConfigErrors(t *testing.T) {
	if _, err := NewObjectStorageWithConfig(ObjectStorageConfig{Type: "unknown"}); err == nil {
		t.Fatal("unknown storage type should fail")
	}
}

func TestNewS3StorageLocalOnly(t *testing.T) {
	storage, err := NewObjectStorageWithConfig(ObjectStorageConfig{
		Type:      S3ObjectStorage,
		Endpoint:  "http://127.0.0.1:1",
		Region:    "us-east-1",
		Bucket:    "bucket",
		AccessKey: "access",
		SecretKey: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if storage == nil {
		t.Fatal("nil storage")
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage.Upload(filepath.Join(os.TempDir(), "definitely-missing-knocidb"), "key"); err == nil {
		t.Fatal("uploading a missing file should fail")
	}

	blocker := filepath.Join(os.TempDir(), "knocidb-download-blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(blocker)
	if err := storage.Download("key", filepath.Join(blocker, "child")); err == nil {
		t.Fatal("download into a file path should fail")
	}
}

func TestNewOSSStorageLocalOnly(t *testing.T) {
	storage, err := NewObjectStorageWithConfig(ObjectStorageConfig{
		Type:      OSSObjectStorage,
		Endpoint:  "http://127.0.0.1:1",
		Bucket:    "bucket",
		AccessKey: "access",
		SecretKey: "secret",
	})
	if err != nil {
		return
	}
	if storage == nil {
		t.Fatal("nil storage")
	}
	defer storage.Close()
	if err := storage.Upload(filepath.Join(os.TempDir(), "definitely-missing-knocidb"), "key"); err == nil {
		t.Fatal("uploading a missing file should fail")
	}
}
