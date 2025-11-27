package wal

import (
    "context"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "time"

    "github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type OSSStorage struct {
    client *oss.Client
    bucket *oss.Bucket
}

func newOSSStorage(config ObjectStorageConfig) (*OSSStorage, error) {
    client, err := oss.New(config.Endpoint, config.AccessKey, config.SecretKey)
    if err != nil {
        return nil, fmt.Errorf("failed to create oss client: %w", err)
    }
    bucket, err := client.Bucket(config.Bucket)
    if err != nil {
        return nil, fmt.Errorf("failed to get bucket: %w", err)
    }
    return &OSSStorage{client: client, bucket: bucket}, nil
}

func (s *OSSStorage) Upload(localPath, objectKey string) error {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    file, err := os.Open(localPath)
    if err != nil {
        return fmt.Errorf("failed to open file %s: %w", localPath, err)
    }
    defer file.Close()
    err = s.bucket.PutObject(objectKey, file, oss.WithContext(ctx))
    if err != nil {
        return fmt.Errorf("failed to upload file to oss: %w", err)
    }
    return nil
}

func (s *OSSStorage) Download(objectKey, localPath string) error {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    dir := filepath.Dir(localPath)
    if err := os.MkdirAll(dir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create directory %s: %w", dir, err)
    }
    body, err := s.bucket.GetObject(objectKey, oss.WithContext(ctx))
    if err != nil {
        return fmt.Errorf("failed to download file from oss: %w", err)
    }
    defer body.Close()
    file, err := os.Create(localPath)
    if err != nil {
        return fmt.Errorf("failed to create file %s: %w", localPath, err)
    }
    defer file.Close()
    if _, err := io.Copy(file, body); err != nil {
        return fmt.Errorf("failed to write file content: %w", err)
    }
    return nil
}

func (s *OSSStorage) List() ([]string, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    var keys []string
    marker := ""
    for {
        resp, err := s.bucket.ListObjects(oss.Marker(marker), oss.MaxKeys(1000), oss.WithContext(ctx))
        if err != nil {
            return nil, fmt.Errorf("failed to list objects in oss: %w", err)
        }
        for _, obj := range resp.Objects {
            keys = append(keys, obj.Key)
        }
        if !resp.IsTruncated {
            break
        }
        marker = resp.NextMarker
    }
    return keys, nil
}

func (s *OSSStorage) Close() error {
    return nil
}

