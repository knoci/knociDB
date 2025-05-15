package knocidb

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDiskIOInit(t *testing.T) {
	diskIO := &DiskIO{
		targetPath:       os.TempDir(),
		samplingInterval: 100,
		windowSize:       5,
		busyRate:         0.8,
	}

	diskIO.Init()

	if diskIO.samplingWindow == nil {
		t.Error("samplingWindow should not be nil after initialization")
	}

	if len(diskIO.samplingWindow) != diskIO.windowSize {
		t.Errorf("Expected samplingWindow length %d, got %d", diskIO.windowSize, len(diskIO.samplingWindow))
	}

	if diskIO.windowPoint != 0 {
		t.Errorf("Expected windowPoint to be 0, got %d", diskIO.windowPoint)
	}
}

func TestDiskIOIsFree(t *testing.T) {
	diskIO := &DiskIO{
		targetPath:       os.TempDir(),
		samplingInterval: 100,
		windowSize:       5,
		busyRate:         -1,
	}

	diskIO.Init()

	free, err := diskIO.IsFree()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !free {
		t.Error("Expected disk to be free when busyRate is negative")
	}

	diskIO.busyRate = 0.8
	diskIO.freeFlag = true

	free, err = diskIO.IsFree()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !free {
		t.Error("Expected disk to be free when freeFlag is true")
	}

	diskIO.freeFlag = false

	free, err = diskIO.IsFree()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if free {
		t.Error("Expected disk to be busy when freeFlag is false")
	}
}

func TestDiskIOMonitor(t *testing.T) {
	diskIO := &DiskIO{
		targetPath:       os.TempDir(),
		samplingInterval: 10,
		windowSize:       3,
		busyRate:         0.8,
	}

	diskIO.Init()

	for i := 0; i < diskIO.windowSize; i++ {
		diskIO.samplingWindow[i] = uint64(i * 10)
	}

	diskIO.windowPoint = 0

	diskIO.mu.Lock()
	diskIO.freeFlag = true
	diskIO.mu.Unlock()

	free, err := diskIO.IsFree()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !free {
		t.Error("Expected disk to be free")
	}

	diskIO.mu.Lock()
	diskIO.freeFlag = false
	diskIO.mu.Unlock()

	free, err = diskIO.IsFree()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if free {
		t.Error("Expected disk to be busy")
	}
}

func TestIsPathOnDevice(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "diskio_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	subDir := filepath.Join(tempDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	result := isPathOnDevice(subDir, tempDir)
	if !result {
		t.Errorf("Expected %s to be on device %s", subDir, tempDir)
	}

	otherDir, err := os.MkdirTemp("", "other_diskio_test")
	if err != nil {
		t.Fatalf("Failed to create other temp dir: %v", err)
	}
	defer os.RemoveAll(otherDir)

	result = isPathOnDevice(otherDir, tempDir)
	if result {
		t.Errorf("Expected %s to not be on device %s", otherDir, tempDir)
	}
}

func TestGetDeviceName(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"/dev/sda1", "sda1"},
		{"/dev/nvme0n1p1", "nvme0n1p1"},
		{"C:\\", "C:\\"},
		{"", ""},
	}

	for _, tc := range testCases {
		result := getDeviceName(tc.input)
		if result != tc.expected {
			t.Errorf("getDeviceName(%s) = %s, expected %s", tc.input, result, tc.expected)
		}
	}
}

func TestDiskIOIntegration(t *testing.T) {

	// 创建DiskIO实例
	diskIO := &DiskIO{
		targetPath:       os.TempDir(),
		samplingInterval: 100,
		windowSize:       3,
		busyRate:         0.8,
	}

	diskIO.Init()

	err := diskIO.Monitor()
	_ = err

	time.Sleep(200 * time.Millisecond)

	free, err := diskIO.IsFree()
	_ = free
	_ = err
}
