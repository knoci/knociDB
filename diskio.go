package knocidb

import (
	"errors"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/disk"
)

type DiskIO struct {
	targetPath       string   // 数据库路径，用于查找磁盘设备
	samplingInterval int      // 采样时间，单位为毫秒
	samplingWindow   []uint64 // 采样窗口，用于在一段时间内采样平均 IoTime
	windowSize       int      // 用于采样的滑动窗口大小
	windowPoint      int      // 窗口中下一个采样的偏移量
	busyRate         float32  // 通过采样时间内 IO 时间的比例来表示 IO 繁忙状态
	freeFlag         bool     // freeFlag 表示磁盘是否空闲
	mu               sync.Mutex
}

func (io *DiskIO) Init() {
	io.samplingWindow = make([]uint64, io.windowSize)
	io.windowPoint = 0
}

func (io *DiskIO) Monitor() error {
	var ioStart disk.IOCountersStat
	var ioEnd disk.IOCountersStat
	var err error

	ioStart, err = GetDiskIOInfo((io.targetPath))
	if err != nil {
		return err
	}

	time.Sleep(time.Duration(io.samplingInterval) * time.Millisecond)

	ioEnd, err = GetDiskIOInfo((io.targetPath))
	if err != nil {
		return err
	}

	// IoTime 是自系统启动以来设备的活动时间，我们在采样期间获取它
	ioTime := ioEnd.IoTime - ioStart.IoTime

	// 设置时间并将指针移动到下一个槽位
	io.samplingWindow[io.windowPoint] = ioTime
	io.windowPoint++
	io.windowPoint %= io.windowSize

	// 获取平均 IoTime
	var sum uint64
	for _, value := range io.samplingWindow {
		sum += value
	}
	meanTime := sum / (uint64(io.windowSize))

	// 其他人可能通过 IsFree 读取 io.freeFlag，所以在更改时需要锁定
	io.mu.Lock()
	defer io.mu.Unlock()
	if meanTime > uint64(float32(io.samplingInterval)*io.busyRate) {
		io.freeFlag = false
	} else {
		io.freeFlag = true
	}
	return nil
}

func (io *DiskIO) IsFree() (bool, error) {
	if io.busyRate < 0 {
		return true, nil
	}
	io.mu.Lock()
	defer io.mu.Unlock()
	return io.freeFlag, nil
}

func GetDiskIOInfo(targetPath string) (disk.IOCountersStat, error) {
	var io disk.IOCountersStat
	// 获取所有挂载点
	partitions, err := disk.Partitions(false)
	if err != nil {
		log.Println("Error getting partitions:", err)
		return io, err
	}

	var targetDevice string

	// 查找目标路径所在的挂载点
	for _, partition := range partitions {
		if isPathOnDevice(targetPath, partition.Mountpoint) {
			targetDevice = partition.Device
			break
		}
	}

	targetDevice = getDeviceName(targetDevice)

	// 获取设备的 I/O 状态
	ioCounters, err := disk.IOCounters()
	if err != nil {
		return io, err
	}

	var exists bool
	if io, exists = ioCounters[targetDevice]; !exists {
		return io, errors.New("device " + targetDevice + " no available I/O statistics")
	}

	return io, nil
}

// 检查路径是否在指定的挂载点上
func getDeviceName(devicePath string) string {
	parts := strings.Split(devicePath, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return devicePath
}

// 检查路径是否在指定的挂载点上
func isPathOnDevice(path, mountpoint string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.Println("error obtaining absolute path:", err)
		return false
	}

	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		log.Println("error obtaining absolute mount point:", err)
		return false
	}

	// 确保路径已标准化以进行比较
	absPath = filepath.Clean(absPath)
	absMountpoint = filepath.Clean(absMountpoint)

	return strings.HasPrefix(absPath, absMountpoint)
}
