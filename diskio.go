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
	if io.windowSize <= 0 {
		io.windowSize = 10
	}
	if io.samplingInterval <= 0 {
		io.samplingInterval = 100
	}
	io.samplingWindow = make([]uint64, io.windowSize)
	io.windowPoint = 0
	io.freeFlag = true
}

func (io *DiskIO) Monitor() error {
	if io.windowSize <= 0 || len(io.samplingWindow) == 0 {
		io.Init()
	}
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

	// IoTime 是自系统启动以来设备的活动时间，防止计数器回绕产生无符号下溢。
	var ioTime uint64
	if ioEnd.IoTime >= ioStart.IoTime {
		ioTime = ioEnd.IoTime - ioStart.IoTime
	}

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
	bestLen := -1

	// 查找目标路径所在的最长匹配挂载点（正确处理 / 与 /data/nvme 等嵌套挂载）。
	for _, partition := range partitions {
		if isPathOnDevice(targetPath, partition.Mountpoint) {
			cleanMount := filepath.Clean(partition.Mountpoint)
			if len(cleanMount) > bestLen {
				bestLen = len(cleanMount)
				targetDevice = partition.Device
			}
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

	// 确保路径已标准化并以目录边界匹配（防止 /data 误匹配 /data2）。
	absPath = filepath.Clean(absPath)
	absMountpoint = filepath.Clean(absMountpoint)
	if absPath == absMountpoint {
		return true
	}
	sep := string(filepath.Separator)
	if !strings.HasSuffix(absMountpoint, sep) {
		absMountpoint += sep
	}
	return strings.HasPrefix(absPath, absMountpoint)
}
