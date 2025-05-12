package main

import (
	"fmt"
	"strings"

	"knocidb"
)

// BatchClient 封装了批处理操作的功能
type BatchClient struct {
	client *Client
	batch  *knocidb.Batch
	queued []string // 用于记录已排队的命令，便于显示
}

// NewBatchClient 创建一个新的批处理客户端
func NewBatchClient(client *Client) *BatchClient {
	return &BatchClient{
		client: client,
		queued: make([]string, 0),
	}
}

// Begin 开始一个新的批处理
func (bc *BatchClient) Begin() (string, error) {
	if bc.batch != nil {
		return "", fmt.Errorf("a batch alredy exsit")
	}

	// 创建一个新的批处理
	bc.batch = bc.client.db.NewBatch(knocidb.DefaultBatchOptions)
	bc.queued = make([]string, 0)

	return "Batch start", nil
}

// Queue 将命令加入队列
func (bc *BatchClient) Queue(cmd string, args []string) (string, error) {
	if bc.batch == nil {
		return "", fmt.Errorf("no batch in used")
	}

	// 记录命令
	cmdStr := cmd
	for _, arg := range args {
		cmdStr += " " + arg
	}
	bc.queued = append(bc.queued, cmdStr)

	// 根据命令类型执行相应的操作
	switch strings.ToUpper(cmd) {
	case "GET":
		// GET 命令在批处理中只是记录，不执行
		return "QUEUED", nil

	case "PUT":
		if len(args) != 2 {
			return "", fmt.Errorf("PUT require two args: SET key value")
		}
		key := []byte(args[0])
		value := []byte(args[1])
		err := bc.batch.Put(key, value)
		if err != nil {
			return "", err
		}
		return "QUEUED", nil

	case "DEL":
		if len(args) < 1 {
			return "", fmt.Errorf("DEL require at least one arg: DEL key [key ...]")
		}
		for _, arg := range args {
			key := []byte(arg)
			err := bc.batch.Delete(key)
			if err != nil {
				return "", err
			}
		}
		return "QUEUED", nil

	default:
		return "", fmt.Errorf("unsupported commands in batch processing: %s", cmd)
	}
}

// Commit 执行批处理
func (bc *BatchClient) Commit() (string, error) {
	if bc.batch == nil {
		return "", fmt.Errorf("no batch in used")
	}

	// 提交批处理
	err := bc.batch.Commit()
	if err != nil {
		return "", err
	}

	// 构建结果
	result := fmt.Sprintf("exec %d cmds:\n", len(bc.queued))
	for i, cmd := range bc.queued {
		result += fmt.Sprintf("%d) %s\n", i+1, cmd)
	}
	result += "OK"

	// 清理批处理状态
	bc.batch = nil
	bc.queued = nil

	return result, nil
}

// Discard 丢弃当前批处理
func (bc *BatchClient) Discard() (string, error) {
	if bc.batch == nil {
		return "", fmt.Errorf("no batch in used")
	}
	// 清理批处理状态
	bc.batch = nil
	bc.queued = nil
	return "DISCARD", nil
}

// IsInBatch 检查是否有批处理在进行中
func (bc *BatchClient) IsInBatch() bool {
	return bc.batch != nil
}

// GetQueuedCommands 获取已排队的命令列表
func (bc *BatchClient) GetQueuedCommands() []string {
	return bc.queued
}
