package knocidb

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/knoci/knocidb/wal"
)

// LogRecordType 是日志记录的类型
type LogRecordType = byte

const (
	// LogRecordNormal 是普通日志记录类型
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted 是已删除的日志记录类型
	LogRecordDeleted
	// LogRecordBatchFinished 是批处理完成的日志记录类型
	LogRecordBatchFinished
)

// 类型 批次ID 键大小 值大小
//
//	1  +  10  +   5   +   5 = 21
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + binary.MaxVarintLen64 + 1

// LogRecord 是键值对的日志记录
type LogRecord struct {
	Key     []byte
	Value   []byte
	Type    LogRecordType
	BatchID uint64
}

// +-------------+-------------+-------------+--------------+-------------+--------------+
// |    type     |  batch id   |   key size  |   value size |      key    |      value   |
// +-------------+-------------+-------------+--------------+-------------+--------------+
//
//	1 byte	      varint(max 10) varint(max 5)  varint(max 5)     varint		varint
func encodeLogRecord(logRecord *LogRecord) []byte {
	header := make([]byte, maxLogRecordHeaderSize)

	header[0] = logRecord.Type
	var index = 1

	// 批次ID
	index += binary.PutUvarint(header[index:], logRecord.BatchID)
	// 键大小
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// 值大小
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// 复制头部
	copy(encBytes[:index], header[:index])
	// 复制键
	copy(encBytes[index:], logRecord.Key)
	// 复制值
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	return encBytes
}

// decodeLogRecord 从给定的字节切片解码日志记录
func decodeLogRecord(buf []byte) *LogRecord {
	recordType := buf[0]

	var index uint32 = 1
	// 批次ID
	batchID, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// 键大小
	keySize, n := binary.Varint(buf[index:])
	index += uint32(n)

	// 值大小
	valueSize, n := binary.Varint(buf[index:])
	index += uint32(n)

	// 复制键
	key := make([]byte, keySize)
	copy(key, buf[index:index+uint32(keySize)])
	index += uint32(keySize)

	// 复制值
	value := make([]byte, valueSize)
	copy(value, buf[index:index+uint32(valueSize)])

	return &LogRecord{
		Key:     key,
		Value:   value,
		BatchID: batchID,
		Type:    recordType,
	}
}

// KeyPosition 是键在值日志中的位置
type KeyPosition struct {
	key       []byte
	partition uint32
	uid       uuid.UUID
	position  *wal.ChunkPosition
}

// ValueLogRecord 是值日志中键值对的记录
type ValueLogRecord struct {
	uid   uuid.UUID
	key   []byte
	value []byte
}

func encodeValueLogRecord(record *ValueLogRecord) []byte {
	keySize := 4
	index := 0
	uidBytes, _ := record.uid.MarshalBinary()
	buf := make([]byte, len(uidBytes)+4+len(record.key)+len(record.value))

	copy(buf[index:], uidBytes)
	index += len(uidBytes)
	// 小端序
	binary.LittleEndian.PutUint32(buf[index:index+keySize], uint32(len(record.key)))
	index += keySize

	copy(buf[index:index+len(record.key)], record.key)
	index += len(record.key)

	copy(buf[index:], record.value)
	return buf
}

func decodeValueLogRecord(buf []byte) *ValueLogRecord {
	keySize := 4
	index := 0
	var uid uuid.UUID
	uidBytes := buf[:len(uid)]
	err := uid.UnmarshalBinary(uidBytes)
	if err != nil {
		return nil
	}
	index += len(uid)

	keyLen := (int)(binary.LittleEndian.Uint32(buf[index : index+keySize]))
	index += keySize

	key := make([]byte, keyLen)
	copy(key, buf[index:index+keyLen])
	index += keyLen

	value := make([]byte, len(buf)-len(uid)-keyLen-keySize)
	copy(value, buf[index:])

	return &ValueLogRecord{uid: uid, key: key, value: value}
}
