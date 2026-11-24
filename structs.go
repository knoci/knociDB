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
	// LogRecordNormalEmpty 是 value 为空但键存在的普通记录
	LogRecordNormalEmpty

	// logRecordHasTTL 标志位：置位时头部紧跟 uvarint(ExpireAt UnixMilli)，兼容无 TTL 的旧格式 WAL。
	logRecordHasTTL LogRecordType = 0x80
)

// 类型(1) + 批次ID(10) + 键大小(5) + 值大小(5) + 可选过期时间(10) = 31
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + binary.MaxVarintLen64*2 + 1

// LogRecord 是键值对的日志记录
type LogRecord struct {
	Key      []byte
	Value    []byte
	Type     LogRecordType
	BatchID  uint64
	ExpireAt int64 // UnixMilli；0 表示无过期时间
}

// +-------------+-------------+-------------+--------------+--------------------+-------------+--------------+
// |    type     |  batch id   |   key size  |   value size | [expireAt if flag] |      key    |      value   |
// +-------------+-------------+-------------+--------------+--------------------+-------------+--------------+
// appendEncodedLogRecord 将 LogRecord 编码追加到 dst 切片中并返回结果切片，避免热路径逐条分配临时切片。
func appendEncodedLogRecord(dst []byte, logRecord *LogRecord) []byte {
	var header [maxLogRecordHeaderSize]byte

	recType := logRecord.Type &^ logRecordHasTTL
	if logRecord.ExpireAt > 0 {
		recType |= logRecordHasTTL
	}
	header[0] = recType
	var index = 1

	// 批次ID
	index += binary.PutUvarint(header[index:], logRecord.BatchID)
	// 键大小
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// 值大小
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	if logRecord.ExpireAt > 0 {
		index += binary.PutUvarint(header[index:], uint64(logRecord.ExpireAt))
	}

	dst = append(dst, header[:index]...)
	dst = append(dst, logRecord.Key...)
	dst = append(dst, logRecord.Value...)
	return dst
}

func encodeLogRecord(logRecord *LogRecord) []byte {
	size := maxLogRecordHeaderSize + len(logRecord.Key) + len(logRecord.Value)
	return appendEncodedLogRecord(make([]byte, 0, size), logRecord)
}

// decodeLogRecord 从给定的字节切片解码日志记录
func decodeLogRecord(buf []byte) *LogRecord {
	if len(buf) == 0 {
		return nil
	}
	hasTTL := (buf[0] & logRecordHasTTL) != 0
	recordType := buf[0] &^ logRecordHasTTL

	var index uint32 = 1
	// 批次ID
	batchID, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil
	}
	index += uint32(n)

	// 键大小
	keySize, n := binary.Varint(buf[index:])
	if n <= 0 || keySize < 0 {
		return nil
	}
	index += uint32(n)

	// 值大小
	valueSize, n := binary.Varint(buf[index:])
	if n <= 0 || valueSize < 0 {
		return nil
	}
	index += uint32(n)

	var expireAt int64
	if hasTTL {
		exp, m := binary.Uvarint(buf[index:])
		if m <= 0 {
			return nil
		}
		expireAt = int64(exp)
		index += uint32(m)
	}

	if index+uint32(keySize)+uint32(valueSize) < index || index+uint32(keySize)+uint32(valueSize) > uint32(len(buf)) {
		return nil
	}

	// 复制键
	key := make([]byte, keySize)
	copy(key, buf[index:index+uint32(keySize)])
	index += uint32(keySize)

	// 复制值
	value := make([]byte, valueSize)
	copy(value, buf[index:index+uint32(valueSize)])

	return &LogRecord{
		Key:      key,
		Value:    value,
		BatchID:  batchID,
		Type:     recordType,
		ExpireAt: expireAt,
	}
}

// KeyPosition 是键在值日志中的位置与过期元数据
type KeyPosition struct {
	key       []byte
	partition uint32
	uid       uuid.UUID
	position  *wal.ChunkPosition
	expireAt  int64 // UnixMilli；0 表示无过期时间
}

// ValueLogRecord 是值日志中键值对的记录
type ValueLogRecord struct {
	uid      uuid.UUID
	key      []byte
	value    []byte
	expireAt int64 // 仅内存态透传至 KeyPosition，不改变 VLog 磁盘格式
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
	const uidLen = 16
	const keySize = 4
	if len(buf) < uidLen+keySize {
		return nil
	}
	index := 0
	var uid uuid.UUID
	if err := uid.UnmarshalBinary(buf[:uidLen]); err != nil {
		return nil
	}
	index += uidLen

	keyLen := int(binary.LittleEndian.Uint32(buf[index : index+keySize]))
	index += keySize
	if index+keyLen > len(buf) {
		return nil
	}

	key := make([]byte, keyLen)
	copy(key, buf[index:index+keyLen])
	index += keyLen

	value := make([]byte, len(buf)-index)
	copy(value, buf[index:])

	return &ValueLogRecord{uid: uid, key: key, value: value}
}
