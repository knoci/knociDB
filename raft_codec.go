package knocidb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	// raftBinaryMagic 标识紧凑二进制 Raft 命令格式（无 JSON/Base64 膨胀）；
	// 旧版 JSON 载荷以 '{' (0x7B) 开头，解码端自动兼容双格式以支持平滑滚动升级。
	raftBinaryMagic byte = 0xA1
)

// raftLookupKey 用于 ClusterDB 本地 SyncRead/ReadLocalNode 零拷贝直传查询键，避免 JSON 编解码。
type raftLookupKey []byte

func encodeRaftCommand(cmd Command) []byte {
	// 估算容量：magic(1) + op(1) + batchID(10) + expireAt(10) + keyLen(5) + valLen(5) + key + val
	buf := make([]byte, 0, 32+len(cmd.Key)+len(cmd.Value))
	buf = append(buf, raftBinaryMagic, byte(cmd.Op))
	buf = binary.AppendUvarint(buf, cmd.BatchID)
	buf = binary.AppendVarint(buf, cmd.ExpireAt)
	buf = binary.AppendUvarint(buf, uint64(len(cmd.Key)))
	buf = append(buf, cmd.Key...)
	buf = binary.AppendUvarint(buf, uint64(len(cmd.Value)))
	buf = append(buf, cmd.Value...)
	return buf
}

func encodeRaftBatchCommand(batch BatchCommand) []byte {
	total := 24
	for _, c := range batch.Commands {
		total += 22 + len(c.Key) + len(c.Value)
	}
	buf := make([]byte, 0, total)
	buf = append(buf, raftBinaryMagic, byte(OpBatch))
	buf = binary.AppendUvarint(buf, batch.BatchID)
	buf = binary.AppendUvarint(buf, uint64(len(batch.Commands)))
	for _, c := range batch.Commands {
		buf = append(buf, byte(c.Op))
		buf = binary.AppendVarint(buf, c.ExpireAt)
		buf = binary.AppendUvarint(buf, uint64(len(c.Key)))
		buf = append(buf, c.Key...)
		buf = binary.AppendUvarint(buf, uint64(len(c.Value)))
		buf = append(buf, c.Value...)
	}
	return buf
}

func decodeRaftWriteCommand(data []byte) (raftWriteCommand, error) {
	var out raftWriteCommand
	if len(data) == 0 {
		return out, ErrInvalidRaftCommand
	}
	if data[0] != raftBinaryMagic {
		if err := json.Unmarshal(data, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	if len(data) < 2 {
		return out, ErrInvalidRaftCommand
	}
	op := uint32(data[1])
	pos := 2
	readUvarint := func() (uint64, error) {
		v, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return 0, ErrInvalidRaftCommand
		}
		pos += n
		return v, nil
	}
	readVarint := func() (int64, error) {
		v, n := binary.Varint(data[pos:])
		if n <= 0 {
			return 0, ErrInvalidRaftCommand
		}
		pos += n
		return v, nil
	}
	readBytes := func() ([]byte, error) {
		n, err := readUvarint()
		if err != nil || n > uint64(len(data)-pos) {
			return nil, ErrInvalidRaftCommand
		}
		b := make([]byte, n)
		copy(b, data[pos:pos+int(n)])
		pos += int(n)
		return b, nil
	}

	switch op {
	case OpPut, OpDelete:
		batchID, err := readUvarint()
		if err != nil {
			return out, err
		}
		expireAt, err := readVarint()
		if err != nil {
			return out, err
		}
		key, err := readBytes()
		if err != nil {
			return out, err
		}
		val, err := readBytes()
		if err != nil {
			return out, err
		}
		out.Command = Command{
			Op:       op,
			Key:      key,
			Value:    val,
			BatchID:  batchID,
			ExpireAt: expireAt,
		}
		return out, nil
	case OpBatch:
		batchID, err := readUvarint()
		if err != nil {
			return out, err
		}
		count, err := readUvarint()
		if err != nil || count > 1<<20 {
			return out, ErrInvalidRaftCommand
		}
		cmds := make([]Command, 0, count)
		for i := uint64(0); i < count; i++ {
			if pos >= len(data) {
				return out, ErrInvalidRaftCommand
			}
			subOp := uint32(data[pos])
			pos++
			expireAt, err := readVarint()
			if err != nil {
				return out, err
			}
			key, err := readBytes()
			if err != nil {
				return out, err
			}
			val, err := readBytes()
			if err != nil {
				return out, err
			}
			cmds = append(cmds, Command{
				Op:       subOp,
				Key:      key,
				Value:    val,
				ExpireAt: expireAt,
			})
		}
		out.Op = OpBatch
		out.BatchID = batchID
		out.Commands = cmds
		return out, nil
	default:
		return out, fmt.Errorf("%w: unknown binary op %d", ErrInvalidRaftCommand, op)
	}
}
