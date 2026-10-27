package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxBulkLength  = 64 << 20
	maxArrayLength = 1 << 20
	maxLineLength  = 1 << 20
)

type simpleString string

// multiReply 表示一次命令要写多个顶层 RESP 回复（SUBSCRIBE 等）。
type multiReply []any

type respError struct{ msg string }

func (e *respError) Error() string { return e.msg }

func errf(format string, args ...any) error {
	return &respError{msg: fmt.Sprintf(format, args...)}
}

func writeRESP(w *bufio.Writer, v any) error {
	switch value := v.(type) {
	case nil:
		_, err := w.WriteString("$-1\r\n")
		return err
	case error:
		return writeError(w, value)
	case simpleString:
		_, err := fmt.Fprintf(w, "+%s\r\n", string(value))
		return err
	case []byte:
		if _, err := fmt.Fprintf(w, "$%d\r\n", len(value)); err != nil {
			return err
		}
		if _, err := w.Write(value); err != nil {
			return err
		}
		_, err := w.WriteString("\r\n")
		return err
	case string:
		return writeRESP(w, []byte(value))
	case int:
		_, err := fmt.Fprintf(w, ":%d\r\n", value)
		return err
	case int64:
		_, err := fmt.Fprintf(w, ":%d\r\n", value)
		return err
	case multiReply:
		for _, item := range value {
			if err := writeRESP(w, item); err != nil {
				return err
			}
		}
		return nil
	case []any:
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(value)); err != nil {
			return err
		}
		for _, item := range value {
			if err := writeRESP(w, item); err != nil {
				return err
			}
		}
		return nil
	case [][]byte:
		out := make([]any, len(value))
		for i := range value {
			out[i] = value[i]
		}
		return writeRESP(w, out)
	default:
		return fmt.Errorf("redis: unsupported response type %T", v)
	}
}

func writeError(w *bufio.Writer, err error) error {
	msg := err.Error()
	prefixes := []string{"ERR ", "WRONGTYPE ", "NOAUTH ", "NOPROTO ", "EXECABORT ", "BUSY ", "MOVED ", "CROSSSLOT ", "CLUSTERDOWN ", "ASK "}
	for _, prefix := range prefixes {
		if strings.HasPrefix(msg, prefix) {
			_, werr := fmt.Fprintf(w, "-%s\r\n", msg)
			return werr
		}
	}
	_, werr := fmt.Fprintf(w, "-ERR %s\r\n", msg)
	return werr
}

func readCommand(r *bufio.Reader) ([][]byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if first != '*' {
		line, err := readLineWithFirst(r, first)
		if err != nil {
			return nil, err
		}
		fields := bytes.Fields(line)
		if len(fields) == 0 {
			return nil, errf("ERR empty command")
		}
		out := make([][]byte, len(fields))
		for i := range fields {
			out[i] = append([]byte(nil), fields[i]...)
		}
		return out, nil
	}

	n, err := readIntLine(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, errf("ERR empty command")
	}
	if n < 0 || n > maxArrayLength {
		return nil, errf("ERR invalid multibulk length")
	}
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		prefix, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if prefix != '$' {
			return nil, errf("ERR expected bulk string")
		}
		size, err := readIntLine(r)
		if err != nil {
			return nil, err
		}
		if size < 0 || size > maxBulkLength {
			return nil, errf("ERR invalid bulk length")
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if buf[size] != '\r' || buf[size+1] != '\n' {
			return nil, errf("ERR invalid bulk terminator")
		}
		out = append(out, buf[:size])
	}
	return out, nil
}

func readLineWithFirst(r *bufio.Reader, first byte) ([]byte, error) {
	line := []byte{first}
	for len(line) <= maxLineLength {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\n' {
			return bytes.TrimSuffix(line, []byte{'\r'}), nil
		}
		line = append(line, b)
	}
	return nil, errf("ERR line too long")
}

func readIntLine(r *bufio.Reader) (int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	n, err := strconv.Atoi(line)
	if err != nil {
		return 0, errf("ERR invalid integer")
	}
	return n, nil
}

var errProtocol = errors.New("redis protocol error")
