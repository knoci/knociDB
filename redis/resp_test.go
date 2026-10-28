package redis

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestReadCommandInline(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("SET key value\r\n"))
	got, err := readCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || string(got[0]) != "SET" || string(got[1]) != "key" || string(got[2]) != "value" {
		t.Fatalf("unexpected command: %q", got)
	}

	if _, err := readCommand(bufio.NewReader(strings.NewReader("\r\n"))); err == nil {
		t.Fatal("empty inline command should fail")
	}
}

func TestReadCommandMultibulk(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	got, err := readCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0]) != "GET" || string(got[1]) != "k" {
		t.Fatalf("unexpected command: %q", got)
	}

	bad := []string{
		"*0\r\n",
		"*-1\r\n",
		"*x\r\n",
		"*1\r\n+PING\r\n",
		"*1\r\n$100\r\nx",
		"*1\r\n$1\r\nx\n",
	}
	for _, input := range bad {
		if _, err := readCommand(bufio.NewReader(strings.NewReader(input))); err == nil {
			t.Errorf("input %q should fail", input)
		}
	}
}

func TestReadIntLineInvalid(t *testing.T) {
	if _, err := readIntLine(bufio.NewReader(strings.NewReader("x\r\n"))); err == nil {
		t.Fatal("invalid integer should fail")
	}
	if _, err := readIntLine(bufio.NewReader(strings.NewReader(""))); err == nil {
		t.Fatal("missing line should fail")
	}
}

func TestWriteRESPTypes(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{nil, "$-1\r\n"},
		{errors.New("oops"), "-ERR oops\r\n"},
		{errf("WRONGTYPE bad"), "-WRONGTYPE bad\r\n"},
		{simpleString("OK"), "+OK\r\n"},
		{[]byte("x"), "$1\r\nx\r\n"},
		{"hello", "$5\r\nhello\r\n"},
		{int(3), ":3\r\n"},
		{int64(-2), ":-2\r\n"},
		{[]any{"a", int64(1)}, "*2\r\n$1\r\na\r\n:1\r\n"},
		{multiReply{"a", int64(2)}, "$1\r\na\r\n:2\r\n"},
		{[][]byte{[]byte("a"), []byte("b")}, "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		if err := writeRESP(w, tt.value); err != nil {
			t.Fatalf("writeRESP(%T): %v", tt.value, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if buf.String() != tt.want {
			t.Errorf("writeRESP(%T) = %q, want %q", tt.value, buf.String(), tt.want)
		}
	}

	var buf bytes.Buffer
	if err := writeRESP(bufio.NewWriter(&buf), struct{}{}); err == nil {
		t.Fatal("unsupported type should fail")
	}
}
