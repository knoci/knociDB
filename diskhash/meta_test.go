package diskhash

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMetaPersistedAfterSplit(t *testing.T) {
	dir := t.TempDir()
	opt := DefaultOptions
	opt.DirPath = dir
	opt.SlotValueLength = 8
	opt.LoadFactor = 0.5

	table, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	match := func(Slot) (bool, error) { return false, nil }
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := make([]byte, 8)
		if err := table.Put(key, value, match); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// 不调用 Close，直接读磁盘上的 meta；分裂后必须已经更新。
	file, err := os.Open(filepath.Join(dir, metaFileName))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var meta tableMeta
	if err := json.NewDecoder(file).Decode(&meta); err != nil {
		t.Fatalf("meta json: %v", err)
	}
	if meta.NumBuckets <= 1 {
		t.Fatalf("meta not updated after split: numBuckets=%d", meta.NumBuckets)
	}
}
