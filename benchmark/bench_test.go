package benchmark

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/knoci/knocidb"
	"github.com/knoci/knocidb/util"
)

func openBenchDB(b *testing.B, syncWrite bool) *knocidb.DB {
	b.Helper()
	opts := knocidb.DefaultOptions
	opts.DirPath = b.TempDir()
	opts.Sync = syncWrite
	opts.AutoCompactSupport = false
	opts.TTLScanInterval = 0
	db, err := knocidb.Open(opts)
	if err != nil {
		b.Fatalf("open bench db: %v", err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func pregenKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = util.GetTestKey(int64(i))
	}
	return keys
}

func BenchmarkPut(b *testing.B) {
	db := openBenchDB(b, false)
	const poolSize = 65536
	keys := pregenKeys(poolSize)
	val := bytes.Repeat([]byte("v"), 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(keys[i%poolSize], val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSync(b *testing.B) {
	db := openBenchDB(b, true)
	const poolSize = 4096
	keys := pregenKeys(poolSize)
	val := bytes.Repeat([]byte("s"), 256)
	wopts := knocidb.WriteOptions{Sync: true}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var seq atomic.Int64
		for pb.Next() {
			idx := int(seq.Add(1)-1) % poolSize
			if err := db.PutWithOptions(keys[idx], val, wopts); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkBatch(b *testing.B) {
	db := openBenchDB(b, false)
	const batchSize = 100
	const poolSize = 10000
	keys := pregenKeys(poolSize)
	val := bytes.Repeat([]byte("b"), 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatch(knocidb.DefaultBatchOptions)
		base := (i * batchSize) % (poolSize - batchSize)
		for j := 0; j < batchSize; j++ {
			if err := batch.Put(keys[base+j], val); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db := openBenchDB(b, false)
	const numKeys = 20000
	keys := pregenKeys(numKeys)
	val := bytes.Repeat([]byte("g"), 256)
	for i := 0; i < numKeys; i++ {
		if err := db.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := db.Get(keys[i%numKeys])
		if err != nil {
			b.Fatal(err)
		}
		if len(got) != len(val) {
			b.Fatalf("unexpected value len: %d", len(got))
		}
	}
}

func BenchmarkScan(b *testing.B) {
	db := openBenchDB(b, false)
	const numKeys = 10000
	val := bytes.Repeat([]byte("x"), 64)
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("pfx-%02d-%06d", i%10, i))
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
	iterOpts := knocidb.IteratorOptions{Prefix: []byte("pfx-05-")}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := db.NewIterator(iterOpts)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			_ = it.Key()
			_ = it.Value()
			count++
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		_ = it.Close()
		if count == 0 {
			b.Fatal("expected non-empty prefix scan")
		}
	}
}

func BenchmarkParallelPutGet(b *testing.B) {
	db := openBenchDB(b, false)
	const poolSize = 16384
	keys := pregenKeys(poolSize)
	val := bytes.Repeat([]byte("p"), 256)
	for i := 0; i < poolSize; i++ {
		if err := db.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}

	var counter atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			k := keys[int(n)%poolSize]
			if n%4 == 0 {
				if err := db.Put(k, val); err != nil {
					b.Error(err)
					return
				}
			} else {
				if _, err := db.Get(k); err != nil {
					b.Error(err)
					return
				}
			}
		}
	})
}
