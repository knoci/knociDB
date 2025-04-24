package benchmark

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"knocidb"
	"knocidb/util"
	"os"
	"testing"
)

var db *knocidb.DB

func openDB() func() {
	options := knocidb.DefaultOptions
	options.DirPath = "/tmp/knocidb-bench"

	var err error
	db, err = knocidb.Open(options)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPut(b *testing.B) {
	destroy := openDB()
	defer destroy()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := db.Put(util.GetTestKey(int64(i)), util.RandomValue(1024))
		//nolint:testifylint // benchmark
		assert.Nil(b, err)
	}
}

func BenchmarkGet(b *testing.B) {
	destroy := openDB()
	defer destroy()
	for i := 0; i < 1000000; i++ {
		err := db.Put(util.GetTestKey(int64(i)), util.RandomValue(128))
		assert.Nil(b, err)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		val, err := db.Get(util.GetTestKey(int64(i)))
		if err == nil {
			assert.NotNil(b, val)
		} else if errors.Is(err, knocidb.ErrKeyNotFound) {
			b.Error(err)
		}
	}
}
