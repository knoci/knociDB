package main

import (
	"bytes"
	"fmt"

	"github.com/knoci/knocidb"
)

func main() {
	options := knocidb.NewDefaultOptions("/tmp/knocidb_iter")

	// 打开数据库
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// 写入示例数据
	for i := 0; i < 100; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key %02d", i)), []byte(fmt.Sprintf("key %02d", i))); err != nil {
			panic(err)
		}
		if err := db.Put([]byte(fmt.Sprintf("abc %02d", i)), []byte(fmt.Sprintf("abc %02d", i))); err != nil {
			panic(err)
		}
	}

	// 1. 正向全量迭代
	iter, err := db.NewIterator(knocidb.IteratorOptions{Reverse: false})
	if err != nil {
		panic(err)
	}
	// 创建迭代器后并发写入的新键值不会污染当前 MVCC 快照视图，也不会触发 memtable 冻结轮转
	if err := db.Put([]byte("key 999-after-snap"), []byte("invisible-in-open-iter")); err != nil {
		panic(err)
	}
	for iter.Rewind(); iter.Valid(); iter.Next() {
		if string(iter.Key()) == "key 999-after-snap" {
			panic("mvcc snapshot isolation violated")
		}
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
	if err := iter.Close(); err != nil {
		panic(err)
	}

	// 2. 反向全量迭代
	iter, err = db.NewIterator(knocidb.IteratorOptions{Reverse: true})
	if err != nil {
		panic(err)
	}
	count := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
	if err := iter.Close(); err != nil {
		panic(err)
	}
	fmt.Println("reverse scan count:", count)

	// 3. O(log N) 前缀 Seek 扫描
	iter, err = db.NewIterator(knocidb.IteratorOptions{Reverse: false, Prefix: []byte("abc ")})
	if err != nil {
		panic(err)
	}
	iter.Seek([]byte("abc 91"))
	for iter.Valid() {
		if !bytes.HasPrefix(iter.Key(), []byte("abc ")) {
			panic("prefix wrong")
		}
		fmt.Println(string(iter.Key()), string(iter.Value()))
		iter.Next()
	}
	if err := iter.Err(); err != nil {
		panic(err)
	}
	if err := iter.Close(); err != nil {
		panic(err)
	}
}
