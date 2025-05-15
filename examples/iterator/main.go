package main

import (
	"bytes"
	"fmt"
	"github.com/knoci/knocidb"
)

func main() {

	options := knocidb.DefaultOptions
	options.DirPath = "/tmp/knocidb_iter"

	// 打开数据库
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// 放入数据
	for i := 0; i < 100; i++ {
		err = db.Put([]byte(fmt.Sprintf("key %d", i)), []byte(fmt.Sprintf("key %d", i)))
		if err != nil {
			panic(err)
		}
		err = db.Put([]byte(fmt.Sprintf("abc %d", i)), []byte(fmt.Sprintf("abc %d", i)))
		if err != nil {
			panic(err)
		}
	}
	iter, err := db.NewIterator(knocidb.IteratorOptions{Reverse: false})
	if err != nil {
		panic(err)
	}
	for iter.Valid() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
		iter.Next()
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}

	iter, err = db.NewIterator(knocidb.IteratorOptions{Reverse: true})
	if err != nil {
		panic(err)
	}
	for iter.Valid() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
		iter.Next()
	}
	err = iter.Close()
	if err != nil {
		panic(err)
	}
	iter, err = db.NewIterator(knocidb.IteratorOptions{Reverse: false, Prefix: []byte("abc")})
	if err != nil {
		panic(err)
	}
	iter.Seek([]byte("abc 91"))
	i := 0
	for iter.Valid() {
		if bytes.Compare(iter.Key(), []byte("abc 30")) == -1 {
			panic("seek wrong")
		}
		fmt.Println(string(iter.Key()), string(iter.Value()))
		if !bytes.HasPrefix(iter.Key(), []byte("abc")) {
			panic("prefix wrong.")
		}
		i++
		if i > 100 {
			break
		}
		iter.Next()
	}

	err = iter.Close()
	if err != nil {
		panic(err)
	}

}
