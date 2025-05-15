package main

import (
	"github.com/knoci/knocidb"
)

// this file shows how to use the basic operations of LotusDB
func main() {
	options := knocidb.DefaultOptions
	options.DirPath = "/tmp/knocidb_basic"

	// open database
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// put a key
	err = db.Put([]byte("name"), []byte("knocidb"))
	if err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
