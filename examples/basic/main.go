package main

import (
	"fmt"
	"time"

	"github.com/knoci/knocidb"
)

// this file shows how to use the basic operations of knociDB
func main() {
	options := knocidb.NewDefaultOptions("/tmp/knocidb_basic")

	// open database
	db, err := knocidb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// put a key
	if err := db.Put([]byte("name"), []byte("knocidb")); err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("name:", string(val))

	// put a key with embedded-metadata TTL
	if err := db.PutWithTTL([]byte("session"), []byte("alive"), 30*time.Second); err != nil {
		panic(err)
	}
	if ttl, ok, err := db.GetTTL([]byte("session")); err != nil {
		panic(err)
	} else if ok {
		fmt.Println("session ttl:", ttl)
	}

	// remove TTL so the key persists
	if persisted, err := db.Persist([]byte("session")); err != nil {
		panic(err)
	} else {
		fmt.Println("session persisted:", persisted)
	}

	// inspect live engine stats
	stats := db.Stats()
	fmt.Printf("stats: shards=%d memBytes=%d seq=%d\n", stats.Shards, stats.ActiveMemtableBytes, stats.Sequence)

	// delete a key
	if err := db.Delete([]byte("name")); err != nil {
		panic(err)
	}
}
