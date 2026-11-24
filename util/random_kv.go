package util

import (
	"fmt"
	"math/rand/v2"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GetTestKey(i int64) []byte {
	return []byte(fmt.Sprintf("knocidb-test-key-%09d", i))
}

func RandomValue(n int) []byte {
	const prefix = "knocidb-test-value-"
	buf := make([]byte, len(prefix)+n)
	copy(buf, prefix)
	for i := 0; i < n; i++ {
		buf[len(prefix)+i] = letters[rand.IntN(len(letters))]
	}
	return buf
}
