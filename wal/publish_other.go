//go:build !windows

package wal

import "os"

func publishSegmentFile(source, target string) error {
	return os.Rename(source, target)
}
