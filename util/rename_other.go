//go:build !windows

package util

import "os"

// RenameDurable moves a file or directory. Callers sync the affected directories.
func RenameDurable(source, target string) error {
	return os.Rename(source, target)
}

// SyncDir persists changes to a directory's entries.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
