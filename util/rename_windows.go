package util

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// RenameDurable moves a file or directory with write-through on Windows.
func RenameDurable(source, target string) error {
	from, err := windowsSegmentPath(source)
	if err != nil {
		return err
	}
	to, err := windowsSegmentPath(target)
	if err != nil {
		return err
	}
	err = windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: source, New: target, Err: err}
	}
	return nil
}

// SyncDir is covered by write-through renames on Windows.
func SyncDir(string) error {
	return nil
}

func windowsSegmentPath(path string) (*uint16, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(path, `\\?\`) && !strings.HasPrefix(path, `\\.\`) {
		// Use the extended-length spelling for both drive and UNC paths.
		if tail, ok := strings.CutPrefix(path, `\\`); ok {
			path = `\\?\UNC\` + tail
		} else {
			path = `\\?\` + path
		}
	}
	return windows.UTF16PtrFromString(path)
}
