//go:build unix

package web

import (
	"fmt"
	"os"
	"path/filepath"

	"syscall"
)

// A user-wide lock prevents different configs/ports from bypassing coordination.
// The state lock also protects shared checkpoints used by different users.
func acquireProcessLock(statePath string) (func(), error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	directory := filepath.Join(cache, "estui")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	var files []*os.File
	release := func() {
		for _, f := range files {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			_ = f.Close()
		}
	}
	for _, path := range []string{filepath.Join(directory, "process.lock"), statePath + ".lock"} {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			release()
			return nil, err
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			_ = f.Close()
			release()
			return nil, fmt.Errorf("another estui process holds %s", path)
		}
		files = append(files, f)
	}
	return release, nil
}
