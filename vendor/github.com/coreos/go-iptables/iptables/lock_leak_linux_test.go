//go:build linux

package iptables

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestContendedLockClosesDescriptorOnUnlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "xtables.lock")
	held, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(held.Fd()), syscall.LOCK_UN)
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustClose := true
	defer func() {
		if mustClose {
			_ = syscall.Close(fd)
		}
	}()
	unlocker, err := (&fileLock{fd: fd}).tryLock()
	if err != nil {
		t.Fatal(err)
	}
	if err := unlocker.Unlock(); err != nil {
		t.Fatal(err)
	}
	mustClose = false
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("contended lock fd remains open: %v", err)
	}
}
