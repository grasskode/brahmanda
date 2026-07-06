// Package lock provides a single-instance file lock using flock(2).
// The lock is held for the lifetime of the process; the kernel releases it
// automatically on exit, including crashes.
package lock

import (
	"fmt"
	"os"
	"syscall"
)

// File is a held flock. Release with Close.
type File struct {
	f *os.File
}

// Acquire opens path (creating it if needed) and takes an exclusive,
// non-blocking flock on it. Returns ErrHeld if another process already
// holds the lock.
func Acquire(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lockfile %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, ErrHeld
		}
		return nil, fmt.Errorf("flock %s: %w", path, err)
	}
	// Truncate before writing: the file may carry a prior holder's PID,
	// and a shorter new PID would otherwise leave its trailing bytes in
	// place (e.g. a low post-reboot PID over a stale longer one).
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, fmt.Errorf("truncate lockfile %s: %w", path, err)
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		f.Close()
		return nil, fmt.Errorf("write pid: %w", err)
	}
	return &File{f: f}, nil
}

// Close releases the flock and closes the file. Safe to call once.
func (l *File) Close() error {
	if l.f == nil {
		return nil
	}
	syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}

// ErrHeld is returned by Acquire when another process holds the lock.
var ErrHeld = fmt.Errorf("lock already held by another process")

// IsHeld reports whether some other process currently holds the lock
// at path. Returns false if the lockfile doesn't exist OR exists but
// has been released (the file persists after kernel-released locks,
// so a plain os.Stat is not enough). Implemented by attempting a
// non-blocking exclusive flock; we never hold the lock — we release
// it immediately if we got it. Used by introspection tools (chitra).
func IsHeld(path string) bool {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err == syscall.EWOULDBLOCK
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}
