package lock

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// A stale lockfile from a prior holder with a longer PID must not leave
// trailing bytes behind the new (shorter) PID — the reader TrimSpaces the
// content, so a leftover digit would surface as a garbled PID.
func TestAcquireTruncatesStalePID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "brahma.lock")
	if err := os.WriteFile(path, []byte("999999999999\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.Itoa(os.Getpid()) + "\n"
	if string(raw) != want {
		t.Errorf("lockfile = %q, want %q (stale bytes not truncated)", string(raw), want)
	}
}
