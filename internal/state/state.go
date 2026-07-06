// Package state owns the on-disk layout under
// <state_dir>/workers/<POOL>/<TASK_ID>/. The orchestrator and the workers
// share this directory: each worker writes its lifecycle files into its
// own pool subdir, the orchestrator scans them to reap finished work and
// recover after a restart. Pools partition different worker classes
// (implement / review / finalize) so they can't collide on identifier.
package state

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Status values written to <task_dir>/status.
const (
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

// Pool names. The set is fixed; workers and the orchestrator both refer to
// these constants so a typo is a compile error, not a silently missing dir.
const (
	PoolImplement = "implement"
	PoolReview    = "review"
	PoolNotify    = "notify"
	PoolFinalize  = "finalize"
)

// Dir is a pool-scoped view of the state directory; one Dir per pool.
// Root is the process-shared base (where the orchestrator's lock lives);
// Pool is the subdirectory under <root>/workers/ that this Dir owns.
type Dir struct {
	Root string
	Pool string
}

// New creates the pool subdir (and intermediate dirs) and returns a Dir.
func New(root, pool string) (*Dir, error) {
	if pool == "" {
		return nil, fmt.Errorf("state.New: pool is required")
	}
	if err := os.MkdirAll(filepath.Join(root, "workers", pool), 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &Dir{Root: root, Pool: pool}, nil
}

// LockPath returns the orchestrator's single-instance lockfile location.
// The lock is process-level, shared across pools — there's only one
// orchestrator regardless of how many pools it manages.
func LockPath(root string) string {
	return filepath.Join(root, "brahma.lock")
}

// DefaultStateDirName is the state-dir basename used when a config sets
// none. Shared by the orchestrator (brahma) and the monitor (chitra) so
// their defaults can't drift apart.
const DefaultStateDirName = "brahmanda"

// DefaultStateDir returns the state dir used when the config sets none:
// $XDG_STATE_HOME/brahmanda, or ~/.local/state/brahmanda when unset.
func DefaultStateDir() (string, error) {
	dir := os.Getenv("XDG_STATE_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		dir = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(dir, DefaultStateDirName), nil
}

// ClaudeErrorMarkerPath is the file workers touch when claudex.Run
// returns any error (rate limit, API unavailability, connection
// failure, skill couldn't complete, etc.). The orchestrator checks for
// it at the top of every tick; presence means "back off one step"
// (double tick_interval, capped at a 30m ceiling). Shared across pools,
// like the lock — any worker's claude error triggers the backoff for
// everything.
func ClaudeErrorMarkerPath(root string) string {
	return filepath.Join(root, "claude-error")
}

// WorkerDir returns the directory for a task without creating it.
func (d *Dir) WorkerDir(taskID string) string {
	return filepath.Join(d.Root, "workers", d.Pool, taskID)
}

// CreateWorker initializes a worker's state directory with pid+started-at+status.
// Called by the worker on startup.
func (d *Dir) CreateWorker(taskID string, pid int) error {
	dir := d.WorkerDir(taskID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create worker dir: %w", err)
	}
	files := map[string]string{
		"pid":        strconv.Itoa(pid),
		"started-at": time.Now().UTC().Format(time.RFC3339),
		"status":     StatusRunning,
	}
	for name, val := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(val+"\n"), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}

// SetStatus updates the status file for a running or finished worker.
func (d *Dir) SetStatus(taskID, status string) error {
	path := filepath.Join(d.WorkerDir(taskID), "status")
	return os.WriteFile(path, []byte(status+"\n"), 0o644)
}

// WriteWorktreePath records where the worker's worktree lives. Called by
// the worker as soon as its worktree is ready; the orchestrator reads
// this on reap to locate the .agent/ folder for archiving.
func (d *Dir) WriteWorktreePath(taskID, path string) error {
	dst := filepath.Join(d.WorkerDir(taskID), "worktree")
	return os.WriteFile(dst, []byte(path+"\n"), 0o644)
}

// Worker is a snapshot of one entry in <state_dir>/workers/.
type Worker struct {
	TaskID       string
	PID          int
	StartedAt    time.Time
	Status       string
	WorktreePath string // empty if the worker died before its worktree was ready
}

// List returns every worker subdir in this pool, regardless of status.
// Caller decides which to act on (e.g. reap finished, hard-kill timed-out, etc.).
func (d *Dir) List() ([]Worker, error) {
	root := filepath.Join(d.Root, "workers", d.Pool)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read workers dir: %w", err)
	}
	var out []Worker
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		w, err := d.readWorker(e.Name())
		if err != nil {
			// Skip half-written dirs rather than blocking the tick on one bad entry.
			continue
		}
		out = append(out, w)
	}
	return out, nil
}

func (d *Dir) readWorker(taskID string) (Worker, error) {
	dir := d.WorkerDir(taskID)
	pidRaw, err := os.ReadFile(filepath.Join(dir, "pid"))
	if err != nil {
		return Worker{}, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidRaw)))
	if err != nil {
		return Worker{}, fmt.Errorf("parse pid: %w", err)
	}
	startedRaw, err := os.ReadFile(filepath.Join(dir, "started-at"))
	if err != nil {
		return Worker{}, err
	}
	startedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(string(startedRaw)))
	if err != nil {
		return Worker{}, fmt.Errorf("parse started-at: %w", err)
	}
	statusRaw, err := os.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return Worker{}, err
	}
	worktree := ""
	if raw, err := os.ReadFile(filepath.Join(dir, "worktree")); err == nil {
		worktree = strings.TrimSpace(string(raw))
	}
	return Worker{
		TaskID:       taskID,
		PID:          pid,
		StartedAt:    startedAt,
		Status:       strings.TrimSpace(string(statusRaw)),
		WorktreePath: worktree,
	}, nil
}

// Remove deletes a worker's state dir. Called after the orchestrator has
// reaped a finished worker and recorded the outcome.
func (d *Dir) Remove(taskID string) error {
	return os.RemoveAll(d.WorkerDir(taskID))
}

// PIDAlive returns true if the given PID still exists. kill(pid, 0) is the
// POSIX way to probe without sending a signal.
func PIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
