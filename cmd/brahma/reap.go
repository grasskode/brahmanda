package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/grasskode/bramha/internal/journal"
)

// reapOrphans reconciles the journal at startup. A worker that logged a
// pipeline-level `started` but no terminal event — because its runner was
// SIGKILLed (orchestrator restart, OOM, host crash) before reaching the
// terminal-journal write — would otherwise read as "in progress" forever,
// since the task-row label is a pure projection of the last event. The
// dead worker can't report its own death, so this survivor appends a
// `dead` event on its behalf.
//
// Liveness is decided by the per-worker pid file written in spawnWorker:
// a missing or dead pid means the worker is gone. A live pid whose /proc
// cmdline still names the worker is an orphan from a prior generation
// that is genuinely still running (its parent died by hard kill, so it
// was reparented to init rather than torn down) — left untouched so it
// can journal its own outcome and is never double-counted dead.
//
// Runs once before pools are seeded. Best-effort: an unreadable file or
// pid never aborts startup.
func reapOrphans(log *slog.Logger, stateDir string) {
	root := journal.Dir(stateDir)
	pools, err := os.ReadDir(root)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Warn("reaper: read journal dir", "err", err)
		}
		return
	}
	reaped := 0
	for _, pd := range pools {
		if !pd.IsDir() {
			continue
		}
		pool := pd.Name()
		files, err := os.ReadDir(filepath.Join(root, pool))
		if err != nil {
			log.Warn("reaper: read pool dir", "pool", pool, "err", err)
			continue
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".jsonl") {
				continue
			}
			workerID := strings.TrimSuffix(f.Name(), ".jsonl")
			path := filepath.Join(root, pool, f.Name())
			if reapWorker(log, stateDir, pool, workerID, path) {
				reaped++
			}
		}
	}
	if reaped > 0 {
		log.Info("reaper: reconciled orphaned workers", "count", reaped)
	}
}

// reapWorker appends a `dead` event for one worker if it started at the
// pipeline level, never recorded a terminal outcome there, and its
// process is gone. Returns true when it wrote the event.
func reapWorker(log *slog.Logger, stateDir, pool, workerID, path string) bool {
	started, hasTerminal, taskID := scanPipelineState(path, pool)
	if !started || hasTerminal {
		return false
	}
	if isWorkerAlive(stateDir, pool, workerID) {
		log.Info("reaper: worker still alive, leaving it", "pool", pool, "worker", workerID)
		return false
	}
	if taskID == "" {
		log.Warn("reaper: started event without task_id, skipping", "pool", pool, "worker", workerID)
		return false
	}
	if err := journal.Append(stateDir, journal.Event{
		TaskID:   taskID,
		WorkerID: workerID,
		Pool:     pool,
		Phase:    pool,
		Outcome:  journal.OutcomeDead,
		Note:     "reaped at orchestrator startup: runner gone without a terminal outcome",
	}); err != nil {
		log.Warn("reaper: append dead event", "pool", pool, "worker", workerID, "err", err)
		return false
	}
	// Drop the stale pid file so a later generation can't mistake it for
	// a live worker; best-effort, absence is not an error.
	_ = os.Remove(pidPath(stateDir, pool, workerID))
	log.Info("reaper: marked worker dead", "pool", pool, "worker", workerID, "task", taskID)
	return true
}

// scanPipelineState reports whether a worker journal file contains a
// pipeline-level (Phase == pool) `started` event and whether any
// pipeline-level terminal event exists, plus the task_id from the
// started event for attribution. Sub-events (Phase != pool, e.g.
// exec:claude:/finalize) are ignored — the task row folds only
// pipeline-level events, and the runner's terminal write is
// pipeline-level, so a successful sub-event must not look terminal.
func scanPipelineState(path, pool string) (started, hasTerminal bool, taskID string) {
	f, err := os.Open(path)
	if err != nil {
		return false, false, ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev journal.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if ev.Phase != pool {
			continue
		}
		switch ev.Outcome {
		case journal.OutcomeStarted:
			started = true
			if taskID == "" {
				taskID = ev.TaskID
			}
		case journal.OutcomeSucceeded, journal.OutcomeFailed, journal.OutcomeTimedOut, journal.OutcomeDead:
			hasTerminal = true
		}
	}
	return started, hasTerminal, taskID
}

// isWorkerAlive reports whether the worker's runner process is still
// running. The pid file (written by spawnWorker, removed on clean exit)
// holds the runner pid; a missing file or a vanished pid means the
// worker is gone. To survive pid reuse, a live pid is trusted only when
// its /proc cmdline still names this worker id.
func isWorkerAlive(stateDir, pool, workerID string) bool {
	data, err := os.ReadFile(pidPath(stateDir, pool, workerID))
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil {
		// ESRCH means no such process. Any other error (e.g. EPERM)
		// means the pid exists; treat as alive to avoid a false reap.
		return !errors.Is(err, syscall.ESRCH)
	}
	return pidIsWorker(pid, workerID)
}

// pidIsWorker guards against pid reuse: a live pid is only this worker's
// runner if its argv still carries the worker id (the runner is invoked
// with -worker-id <id>). When /proc can't be read we cannot disprove it,
// so we assume it is the worker — never reaping a possibly-live process.
func pidIsWorker(pid int, workerID string) bool {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return true
	}
	return strings.Contains(string(data), workerID) // cmdline is NUL-separated argv
}

func pidPath(stateDir, pool, workerID string) string {
	return filepath.Join(stateDir, "workers", pool, workerID+".pid")
}
