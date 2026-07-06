package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
)

// writeWorkerJournal writes the given events as one worker's .jsonl file.
func writeWorkerJournal(t *testing.T, stateDir, pool, workerID string, evs ...journal.Event) {
	t.Helper()
	dir := filepath.Join(journal.Dir(stateDir), pool)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, workerID+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, ev := range evs {
		if err := json.NewEncoder(f).Encode(ev); err != nil {
			t.Fatal(err)
		}
	}
}

func writePid(t *testing.T, stateDir, pool, workerID string, pid int) {
	t.Helper()
	p := pidPath(stateDir, pool, workerID)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// deadOutcomeFor returns the latest pipeline-level outcome for a task,
// reading the journal the way the monitor does.
func latestOutcome(t *testing.T, stateDir, pool, taskID string) journal.Outcome {
	t.Helper()
	evs, err := journal.Read(stateDir, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var out journal.Outcome
	for _, ev := range evs {
		if ev.TaskID == taskID && ev.Pool == pool && ev.Phase == pool {
			out = ev.Outcome
		}
	}
	return out
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func started(pool, task string) journal.Event {
	return journal.Event{TaskID: task, WorkerID: "w", Pool: pool, Phase: pool, Outcome: journal.OutcomeStarted, Timestamp: time.Now().UTC()}
}

func TestReapMarksOrphanDead(t *testing.T) {
	dir := t.TempDir()
	// started, no terminal, no pid file → worker is gone.
	writeWorkerJournal(t, dir, "finalize", "orphan1", started("finalize", "QUA-1"))

	reapOrphans(quietLogger(), dir)

	if got := latestOutcome(t, dir, "finalize", "QUA-1"); got != journal.OutcomeDead {
		t.Fatalf("outcome = %q, want %q", got, journal.OutcomeDead)
	}
}

func TestReapSkipsTerminal(t *testing.T) {
	dir := t.TempDir()
	ev := started("implement", "QUA-2")
	done := ev
	done.Outcome = journal.OutcomeSucceeded
	done.Timestamp = ev.Timestamp.Add(time.Second)
	writeWorkerJournal(t, dir, "implement", "w2", ev, done)

	reapOrphans(quietLogger(), dir)

	if got := latestOutcome(t, dir, "implement", "QUA-2"); got != journal.OutcomeSucceeded {
		t.Fatalf("outcome = %q, want it left as %q", got, journal.OutcomeSucceeded)
	}
}

// A successful *sub*-event (Phase != pool) must not count as a terminal
// outcome for the worker — only pipeline-level events terminate it.
func TestReapIgnoresSubEventSuccess(t *testing.T) {
	dir := t.TempDir()
	start := started("finalize", "QUA-3")
	sub := start
	sub.Phase = "exec:claude:/finalize"
	sub.Outcome = journal.OutcomeSucceeded
	sub.Timestamp = start.Timestamp.Add(time.Second)
	writeWorkerJournal(t, dir, "finalize", "w3", start, sub)

	reapOrphans(quietLogger(), dir)

	if got := latestOutcome(t, dir, "finalize", "QUA-3"); got != journal.OutcomeDead {
		t.Fatalf("outcome = %q, want %q (sub-event success is not terminal)", got, journal.OutcomeDead)
	}
}

func TestReapReapsDeadPid(t *testing.T) {
	dir := t.TempDir()
	writeWorkerJournal(t, dir, "finalize", "w4", started("finalize", "QUA-4"))

	// A pid that existed and is now gone: start a trivial process, wait
	// for it to exit, then use its (now free) pid.
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot spawn helper process: %v", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Wait()
	writePid(t, dir, "finalize", "w4", pid)

	reapOrphans(quietLogger(), dir)

	if got := latestOutcome(t, dir, "finalize", "QUA-4"); got != journal.OutcomeDead {
		t.Fatalf("outcome = %q, want %q", got, journal.OutcomeDead)
	}
}

func TestReapLeavesLiveWorker(t *testing.T) {
	self, err := os.ReadFile("/proc/self/cmdline")
	if err != nil || !strings.Contains(string(self), "test") {
		t.Skip("needs /proc and a recognizable argv token")
	}
	dir := t.TempDir()
	// Use a worker id present in our own argv so the pid-reuse guard
	// recognizes this live process as the worker.
	const workerID = "test"
	writeWorkerJournal(t, dir, "finalize", workerID, started("finalize", "QUA-5"))
	writePid(t, dir, "finalize", workerID, os.Getpid())

	reapOrphans(quietLogger(), dir)

	if got := latestOutcome(t, dir, "finalize", "QUA-5"); got != journal.OutcomeStarted {
		t.Fatalf("outcome = %q, want it left as %q (worker is alive)", got, journal.OutcomeStarted)
	}
}
