package journal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newEvent(pool, workerID, taskID string, outcome Outcome) Event {
	return Event{
		TaskID:   taskID,
		WorkerID: workerID,
		Pool:     pool,
		Phase:    pool,
		Outcome:  outcome,
	}
}

func TestAppend_RoutesToPerWorkerFile(t *testing.T) {
	root := t.TempDir()
	ev := newEvent("triage", "w-1", "TASK-1", OutcomeStarted)
	if err := Append(root, ev); err != nil {
		t.Fatalf("Append: %v", err)
	}
	want := FileFor(root, "triage", "w-1")
	if _, err := os.Stat(want); err != nil {
		t.Errorf("expected file %s, stat err = %v", want, err)
	}
	// Cross-check: nothing should be written outside that path.
	if _, err := os.Stat(filepath.Join(root, "journal.jsonl")); err == nil {
		t.Error("legacy journal.jsonl should NOT exist in the new layout")
	}
}

func TestAppend_MultipleEventsSameWorkerAccumulate(t *testing.T) {
	root := t.TempDir()
	for _, oc := range []Outcome{OutcomeStarted, OutcomeSucceeded} {
		if err := Append(root, newEvent("triage", "w-1", "TASK-1", oc)); err != nil {
			t.Fatalf("Append(%s): %v", oc, err)
		}
	}
	body, err := os.ReadFile(FileFor(root, "triage", "w-1"))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if got := strings.Count(string(body), "\n"); got != 2 {
		t.Errorf("file has %d lines, want 2", got)
	}
}

func TestRead_WalksAllPoolsAndWorkers(t *testing.T) {
	root := t.TempDir()
	for _, ev := range []Event{
		newEvent("triage", "w-1", "TASK-A", OutcomeStarted),
		newEvent("triage", "w-1", "TASK-A", OutcomeSucceeded),
		newEvent("triage", "w-2", "TASK-B", OutcomeStarted),
		newEvent("review", "w-9", "TASK-A", OutcomeStarted),
		newEvent("review", "w-9", "TASK-A", OutcomeFailed),
	} {
		if err := Append(root, ev); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	events, err := Read(root, time.Time{})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("got %d events, want 5: %+v", len(events), events)
	}
	// Read must return events sorted by timestamp.
	for i := 1; i < len(events); i++ {
		if events[i].Timestamp.Before(events[i-1].Timestamp) {
			t.Errorf("events not sorted: events[%d].ts=%s < events[%d].ts=%s",
				i, events[i].Timestamp, i-1, events[i-1].Timestamp)
		}
	}
}

func TestRead_MissingDirReturnsEmptyNoError(t *testing.T) {
	events, err := Read(t.TempDir(), time.Time{})
	if err != nil {
		t.Fatalf("Read on empty state root: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestRead_SinceFilter(t *testing.T) {
	root := t.TempDir()
	old := newEvent("triage", "w-1", "TASK-A", OutcomeStarted)
	old.Timestamp = time.Now().Add(-2 * time.Hour).UTC()
	newer := newEvent("triage", "w-1", "TASK-A", OutcomeSucceeded)
	newer.Timestamp = time.Now().UTC()
	if err := Append(root, old); err != nil {
		t.Fatal(err)
	}
	if err := Append(root, newer); err != nil {
		t.Fatal(err)
	}
	events, err := Read(root, time.Now().Add(-30*time.Minute))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(events) != 1 || events[0].Outcome != OutcomeSucceeded {
		t.Errorf("since filter wrong: got %+v", events)
	}
}

func TestRead_SkipsCorruptLines(t *testing.T) {
	root := t.TempDir()
	if err := Append(root, newEvent("triage", "w-1", "TASK-A", OutcomeStarted)); err != nil {
		t.Fatal(err)
	}
	// Hand-inject a bad line into the same file.
	path := FileFor(root, "triage", "w-1")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("this is not json\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := Append(root, newEvent("triage", "w-1", "TASK-A", OutcomeSucceeded)); err != nil {
		t.Fatal(err)
	}
	events, err := Read(root, time.Time{})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("got %d events, want 2 (started + succeeded; corrupt line skipped): %+v", len(events), events)
	}
}

func TestAppend_Validation(t *testing.T) {
	root := t.TempDir()
	cases := []struct {
		label string
		mut   func(*Event)
		want  string
	}{
		{"missing-task-id", func(e *Event) { e.TaskID = "" }, "task_id required"},
		{"missing-worker-id", func(e *Event) { e.WorkerID = "" }, "worker_id required"},
		{"missing-pool", func(e *Event) { e.Pool = "" }, "pool required"},
		{"missing-phase", func(e *Event) { e.Phase = "" }, "phase required"},
		{"missing-outcome", func(e *Event) { e.Outcome = "" }, "outcome required"},
		{"unsafe-pool", func(e *Event) { e.Pool = "..//etc" }, "unsafe for a filesystem path"},
		{"unsafe-worker-id", func(e *Event) { e.WorkerID = "x/y" }, "unsafe for a filesystem path"},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			ev := newEvent("triage", "w-1", "TASK-A", OutcomeStarted)
			c.mut(&ev)
			err := Append(root, ev)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("expected error containing %q, got %v", c.want, err)
			}
		})
	}
}

func TestFileFor_PathShape(t *testing.T) {
	got := FileFor("/tmp/state", "triage", "abc123")
	want := filepath.Join("/tmp/state", "journal", "triage", "abc123.jsonl")
	if got != want {
		t.Errorf("FileFor = %q, want %q", got, want)
	}
}
