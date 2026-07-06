package runner

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
)

// writeScript writes a #!/bin/sh script into a tempdir and returns its
// absolute path. The runner exec's commands via `sh -c <command>`, so
// invoking the script path directly works.
func writeScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func readJournal(t *testing.T, stateRoot string) []journal.Event {
	t.Helper()
	events, err := journal.Read(stateRoot, time.Time{})
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	return events
}

func newCfg(stateRoot, script string) Config {
	return Config{
		Pipeline:  "test-pool",
		WorkerID:  "w-1",
		StateRoot: stateRoot,
		Command:   script,
		Timeout:   5 * time.Second,
		Stdout:    &bytes.Buffer{},
		Stderr:    &bytes.Buffer{},
	}
}

func TestRun_SuccessWithNDJSON(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `
echo '{"task_id":"QUA-1","outcome":"started"}'
echo '{"task_id":"QUA-1","outcome":"succeeded"}'
`)
	res, err := Run(context.Background(), newCfg(stateRoot, script))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != journal.OutcomeSucceeded {
		t.Errorf("Outcome = %s, want succeeded", res.Outcome)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", res.ExitCode)
	}
	if res.TaskID != "QUA-1" {
		t.Errorf("TaskID = %q, want QUA-1", res.TaskID)
	}
	events := readJournal(t, stateRoot)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(events), events)
	}
	if events[0].Outcome != journal.OutcomeStarted || events[1].Outcome != journal.OutcomeSucceeded {
		t.Errorf("event outcomes wrong: %+v", events)
	}
	for _, ev := range events {
		if ev.Pool != "test-pool" {
			t.Errorf("Pool = %q, want test-pool", ev.Pool)
		}
		if ev.Phase != "test-pool" {
			t.Errorf("Phase = %q, want test-pool (default)", ev.Phase)
		}
	}
}

func TestRun_NoNDJSON_NoJournalEntries(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `echo "no work today" >&2; exit 0`)
	res, err := Run(context.Background(), newCfg(stateRoot, script))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != journal.OutcomeSucceeded {
		t.Errorf("Outcome = %s, want succeeded", res.Outcome)
	}
	if res.TaskID != "" {
		t.Errorf("TaskID = %q, want empty", res.TaskID)
	}
	if events := readJournal(t, stateRoot); len(events) != 0 {
		t.Errorf("expected no journal events, got %+v", events)
	}
}

func TestRun_WorkerSkipsTerminal_RunnerFillsIt(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `echo '{"task_id":"QUA-2","outcome":"started"}'; exit 0`)
	res, err := Run(context.Background(), newCfg(stateRoot, script))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != journal.OutcomeSucceeded {
		t.Errorf("Outcome = %s, want succeeded", res.Outcome)
	}
	events := readJournal(t, stateRoot)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (started + runner-emitted succeeded): %+v", len(events), events)
	}
	if events[0].Outcome != journal.OutcomeStarted {
		t.Errorf("event[0] outcome = %s, want started", events[0].Outcome)
	}
	if events[1].Outcome != journal.OutcomeSucceeded || events[1].TaskID != "QUA-2" {
		t.Errorf("event[1] = %+v, want runner-emitted succeeded for QUA-2", events[1])
	}
}

func TestRun_WorkerEmitsTerminal_RunnerDoesNotDuplicate(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `
echo '{"task_id":"QUA-3","outcome":"started"}'
echo '{"task_id":"QUA-3","outcome":"succeeded","note":"all good"}'
`)
	_, err := Run(context.Background(), newCfg(stateRoot, script))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := readJournal(t, stateRoot)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (started + worker terminal): %+v", len(events), events)
	}
	if events[1].Note != "all good" {
		t.Errorf("event[1] note = %q, want %q (worker's terminal preserved)", events[1].Note, "all good")
	}
}

func TestRun_NonzeroExit_TerminalIsFailed(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `
echo '{"task_id":"QUA-4","outcome":"started"}'
exit 7
`)
	res, err := Run(context.Background(), newCfg(stateRoot, script))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != journal.OutcomeFailed {
		t.Errorf("Outcome = %s, want failed", res.Outcome)
	}
	if res.ExitCode != 7 {
		t.Errorf("ExitCode = %d, want 7", res.ExitCode)
	}
	events := readJournal(t, stateRoot)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(events), events)
	}
	if events[1].Outcome != journal.OutcomeFailed {
		t.Errorf("event[1] outcome = %s, want failed", events[1].Outcome)
	}
	if !strings.Contains(events[1].Note, "exit 7") {
		t.Errorf("event[1] note = %q, want it to mention 'exit 7'", events[1].Note)
	}
}

func TestRun_Timeout_TerminalIsTimedOut(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `
echo '{"task_id":"QUA-5","outcome":"started"}'
sleep 10
`)
	cfg := newCfg(stateRoot, script)
	cfg.Timeout = 500 * time.Millisecond
	start := time.Now()
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	elapsed := time.Since(start)
	if !res.TimedOut {
		t.Errorf("TimedOut = false, want true")
	}
	if res.Outcome != journal.OutcomeTimedOut {
		t.Errorf("Outcome = %s, want timed_out", res.Outcome)
	}
	// Should be killed quickly (within the 5s SIGKILL escalation budget).
	if elapsed > 6*time.Second {
		t.Errorf("elapsed = %s, expected < 6s; process group kill may have failed", elapsed)
	}
	events := readJournal(t, stateRoot)
	if len(events) < 2 {
		t.Fatalf("got %d events, want >= 2: %+v", len(events), events)
	}
	terminal := events[len(events)-1]
	if terminal.Outcome != journal.OutcomeTimedOut {
		t.Errorf("terminal outcome = %s, want timed_out", terminal.Outcome)
	}
	if !strings.Contains(terminal.Note, "timeout") {
		t.Errorf("terminal note = %q, want it to mention 'timeout'", terminal.Note)
	}
}

func TestRun_MalformedLine_SkippedAndContinues(t *testing.T) {
	stateRoot := t.TempDir()
	stderr := &bytes.Buffer{}
	script := writeScript(t, `
echo 'not json at all'
echo '{"missing_task_id":"oops"}'
echo '{"task_id":"QUA-6","outcome":"succeeded"}'
`)
	cfg := newCfg(stateRoot, script)
	cfg.Stderr = stderr
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != journal.OutcomeSucceeded {
		t.Errorf("Outcome = %s, want succeeded", res.Outcome)
	}
	events := readJournal(t, stateRoot)
	if len(events) != 1 || events[0].TaskID != "QUA-6" {
		t.Errorf("got events %+v, want exactly [QUA-6 succeeded]", events)
	}
	if !strings.Contains(stderr.String(), "skip malformed") {
		t.Errorf("expected stderr to contain skip-malformed warning, got: %s", stderr.String())
	}
}

func TestRun_EnvPropagation(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `
printf '{"task_id":"%s","outcome":"succeeded","note":"%s|%s|%s"}\n' \
  "$AGENT_PIPELINE" "$AGENT_WORKER_ID" "$AGENT_STATE_ROOT" "$AGENT_WORKER_INDEX"
`)
	cfg := newCfg(stateRoot, script)
	cfg.Pipeline = "triage"
	cfg.WorkerID = "wid-xyz"
	cfg.WorkerIndex = 2
	_, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := readJournal(t, stateRoot)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(events), events)
	}
	ev := events[0]
	if ev.TaskID != "triage" {
		t.Errorf("AGENT_PIPELINE not exposed: TaskID = %q", ev.TaskID)
	}
	wantNote := "wid-xyz|" + stateRoot + "|2"
	if ev.Note != wantNote {
		t.Errorf("env note = %q, want %q", ev.Note, wantNote)
	}
}

func TestRun_CustomPhase(t *testing.T) {
	stateRoot := t.TempDir()
	script := writeScript(t, `
echo '{"task_id":"QUA-7","phase":"setup","outcome":"started"}'
echo '{"task_id":"QUA-7","phase":"setup","outcome":"succeeded"}'
`)
	_, err := Run(context.Background(), newCfg(stateRoot, script))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := readJournal(t, stateRoot)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(events), events)
	}
	for _, ev := range events {
		if ev.Phase != "setup" {
			t.Errorf("phase = %q, want setup (worker-supplied)", ev.Phase)
		}
	}
}

func TestRun_StdoutMirroredToCfgStdout(t *testing.T) {
	stateRoot := t.TempDir()
	stdout := &bytes.Buffer{}
	script := writeScript(t, `echo hello world; echo '{"task_id":"QUA-8"}'`)
	cfg := newCfg(stateRoot, script)
	cfg.Stdout = stdout
	_, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(stdout.String(), "hello world") {
		t.Errorf("worker stdout not mirrored; got: %s", stdout.String())
	}
}

func TestRun_ConfigValidation(t *testing.T) {
	stateRoot := t.TempDir()
	base := newCfg(stateRoot, "true")
	cases := []struct {
		label string
		mut   func(*Config)
		want  string
	}{
		{"missing-pipeline", func(c *Config) { c.Pipeline = "" }, "pipeline name is required"},
		{"missing-worker-id", func(c *Config) { c.WorkerID = "" }, "worker id is required"},
		{"missing-state-root", func(c *Config) { c.StateRoot = "" }, "state root is required"},
		{"missing-command", func(c *Config) { c.Command = "   " }, "command is required"},
		{"zero-timeout", func(c *Config) { c.Timeout = 0 }, "timeout must be > 0"},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			cfg := base
			c.mut(&cfg)
			_, err := Run(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("expected error containing %q, got %v", c.want, err)
			}
		})
	}
}
