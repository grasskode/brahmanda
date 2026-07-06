// Package runner executes one worker invocation declared by a pipeline
// spec: sets up AGENT_* env, runs the command under `sh -c`, parses the
// worker's stdout as NDJSON journal events, enforces a timeout via
// process-group SIGTERM-then-SIGKILL, and writes a terminal journal
// event derived from the exit status when the worker did not emit one.
//
// The runner is intentionally short-lived — one invocation per call to
// Run. The long-running orchestrator spawns runners; the runner spawns
// the worker. Keeping the runner out of the orchestrator process means
// a crashed orchestrator never leaves a half-journaled worker behind.
package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
)

// Config is the runner's complete input for one invocation.
type Config struct {
	// Pipeline is the pool name attached to every journal event.
	Pipeline string
	// WorkerID uniquely identifies this invocation (used in logs).
	WorkerID string
	// StateRoot is the orchestrator state dir; the journal is rooted here.
	StateRoot string
	// Command is the shell snippet passed to `sh -c`. Any shell construct
	// (env interpolation, pipes, &&) is permitted.
	Command string
	// Timeout bounds the worker's lifetime; exceeding it triggers
	// process-group SIGTERM then SIGKILL.
	Timeout time.Duration
	// WorkerIndex is the pool slot (0..PoolSize-1) being filled. Exposed
	// to the worker as AGENT_WORKER_INDEX in case it wants to shard.
	WorkerIndex int

	// Stdout / Stderr are where the worker's piped output is mirrored.
	// Defaults to os.Stdout / os.Stderr when nil. Tests pass buffers.
	Stdout io.Writer
	Stderr io.Writer
}

// Result is the terminal summary the caller (orchestrator or CLI) reads
// to set its exit code. Populated even when the worker itself failed.
type Result struct {
	Outcome  journal.Outcome
	ExitCode int
	TimedOut bool
	Note     string // human-readable terminal note, journaled when present
	TaskID   string // most-recent task_id observed from worker stdout, empty if none
}

// Run executes one worker invocation. Returns the terminal Result.
// Error is reserved for invalid Config or unrecoverable setup failures
// (e.g. process couldn't start at all); worker-side failures are encoded
// in Result.Outcome and never returned as a Go error.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	stdout := cfg.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := cfg.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}

	env := append(os.Environ(),
		"AGENT_PIPELINE="+cfg.Pipeline,
		"AGENT_WORKER_ID="+cfg.WorkerID,
		"AGENT_STATE_ROOT="+cfg.StateRoot,
		"AGENT_WORKER_INDEX="+strconv.Itoa(cfg.WorkerIndex),
		// Signals headless mode to claude skills. Skills that would
		// otherwise pause for AskUserQuestion (e.g. /finalize confirming
		// follow-ups) check this and proceed with safe defaults.
		"AGENT_AUTONOMOUS=1",
	)

	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	cmd := exec.Command("sh", "-c", cfg.Command)
	cmd.Env = env
	// Setpgid puts the worker in its own process group; on timeout we
	// signal the whole group so any grandchildren (claude, gh, git, …)
	// die with it instead of outliving the runner.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return Result{}, fmt.Errorf("stdout pipe: %w", err)
	}
	// Worker stderr is piped (not wired directly to cfg.Stderr) so we
	// can copy it under the same mutex the parser uses — sharing the
	// stderr writer between goroutines is fine only if writes are
	// serialised, since *bytes.Buffer (the test path) is not concurrent-safe.
	workerStderr, err := cmd.StderrPipe()
	if err != nil {
		return Result{}, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("start worker: %w", err)
	}
	pid := cmd.Process.Pid

	// Serialise writes to the user-supplied stderr writer. The parser
	// writes warnings; the copier writes worker stderr. A tail buffer
	// shadow-keeps the last N bytes of everything written, so a worker
	// that exits non-zero before claiming a task still surfaces an
	// error in the journal (otherwise the run is invisible to the
	// monitor).
	var stderrMu sync.Mutex
	tail := &tailBuffer{max: 2048}
	safeStderr := &lockedWriter{mu: &stderrMu, w: io.MultiWriter(stderr, tail)}

	// Mirror worker stderr to the user's stderr writer. Bounded by EOF
	// on the pipe, which happens when the worker exits and cmd.Wait
	// closes the read end.
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		_, _ = io.Copy(safeStderr, workerStderr)
	}()

	// Watch the bounded ctx; on cancellation send SIGTERM to the worker's
	// process group, then SIGKILL 5s later if it's still around. Manual
	// rather than relying on exec.CommandContext so the signal hits the
	// whole pgroup, not just the immediate sh -c child.
	killWatch := make(chan struct{})
	go func() {
		select {
		case <-runCtx.Done():
			_ = syscall.Kill(-pid, syscall.SIGTERM)
			select {
			case <-time.After(5 * time.Second):
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			case <-killWatch:
			}
		case <-killWatch:
		}
	}()

	parse := stdoutParser{
		pipeline:  cfg.Pipeline,
		workerID:  cfg.WorkerID,
		stateRoot: cfg.StateRoot,
		mirror:    stdout,
		stderr:    safeStderr,
	}
	parse.run(stdoutPipe)

	waitErr := cmd.Wait()
	close(killWatch)
	<-stderrDone // ensure copier has drained before we read the buffer

	timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded)
	exitCode := 0
	switch {
	case waitErr == nil:
		exitCode = 0
	case timedOut:
		exitCode = -1
	default:
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	var outcome journal.Outcome
	var note string
	switch {
	case timedOut:
		outcome = journal.OutcomeTimedOut
		note = fmt.Sprintf("exceeded %s timeout", cfg.Timeout)
	case waitErr == nil:
		outcome = journal.OutcomeSucceeded
	default:
		outcome = journal.OutcomeFailed
		note = fmt.Sprintf("exit %d: %s", exitCode, waitErr.Error())
	}

	// Emit a terminal event ourselves when the worker didn't journal a
	// terminal outcome.
	//
	//   * worker claimed a task (lastTaskID set) but had no terminal
	//     event       → emit one under the claimed task_id
	//   * worker exited non-zero without claiming a task
	//                 → emit a synthetic event so the failure is visible
	//                   in the monitor; carries the stderr tail in note
	//   * worker exited 0 without claiming a task (legitimate no-op,
	//     e.g. "no work found") → emit nothing
	switch {
	case !parse.sawTerminal && parse.lastTaskID != "":
		if err := journal.Append(cfg.StateRoot, journal.Event{
			TaskID:   parse.lastTaskID,
			WorkerID: cfg.WorkerID,
			Pool:     cfg.Pipeline,
			Phase:    cfg.Pipeline,
			Outcome:  outcome,
			Note:     note,
		}); err != nil {
			fmt.Fprintf(safeStderr, "runner: terminal journal append failed: %v\n", err)
		}
	case parse.lastTaskID == "" && outcome != journal.OutcomeSucceeded:
		synthetic := note
		if t := strings.TrimSpace(tail.String()); t != "" {
			synthetic = note + "\n--- stderr tail ---\n" + t
		}
		if err := journal.Append(cfg.StateRoot, journal.Event{
			TaskID:   syntheticTaskID,
			WorkerID: cfg.WorkerID,
			Pool:     cfg.Pipeline,
			Phase:    cfg.Pipeline,
			Outcome:  outcome,
			Note:     synthetic,
		}); err != nil {
			fmt.Fprintf(safeStderr, "runner: silent-failure journal append failed: %v\n", err)
		}
	}

	return Result{
		Outcome:  outcome,
		ExitCode: exitCode,
		TimedOut: timedOut,
		Note:     note,
		TaskID:   parse.lastTaskID,
	}, nil
}

// syntheticTaskID is the task_id we attach to journal events the
// runner synthesises when a worker exits non-zero before claiming a
// task. The monitor surfaces these in a dedicated "silent failures"
// panel.
const syntheticTaskID = "(no-task)"

// SyntheticTaskID is the public alias readers (chitra) use to
// detect synthetic events. Kept as a const for cheap comparisons.
const SyntheticTaskID = syntheticTaskID

// lockedWriter wraps an io.Writer in a mutex so concurrent producers
// (parser warnings + worker stderr copier) can share one writer when
// the underlying *bytes.Buffer / *os.File is not concurrent-safe.
type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

// tailBuffer keeps the trailing `max` bytes of everything written to
// it. Used to attach an stderr tail to a synthetic journal event when
// a worker exits non-zero without claiming a task. Safe for concurrent
// use.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// stdoutParser reads the worker's stdout line-by-line, decodes each as a
// journal event, appends to the journal, and mirrors the raw line to the
// runner's stdout so operators tailing logs still see worker output.
type stdoutParser struct {
	pipeline    string
	workerID    string
	stateRoot   string
	mirror      io.Writer
	stderr      io.Writer
	mu          sync.Mutex
	lastTaskID  string
	sawTerminal bool
}

func (p *stdoutParser) run(r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintln(p.mirror, line) // mirror verbatim
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		evt, err := parseEvent(trimmed, p.pipeline, p.workerID)
		if err != nil {
			fmt.Fprintf(p.stderr, "runner: skip malformed stdout line: %v\n  line: %s\n", err, trimmed)
			continue
		}
		p.mu.Lock()
		p.lastTaskID = evt.TaskID
		if isTerminalOutcome(evt.Outcome) {
			p.sawTerminal = true
		}
		p.mu.Unlock()
		if err := journal.Append(p.stateRoot, evt); err != nil {
			fmt.Fprintf(p.stderr, "runner: journal append failed: %v\n", err)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(p.stderr, "runner: scan stdout: %v\n", err)
	}
}

// workerEvent is the shape the worker emits per stdout line. Only
// task_id is required; the runner fills in defaults for the rest.
type workerEvent struct {
	TaskID  string `json:"task_id"`
	Phase   string `json:"phase,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Note    string `json:"note,omitempty"`
}

func parseEvent(line, pipeline, workerID string) (journal.Event, error) {
	var w workerEvent
	if err := json.Unmarshal([]byte(line), &w); err != nil {
		return journal.Event{}, fmt.Errorf("not JSON: %w", err)
	}
	if w.TaskID == "" {
		return journal.Event{}, fmt.Errorf("task_id required")
	}
	phase := w.Phase
	if phase == "" {
		phase = pipeline
	}
	outcome := journal.Outcome(w.Outcome)
	if outcome == "" {
		outcome = journal.OutcomeStarted
	}
	return journal.Event{
		TaskID:   w.TaskID,
		WorkerID: workerID,
		Pool:     pipeline,
		Phase:    phase,
		Outcome:  outcome,
		Note:     w.Note,
	}, nil
}

func isTerminalOutcome(o journal.Outcome) bool {
	switch o {
	case journal.OutcomeSucceeded, journal.OutcomeFailed, journal.OutcomeTimedOut, journal.OutcomeDead:
		return true
	}
	return false
}

func (c *Config) validate() error {
	if c.Pipeline == "" {
		return fmt.Errorf("pipeline name is required")
	}
	if c.WorkerID == "" {
		return fmt.Errorf("worker id is required")
	}
	if c.StateRoot == "" {
		return fmt.Errorf("state root is required")
	}
	if strings.TrimSpace(c.Command) == "" {
		return fmt.Errorf("command is required")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be > 0, got %s", c.Timeout)
	}
	return nil
}
