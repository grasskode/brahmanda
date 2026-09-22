// Package journal is the orchestrator's append-only outcome log.
//
// Layout:
//
//	<state_dir>/journal/<pool>/<worker_id>.jsonl
//
// One file per worker invocation, owned exclusively by its writer.
// Multiple events from the same invocation (started + terminal, plus
// any user-emitted progress) accumulate in that file via O_APPEND.
// No two processes write to the same file, so there is no
// cross-process contention to coordinate.
//
// Retention is out of band: see dist/cleanup.sh. The package no
// longer rewrites the journal; it only appends and reads.
//
// Append is safe for concurrent goroutines in one process (the
// per-state-root mutex serialises them) and safe across processes
// because each process targets a distinct file.
package journal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Outcome is the terminal state a worker reached, or "started" while
// it's still running. The set is fixed; the orchestrator and the
// monitor both pattern-match against these strings.
type Outcome string

const (
	OutcomeStarted   Outcome = "started"
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
	OutcomeTimedOut  Outcome = "timed_out"
	OutcomeDead      Outcome = "dead" // worker process gone without writing a terminal status
)

// Event is one journal entry — exactly one line in a worker's .jsonl
// file. WorkerID is what routes the event to its file; TaskID is
// what the operator queries against.
type Event struct {
	Timestamp time.Time `json:"ts"`
	TaskID    string    `json:"task_id"`
	WorkerID  string    `json:"worker_id"`
	Pool      string    `json:"pool"`
	Phase     string    `json:"phase"`
	Outcome   Outcome   `json:"outcome"`
	Note      string    `json:"note,omitempty"`
	// Agent identifies the agent session this event's worker drove, when
	// known. Optional: most events don't carry one. The token monitor
	// joins its session id to the agent's usage log for per-phase burn.
	Agent *AgentSession `json:"agent,omitempty"`
}

// AgentSession identifies one run of an agent runtime. Runtime names
// the agent (e.g. "claude"); Model is the model it ran (empty when the
// runtime is not model-parameterised), letting burn be attributed by
// model; SessionID is that runtime's own session identifier, which
// locates the run's usage log.
//
// CostUSD and Usage are what the run cost, as reported by the runtime's
// own accounting (claude's result envelope). A worker emits them on an
// event once the run has returned; the pre-run breadcrumb carries
// neither. Both are per invocation — a session resumed several times
// reports each run separately and readers sum them.
type AgentSession struct {
	Runtime   string  `json:"runtime"`
	Model     string  `json:"model,omitempty"`
	SessionID string  `json:"session_id"`
	CostUSD   float64 `json:"cost_usd,omitempty"`
	Usage     *Usage  `json:"usage,omitempty"`
}

// Usage is one run's token counts. Field names follow the claude API's
// usage object so a worker can pass the envelope's usage through
// unchanged. Cache reads are kept separate from input — they are an
// order of magnitude cheaper and would misrepresent burn if merged.
type Usage struct {
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadTokens     int64 `json:"cache_read_input_tokens"`
}

// Add accumulates other into u.
func (u *Usage) Add(other Usage) {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.CacheCreationTokens += other.CacheCreationTokens
	u.CacheReadTokens += other.CacheReadTokens
}

// Total is the sum of every token class.
func (u Usage) Total() int64 {
	return u.InputTokens + u.OutputTokens + u.CacheCreationTokens + u.CacheReadTokens
}

// IsZero reports whether no tokens were recorded.
func (u Usage) IsZero() bool { return u.Total() == 0 }

// safeNameRE bounds Pool and WorkerID to a kebab-case-ish charset so
// they're safe to embed in a filesystem path. Same shape as the
// pipelinespec.Spec.Name regex, plus underscores for worker IDs that
// may include them.
var safeNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Dir is the canonical journal root for the given state root.
// Useful for cleanup scripts and tests.
func Dir(stateRoot string) string {
	return filepath.Join(stateRoot, "journal")
}

// FileFor returns the absolute path of the file Append would write to
// for the given pool/worker. Exposed for tests and tooling.
func FileFor(stateRoot, pool, workerID string) string {
	return filepath.Join(Dir(stateRoot), pool, workerID+".jsonl")
}

// Append writes one event to <state>/journal/<pool>/<worker_id>.jsonl.
// Creates the directory if needed. Each call is one atomic O_APPEND
// write of a single line, so concurrent writers (different
// pools/workers) never interfere. Returns an error only for
// validation, I/O, or encoding failures — the caller can choose to
// downgrade those to warnings.
func Append(stateRoot string, ev Event) error {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	if err := validateEvent(ev); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	mu := lockFor(stateRoot)
	mu.Lock()
	defer mu.Unlock()

	path := FileFor(stateRoot, ev.Pool, ev.WorkerID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("journal: mkdir %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("journal: open %s: %w", path, err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(ev); err != nil {
		return fmt.Errorf("journal: encode: %w", err)
	}
	return nil
}

// Read returns every event under <state>/journal with Timestamp >= since,
// sorted by timestamp. A missing journal dir returns no events without
// error (a fresh state root is not an error). Malformed lines and
// unreadable files are skipped so a single bad worker file never
// breaks the monitor.
func Read(stateRoot string, since time.Time) ([]Event, error) {
	root := Dir(stateRoot)
	var events []Event
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".jsonl") {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			// Permission / transient issue on one file shouldn't
			// abort the whole read — log path-level errors via the
			// returned error only when the walk itself fails.
			return nil
		}
		es := readFiltered(f, since)
		f.Close()
		events = append(events, es...)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return events, fmt.Errorf("journal: walk: %w", err)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Timestamp.Before(events[j].Timestamp) })
	return events, nil
}

// ReadFrom decodes the events in one journal file (or any NDJSON
// stream), keeping those at or after since; a zero since keeps all. Bad
// lines are skipped silently.
func ReadFrom(r io.Reader, since time.Time) []Event {
	return readFiltered(r, since)
}

// readFiltered streams r line-by-line, decoding each as Event, keeping
// only those at or after since. Bad lines are skipped silently.
func readFiltered(r io.Reader, since time.Time) []Event {
	var out []Event
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		if !since.IsZero() && ev.Timestamp.Before(since) {
			continue
		}
		out = append(out, ev)
	}
	return out
}

func validateEvent(ev Event) error {
	if ev.TaskID == "" {
		return fmt.Errorf("incomplete event: task_id required (%+v)", ev)
	}
	if ev.WorkerID == "" {
		return fmt.Errorf("incomplete event: worker_id required (%+v)", ev)
	}
	if ev.Pool == "" {
		return fmt.Errorf("incomplete event: pool required (%+v)", ev)
	}
	if ev.Phase == "" {
		return fmt.Errorf("incomplete event: phase required (%+v)", ev)
	}
	if ev.Outcome == "" {
		return fmt.Errorf("incomplete event: outcome required (%+v)", ev)
	}
	if !safeNameRE.MatchString(ev.Pool) {
		return fmt.Errorf("pool %q contains characters unsafe for a filesystem path", ev.Pool)
	}
	if !safeNameRE.MatchString(ev.WorkerID) {
		return fmt.Errorf("worker_id %q contains characters unsafe for a filesystem path", ev.WorkerID)
	}
	return nil
}

// lockFor returns the per-stateRoot mutex that serialises Append/Read
// within one process. Different processes hit different files so they
// don't need cross-process coordination.
var (
	locksMu sync.Mutex
	locks   = map[string]*sync.Mutex{}
)

func lockFor(root string) *sync.Mutex {
	locksMu.Lock()
	defer locksMu.Unlock()
	if m, ok := locks[root]; ok {
		return m
	}
	m := &sync.Mutex{}
	locks[root] = m
	return m
}
