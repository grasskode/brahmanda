// Package agentui implements chitra's snapshot collection and
// rendering, decoupled from brahma. Both the one-shot chitra output
// and the --watch bubbletea TUI consume the same Snapshot value, so
// the rendered information agrees across modes.
//
// Everything the monitor shows is a projection of orchestrator-owned
// state — it never reads worker scratch (the worktrees' .agent/ files).
// "What ran, what succeeded, what failed" comes entirely from the
// journal, so the monitor stays decoupled from whatever pipeline the
// workers happen to implement. Layout knowledge encoded here:
//
//   - <state>/brahma.lock                 (single-instance flock + pid)
//   - <state>/agent-errors/claude               (backoff marker)
//   - <state>/journal/<pool>/<worker_id>.jsonl  (per-worker event files)
//   - <state>/workers/<pool>/<worker_id>.pid    (liveness for the RUNNING count)
//   - <state>/runtime.yaml                      (configured pipeline list)
//   - <state>/accounts.yaml                     (operator identity panel)
package agentui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/grasskode/brahmanda/internal/budget"
	"github.com/grasskode/brahmanda/internal/journal"
	"github.com/grasskode/brahmanda/internal/lock"
	"github.com/grasskode/brahmanda/internal/pipelinespec"
	"github.com/grasskode/brahmanda/internal/state"
	"github.com/grasskode/brahmanda/internal/tokens"
)

// Snapshot is one frame of monitor state.
type Snapshot struct {
	CapturedAt    time.Time
	StateDir      string
	WorktreesRoot string
	ConfigPath    string // resolved from <state>/runtime.yaml, "" if not running

	// Windowing.
	Since      time.Duration
	StaleAfter time.Duration
	SinceLabel string // human description: "today" / "last 6h" / "all time"

	// Per-pipeline tick intervals from runtime.yaml (or zero map if
	// orchestrator not running). Used to populate the TICK column.
	PipelineTicks map[string]time.Duration
	// Per-pipeline configured pool size (max concurrent worker
	// invocations). Empty when the orchestrator is not running.
	PipelineCapacities map[string]int
	// PipelineBudgets holds the rolling-budget position of every pipeline
	// that declares a budget (from runtime.yaml + journal cost events).
	// GlobalBudget is the cross-pipeline cap, nil when none is configured.
	PipelineBudgets map[string]BudgetStat
	GlobalBudget    *BudgetStat
	// PipelineOrder is pipeline names in the order they appear in the
	// orchestrator's config (preserved through runtime.yaml). Used to
	// render the pipelines table in operator-meaningful order rather
	// than alphabetically. Empty when the orchestrator isn't running.
	PipelineOrder []string
	// MaxWorkers is the global concurrent-worker cap shared across all
	// pipelines (from runtime.yaml). Zero when the orchestrator is not
	// running or predates the global-pool field.
	MaxWorkers int

	// Panels.
	Orchestrator   OrchestratorPanel
	Backoff        BackoffPanel
	Accounts       state.Accounts
	Pipelines      []PipelineStat
	Tasks          []TaskRow
	Tokens         TokensPanel
	SilentFailures []SilentFailure // workers that exited non-zero without claiming a task
}

// SilentFailure is one journal event the runner synthesised when a
// worker exited non-zero before claiming any task. The note typically
// carries the stderr tail.
type SilentFailure struct {
	When     time.Time
	Pipeline string
	WorkerID string
	Note     string
}

type OrchestratorPanel struct {
	Alive bool
	PID   string
	Since time.Time
}

type BackoffPanel struct {
	Active    bool
	WrittenAt time.Time
}

// PipelineStat is one pipeline's aggregate over the lookback window.
// All counters fold only pipeline-level events (ev.Phase == ev.Pool);
// fine-grained exec:* sub-events emitted by some workers are noise at
// this level and would inflate Errors when one retry chain bursts out
// dozens of failures.
type PipelineStat struct {
	Name        string
	Ran         int       // distinct started events in window
	Running     int       // actually-running workers right now (live log file + no pipeline terminal yet)
	Errors      int       // failed + timed_out + dead, pipeline-level only
	LastError   time.Time // latest pipeline-level error
	LastSuccess time.Time // latest pipeline-level success
}

// taskLogPrefix marks a task-scoped log line. A worker emits an event
// whose phase is "log:<key>" to float a line onto the task's row in the
// monitor without pretending to be a pipeline stage: it never joins the
// phase chain and never touches pool counters. <key> lets a worker that
// runs every tick refresh the same line (latest note wins) instead of
// stacking duplicates; distinct keys yield distinct lines.
const taskLogPrefix = "log:"

// TaskRow is one task's phase chain, ordered by the configured pipeline
// order (first-seen for phases outside it), plus any task-scoped log lines
// (see taskLogPrefix).
type TaskRow struct {
	TaskID     string
	Chain      []PhaseStep
	Logs       []TaskLogLine
	LastUpdate time.Time // timestamp of the task's most recent journal event in the window
}

// TaskLogLine is one task-scoped log line surfaced under the task header,
// ahead of the phase chain. Outcome only drives the line's colour; it is
// not a pipeline outcome and is excluded from pool success/error counts.
type TaskLogLine struct {
	Key     string // the "log:<key>" the line was emitted under
	Text    string
	Outcome journal.Outcome
}

type PhaseStep struct {
	Phase   string
	Outcome journal.Outcome
	// Note is the latest pipeline-level note for this phase (typically
	// the failure reason). Only populated when meaningful, since the
	// renderer suppresses notes on succeeded phases to keep the chain
	// scannable.
	Note string
	// LastAt is the timestamp of this phase's most recent event. The
	// renderer picks a task's current stage by newest LastAt rather than
	// by chain position, so a windowed or out-of-order chain still
	// reports the latest activity.
	LastAt time.Time
}

type TokensPanel struct {
	Available     bool // false when there was no state dir OR collection erred
	CostAvailable bool // true when at least one session reported its cost
	ByPhase       []tokens.Rollup
	ByTask        []tokens.Rollup
	Note          string // error / "by phase, last 24h" etc.
}

// BudgetStat is one pipeline's rolling-budget position: what the config
// caps it at and what its workers reported spending inside the trailing
// window. Exceeded means the orchestrator is currently holding launches;
// ResetAt is when enough spend has aged out for it to resume.
type BudgetStat struct {
	Limit    pipelinespec.Budget
	Spent    float64
	Exceeded bool
	ResetAt  time.Time
}

// CollectOptions controls a snapshot. Most fields can be left empty
// — Collect will discover sensible defaults from <state_dir>/runtime.yaml
// (written by the orchestrator) and from environment variables.
//   - WorktreesRoot: empty → read from runtime.yaml or $WORKTREES_ROOT;
//     still empty → worktrees-census + stale-locks panels are skipped.
//   - ClaudeHome:    empty → $CLAUDE_HOME, else ~/.claude.
//   - Since/SinceCutoff/SinceLabel: see Since handling. Since takes
//     precedence over SinceCutoff when both are set.
type CollectOptions struct {
	StateDir      string
	WorktreesRoot string
	ClaudeHome    string
	// Since is the duration lookback. If SinceCutoff is non-zero it
	// takes precedence (used by --since today which resolves to local
	// midnight, not a fixed duration).
	Since       time.Duration
	SinceCutoff time.Time
	SinceLabel  string
	// SinceSpec is the raw --since value ("today" / "6h" / "all"). When
	// set, Collect re-resolves it against the current time on every
	// refresh and overrides Since / SinceCutoff / SinceLabel — so a
	// long-running --watch keeps a moving window instead of freezing the
	// cutoff at startup. Empty means "use SinceCutoff / Since as given".
	SinceSpec  string
	StaleAfter time.Duration
}

// ResolveSince interprets a --since spec relative to now, returning the
// lookback cutoff, its duration (zero for "today" / "all"), and a human
// label. The monitor calls this on every refresh (not once at startup)
// so a long-running --watch keeps a moving window: "today" tracks the
// current local midnight and "6h" stays six hours behind the wall clock.
//
//	"today" → 00:00 local today        + "today"
//	"all"   → zero Time (no cutoff)     + "all time"
//	"6h"    → now-6h (any Go duration)  + "last 6h"
func ResolveSince(spec string, now time.Time) (time.Time, time.Duration, string, error) {
	switch strings.ToLower(strings.TrimSpace(spec)) {
	case "today":
		y, m, d := now.Local().Date()
		return time.Date(y, m, d, 0, 0, 0, 0, now.Location()), 0, "today", nil
	case "all", "":
		return time.Time{}, 0, "all time", nil
	}
	d, err := time.ParseDuration(spec)
	if err != nil {
		return time.Time{}, 0, "", fmt.Errorf("expected 'today', 'all', or a duration like '6h' or '7d', got %q: %w", spec, err)
	}
	return now.Add(-d), d, "last " + compactDuration(d), nil
}

// Collect builds one Snapshot. Safe to call concurrently; no shared
// mutable state. Individual sections degrade gracefully when their
// source is missing.
func Collect(ctx context.Context, opts CollectOptions) Snapshot {
	rt := readRuntime(opts.StateDir)

	// Layer 1: explicit flag value, layer 2: runtime.yaml, layer 3: env,
	// layer 4: hardcoded default.
	if opts.WorktreesRoot == "" {
		if rt != nil && rt.WorktreesRoot != "" {
			opts.WorktreesRoot = rt.WorktreesRoot
		} else if v := strings.TrimSpace(os.Getenv("WORKTREES_ROOT")); v != "" {
			opts.WorktreesRoot = v
		}
	}
	opts.WorktreesRoot = expandPath(opts.WorktreesRoot)
	if opts.ClaudeHome == "" {
		switch {
		case rt != nil && rt.ClaudeHome != "":
			opts.ClaudeHome = rt.ClaudeHome
		case strings.TrimSpace(os.Getenv("CLAUDE_HOME")) != "":
			opts.ClaudeHome = strings.TrimSpace(os.Getenv("CLAUDE_HOME"))
		default:
			if home, err := os.UserHomeDir(); err == nil {
				opts.ClaudeHome = filepath.Join(home, ".claude")
			}
		}
	}
	opts.ClaudeHome = expandPath(opts.ClaudeHome)
	if opts.StaleAfter <= 0 {
		opts.StaleAfter = 1 * time.Hour
	}

	// Resolve the time cutoff. SinceCutoff is what we actually compare
	// against; Since is only used to compute one when SinceCutoff is
	// zero (caller used --since 6h or default).
	captured := time.Now()
	// Re-resolve the lookback window against the current time each refresh
	// so the cutoff slides with the clock instead of staying frozen at the
	// value computed when --watch launched.
	if opts.SinceSpec != "" {
		if c, dur, label, err := ResolveSince(opts.SinceSpec, captured); err == nil {
			opts.SinceCutoff = c
			opts.Since = dur
			opts.SinceLabel = label
		}
	}
	cutoff := opts.SinceCutoff
	if cutoff.IsZero() && opts.Since > 0 {
		cutoff = captured.Add(-opts.Since)
	}

	s := Snapshot{
		CapturedAt:         captured,
		StateDir:           opts.StateDir,
		WorktreesRoot:      opts.WorktreesRoot,
		Since:              opts.Since,
		StaleAfter:         opts.StaleAfter,
		SinceLabel:         opts.SinceLabel,
		PipelineTicks:      map[string]time.Duration{},
		PipelineCapacities: map[string]int{},
		PipelineBudgets:    map[string]BudgetStat{},
	}
	budgets := map[string]pipelinespec.Budget{}
	var globalBudget pipelinespec.Budget
	if rt != nil {
		s.ConfigPath = rt.ConfigPath
		s.MaxWorkers = rt.MaxWorkers
		globalBudget, _ = pipelinespec.ParseBudget(rt.Budget)
		for _, p := range rt.Pipelines {
			s.PipelineTicks[p.Name] = p.TickInterval
			s.PipelineCapacities[p.Name] = p.PoolSize
			s.PipelineOrder = append(s.PipelineOrder, p.Name)
			if b, err := pipelinespec.ParseBudget(p.Budget); err == nil && !b.IsZero() {
				budgets[p.Name] = b
			}
		}
	}

	s.collectOrchestrator()
	s.collectBackoff()
	s.collectAccounts()

	events := readEvents(opts.StateDir, cutoff)
	s.collectPipelines(events)
	s.collectTasks(events)
	s.collectSilentFailures(events)
	s.collectTokens(ctx, opts, cutoff)
	s.collectBudgets(opts.StateDir, captured, globalBudget, budgets)

	return s
}

// runtimeFile mirrors the orchestrator's on-disk shape; only the
// fields the monitor needs are decoded.
type runtimeFile struct {
	ConfigPath    string `yaml:"config_path"`
	WorktreesRoot string `yaml:"worktrees_root"`
	ClaudeHome    string `yaml:"claude_home"`
	MaxWorkers    int    `yaml:"max_workers"`
	Budget        string `yaml:"budget"`
	Pipelines     []struct {
		Name         string        `yaml:"name"`
		PoolSize     int           `yaml:"pool_size"`
		TickInterval time.Duration `yaml:"tick_interval"`
		Budget       string        `yaml:"budget"`
	} `yaml:"pipelines"`
}

// expandPath resolves leading ~/ and $VAR references — same trick the
// workers use, so values stored in runtime.yaml as literal "$HOME/..."
// (from a shell that single-quoted the export) still resolve.
func expandPath(p string) string {
	if p == "" {
		return p
	}
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			p = filepath.Join(home, p[2:])
		}
	} else if p == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			p = home
		}
	}
	return os.ExpandEnv(p)
}

func readRuntime(stateDir string) *runtimeFile {
	raw, err := os.ReadFile(filepath.Join(stateDir, "runtime.yaml"))
	if err != nil {
		return nil
	}
	var rt runtimeFile
	if err := yaml.Unmarshal(raw, &rt); err != nil {
		return nil
	}
	return &rt
}

const syntheticTaskID = "(no-task)"

// collectSilentFailures pulls out journal events the runner synthesised
// for workers that exited non-zero before claiming a task. These
// surface on a dedicated panel so the operator sees env / startup
// errors without tailing per-worker log files.
func (s *Snapshot) collectSilentFailures(events []journal.Event) {
	for _, ev := range events {
		if ev.TaskID != syntheticTaskID {
			continue
		}
		s.SilentFailures = append(s.SilentFailures, SilentFailure{
			When:     ev.Timestamp,
			Pipeline: ev.Pool,
			WorkerID: ev.WorkerID,
			Note:     ev.Note,
		})
	}
	// Most recent first — that's what an operator scanning will want.
	sort.Slice(s.SilentFailures, func(i, j int) bool {
		return s.SilentFailures[i].When.After(s.SilentFailures[j].When)
	})
}

func readEvents(stateRoot string, cutoff time.Time) []journal.Event {
	events, err := journal.Read(stateRoot, cutoff)
	if err != nil {
		return nil
	}
	return events
}

func (s *Snapshot) collectOrchestrator() {
	path := filepath.Join(s.StateDir, "brahma.lock")
	if lock.IsHeld(path) {
		s.Orchestrator.Alive = true
	}
	if info, err := os.Stat(path); err == nil {
		s.Orchestrator.Since = info.ModTime()
	}
	if raw, err := os.ReadFile(path); err == nil {
		s.Orchestrator.PID = strings.TrimSpace(string(raw))
	}
}

func (s *Snapshot) collectBackoff() {
	// New layout takes precedence; fall back to the legacy path so a
	// half-migrated state dir still surfaces the signal.
	for _, p := range []string{
		filepath.Join(s.StateDir, "agent-errors", "claude"),
		state.ClaudeErrorMarkerPath(s.StateDir),
	} {
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		s.Backoff.Active = true
		s.Backoff.WrittenAt = info.ModTime()
		return
	}
}

func (s *Snapshot) collectAccounts() {
	if a, err := state.ReadAccounts(s.StateDir); err == nil {
		s.Accounts = a
	}
}

// collectPipelines folds pipeline-level events by pool. Sub-events
// (Phase != Pool, e.g. exec:gh:pr-edit:add-label) are filtered — they
// would inflate Errors when one worker bursts a long retry chain.
//
// Running is NOT derived from the journal here — stale started events
// from crashed workers that never wrote a terminal would make the
// count exceed pool capacity. That number is computed afterward by
// collectRunningFromLogs, which uses the worker log file mtime as the
// "alive" signal.
func (s *Snapshot) collectPipelines(events []journal.Event) {
	per := map[string]*PipelineStat{}
	seenStart := map[string]map[string]bool{} // pool → workerID dedup
	for _, ev := range events {
		if ev.Phase != ev.Pool {
			continue
		}
		st, ok := per[ev.Pool]
		if !ok {
			st = &PipelineStat{Name: ev.Pool}
			per[ev.Pool] = st
			seenStart[ev.Pool] = map[string]bool{}
		}
		switch ev.Outcome {
		case journal.OutcomeStarted:
			if !seenStart[ev.Pool][ev.WorkerID] {
				st.Ran++
				seenStart[ev.Pool][ev.WorkerID] = true
			}
		case journal.OutcomeSucceeded:
			if ev.Timestamp.After(st.LastSuccess) {
				st.LastSuccess = ev.Timestamp
			}
		case journal.OutcomeFailed, journal.OutcomeTimedOut, journal.OutcomeDead:
			st.Errors++
			if ev.Timestamp.After(st.LastError) {
				st.LastError = ev.Timestamp
			}
		}
	}
	for _, st := range per {
		s.Pipelines = append(s.Pipelines, *st)
	}
	// Make sure every configured pipeline appears even when the journal
	// hasn't produced any pipeline-level events in the window — an idle
	// pipeline still has a tick / capacity worth showing.
	have := map[string]bool{}
	for _, p := range s.Pipelines {
		have[p.Name] = true
	}
	for _, name := range s.PipelineOrder {
		if !have[name] {
			s.Pipelines = append(s.Pipelines, PipelineStat{Name: name})
		}
	}
	s.collectRunningFromLogs()
	rank := make(map[string]int, len(s.PipelineOrder))
	for i, name := range s.PipelineOrder {
		rank[name] = i
	}
	sort.Slice(s.Pipelines, func(i, j int) bool {
		ri, iOK := rank[s.Pipelines[i].Name]
		rj, jOK := rank[s.Pipelines[j].Name]
		switch {
		case iOK && jOK:
			return ri < rj
		case iOK:
			return true
		case jOK:
			return false
		default:
			return s.Pipelines[i].Name < s.Pipelines[j].Name
		}
	})
}

// collectRunningFromLogs computes PipelineStat.Running per pool by
// counting workers whose sibling <worker_id>.pid file points to a
// live process. The orchestrator writes that file on spawn and
// removes it on exit, so its presence + a live PID is the
// authoritative "still running" signal. Falling back to log mtime
// over-counts workers that exited cleanly without writing a journal
// terminal (the most common idle-poll pattern).
func (s *Snapshot) collectRunningFromLogs() {
	root := filepath.Join(s.StateDir, "workers")
	pools, err := os.ReadDir(root)
	if err != nil {
		return
	}
	counts := map[string]int{}
	for _, pe := range pools {
		if !pe.IsDir() {
			continue
		}
		pool := pe.Name()
		files, err := os.ReadDir(filepath.Join(root, pool))
		if err != nil {
			continue
		}
		for _, fe := range files {
			if fe.IsDir() || !strings.HasSuffix(fe.Name(), ".pid") {
				continue
			}
			workerID := strings.TrimSuffix(fe.Name(), ".pid")
			pid, ok := readPIDFile(filepath.Join(root, pool, fe.Name()))
			if !ok {
				continue
			}
			if !state.PIDAlive(pid) {
				continue
			}
			_ = workerID // reserved for future per-worker correlation
			counts[pool]++
		}
	}
	for i := range s.Pipelines {
		s.Pipelines[i].Running = counts[s.Pipelines[i].Name]
	}
}

// readPIDFile returns the integer pid stored in path, or false on any
// error / non-numeric content. Tolerates a trailing newline.
func readPIDFile(path string) (int, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// collectTasks folds events by task_id into two lanes: the pipeline
// chain (one entry per pipeline, keyed by Phase == Pool, the runner's
// convention for pipeline-level start/terminal events) and task-scoped
// log lines (Phase == "log:<key>", surfaced under the header). The
// journal also carries fine-grained exec sub-events named like
// "exec:git:branch-delete"; those belong to neither lane and are
// dropped. Latest outcome and note win per phase and per log key, so
// both lanes reflect the most recent worker's state. The chain is
// ordered by the configured pipeline order so it reads as the pipeline
// regardless of the lookback window; phases outside that order (or when
// no orchestrator config is loaded) keep first-seen order, after the
// known ones.
func (s *Snapshot) collectTasks(events []journal.Event) {
	if len(events) == 0 {
		return
	}
	rank := make(map[string]int, len(s.PipelineOrder))
	for i, name := range s.PipelineOrder {
		rank[name] = i
	}
	type taskAgg struct {
		order    []string
		seen     map[string]bool
		outcomes map[string]journal.Outcome
		notes    map[string]string
		phaseAt  map[string]time.Time // newest event timestamp per chain phase
		logOrder []string
		logSeen  map[string]bool
		logLines map[string]TaskLogLine
		last     time.Time // newest event timestamp seen for this task
	}
	agg := map[string]*taskAgg{}
	for _, ev := range events {
		if ev.TaskID == syntheticTaskID {
			continue // surfaced separately in SilentFailures
		}
		isChain := ev.Phase == ev.Pool
		isLog := strings.HasPrefix(ev.Phase, taskLogPrefix)
		if !isChain && !isLog {
			continue // fine-grained exec:* sub-event; belongs to neither lane
		}
		t, ok := agg[ev.TaskID]
		if !ok {
			t = &taskAgg{
				seen:     map[string]bool{},
				outcomes: map[string]journal.Outcome{},
				notes:    map[string]string{},
				phaseAt:  map[string]time.Time{},
				logSeen:  map[string]bool{},
				logLines: map[string]TaskLogLine{},
			}
			agg[ev.TaskID] = t
		}
		if ev.Timestamp.After(t.last) {
			t.last = ev.Timestamp
		}
		if isChain {
			if !t.seen[ev.Phase] {
				t.seen[ev.Phase] = true
				t.order = append(t.order, ev.Phase)
			}
			t.outcomes[ev.Phase] = ev.Outcome
			t.notes[ev.Phase] = ev.Note
			if ev.Timestamp.After(t.phaseAt[ev.Phase]) {
				t.phaseAt[ev.Phase] = ev.Timestamp
			}
			continue
		}
		// task-scoped log line, keyed by the full "log:<key>" phase.
		if !t.logSeen[ev.Phase] {
			t.logSeen[ev.Phase] = true
			t.logOrder = append(t.logOrder, ev.Phase)
		}
		t.logLines[ev.Phase] = TaskLogLine{
			Key:     strings.TrimPrefix(ev.Phase, taskLogPrefix),
			Text:    ev.Note,
			Outcome: ev.Outcome,
		}
	}
	for taskID, t := range agg {
		ordered := append([]string(nil), t.order...)
		sort.SliceStable(ordered, func(i, j int) bool {
			return phaseRank(rank, ordered[i]) < phaseRank(rank, ordered[j])
		})
		chain := make([]PhaseStep, 0, len(ordered))
		for _, ph := range ordered {
			chain = append(chain, PhaseStep{Phase: ph, Outcome: t.outcomes[ph], Note: t.notes[ph], LastAt: t.phaseAt[ph]})
		}
		logs := make([]TaskLogLine, 0, len(t.logOrder))
		for _, key := range t.logOrder {
			if line := t.logLines[key]; line.Text != "" {
				logs = append(logs, line)
			}
		}
		s.Tasks = append(s.Tasks, TaskRow{TaskID: taskID, Chain: chain, Logs: logs, LastUpdate: t.last})
	}
	sort.Slice(s.Tasks, func(i, j int) bool { return s.Tasks[i].TaskID < s.Tasks[j].TaskID })
}

// phaseRank returns phase's index in the configured pipeline order, or a
// sentinel one past the end for phases absent from it — so unknown phases
// sort after the known ones while a stable sort preserves their first-seen
// order among themselves.
func phaseRank(rank map[string]int, phase string) int {
	if r, ok := rank[phase]; ok {
		return r
	}
	return len(rank)
}

// collectTokens rolls up the cost and usage workers reported in the
// journal via internal/tokens. ClaudeHome is optional — it only enables
// the tokens-only jsonl fallback for sessions that never reported.
// Failures leave Tokens.Available=false; the panel renders a single
// explanatory line.
func (s *Snapshot) collectTokens(ctx context.Context, opts CollectOptions, cutoff time.Time) {
	if opts.StateDir == "" {
		s.Tokens.Note = "skipped — need a state dir"
		return
	}
	subCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := tokens.Collect(subCtx, tokens.Options{
		StateRoot:  opts.StateDir,
		ClaudeHome: opts.ClaudeHome,
		Since:      cutoff,
	})
	if err != nil {
		s.Tokens.Note = "collect failed: " + err.Error()
		return
	}
	s.Tokens.Available = true
	s.Tokens.CostAvailable = res.CostAvailable
	s.Tokens.ByPhase = res.ByPhase
	s.Tokens.ByTask = res.ByTask
	s.Tokens.Note = fmt.Sprintf("last %s", opts.Since)
}

// collectBudgets evaluates every configured rolling budget at now. It
// re-reads the journal over the longest budget window, which may reach
// further back than the panel's lookback cutoff.
func (s *Snapshot) collectBudgets(stateDir string, now time.Time, global pipelinespec.Budget, perPipeline map[string]pipelinespec.Budget) {
	if stateDir == "" || (global.IsZero() && len(perPipeline) == 0) {
		return
	}
	window := global.Window
	for _, b := range perPipeline {
		if b.Window > window {
			window = b.Window
		}
	}
	events, err := journal.Read(stateDir, now.Add(-window))
	if err != nil {
		return
	}
	toStat := func(b pipelinespec.Budget, pool string) BudgetStat {
		var l budget.Ledger
		l.AddEvents(events, pool)
		st := l.Status(b, now)
		return BudgetStat{Limit: b, Spent: st.Spent, Exceeded: st.Exceeded, ResetAt: st.ResetAt}
	}
	if !global.IsZero() {
		g := toStat(global, "")
		s.GlobalBudget = &g
	}
	for name, b := range perPipeline {
		s.PipelineBudgets[name] = toStat(b, name)
	}
}
