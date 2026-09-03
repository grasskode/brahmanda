// Command brahma is the config-driven orchestrator — the creator that
// spawns and keeps every worker pool full. One YAML file describes both
// the state directory and the pipelines to run; each pipeline lazy-fills
// its pool by exec'ing the srishti runner binary every tick_interval.
// brahma itself never executes worker code — it spawns srishti
// subprocesses and tracks counts.
//
// Invocation:
//
//	brahma <config.yaml>
//
// srishti (runner) binary lookup (in order):
//
//  1. $SRISHTI_BIN — exact path
//  2. Sibling of this binary (`<dir>/srishti`)
//  3. `srishti` on $PATH
//
// Single-instance enforced via flock on <state_dir>/brahma.lock.
// SIGINT or SIGTERM stops new spawns and drains in-flight runners
// (each bounded by its own step_timeout) before exit.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/grasskode/brahmanda/internal/budget"
	"github.com/grasskode/brahmanda/internal/journal"
	"github.com/grasskode/brahmanda/internal/lock"
	"github.com/grasskode/brahmanda/internal/pipelinespec"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: brahma <config.yaml>")
		os.Exit(2)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "brahma: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := pipelinespec.LoadFile(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	// Overlay the env_file values onto brahma's own environment so its
	// runtime env matches what workers see. Anything reading os.Getenv
	// afterwards (runtime.yaml, diagnostics) resolves config-declared
	// vars, not just those exported in brahma's shell.
	if err := applyConfigEnv(cfg); err != nil {
		return fmt.Errorf("apply env_file: %w", err)
	}
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	runnerPath, err := resolveRunner()
	if err != nil {
		return err
	}

	logOut := io.Writer(os.Stderr)
	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("open log file: %w", err)
		}
		defer f.Close()
		logOut = f
	}
	log := slog.New(slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo}))

	lockPath := filepath.Join(cfg.StateDir, "brahma.lock")
	held, err := lock.Acquire(lockPath)
	if err != nil {
		if errors.Is(err, lock.ErrHeld) {
			return fmt.Errorf("another brahma is running (lock: %s)", lockPath)
		}
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer held.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Write runtime.yaml so the monitor can auto-discover everything it
	// needs (worktrees root, claude home, per-pipeline tick) without the
	// operator passing flags. Removed on graceful shutdown.
	if err := writeRuntimeFile(cfg, configPath, runnerPath); err != nil {
		log.Warn("write runtime.yaml failed (continuing)", "err", err)
	}
	defer removeRuntimeFile(cfg.StateDir)

	workerEnv := cfg.WorkerEnvPairs()

	log.Info("brahma starting",
		"config", configPath,
		"state_dir", cfg.StateDir,
		"pipelines", len(cfg.Pipelines),
		"max_workers", cfg.MaxWorkers,
		"runner", runnerPath,
		"env_file", cfg.EnvFile,
		"worker_env_vars", len(workerEnv),
		"pid", os.Getpid(),
	)

	// Reconcile workers stranded by a prior generation's hard kill before
	// seeding pools, so they read as `dead` rather than "in progress"
	// forever. Holding the lock guarantees we're the only orchestrator.
	reapOrphans(log, cfg.StateDir)

	sched := newScheduler(log, cfg.StateDir, runnerPath, cfg.MaxWorkers, cfg.Pipelines, workerEnv)
	sched.globalBudget = cfg.Budget
	sched.run(ctx)
	log.Info("orchestrator stopped")
	return nil
}

// spawnFunc launches one worker and blocks until it exits. The
// scheduler's default runs the srishti runner via spawnWorker; tests
// substitute a stub to exercise admission without real subprocesses.
type spawnFunc func(ctx context.Context, pLog *slog.Logger, spec pipelinespec.Spec, slot int, workerID string)

// scheduler owns global worker admission. A single loop hands out the
// max_workers global slots to pipelines round-robin: a pipeline is
// eligible for a slot when it is running fewer than its pool_size
// workers, at least its tick_interval has elapsed since it last
// spawned one, and neither its own rolling budget nor the global one is
// exhausted. Each pass fills greedily — it keeps handing out free
// slots in rotation until the global cap is reached or no pipeline is
// eligible — so the pool ramps to capacity without any single pipeline
// monopolising it.
type scheduler struct {
	log          *slog.Logger
	stateDir     string
	maxWorkers   int
	specs        []pipelinespec.Spec
	globalBudget pipelinespec.Budget
	spawn        spawnFunc

	mu            sync.Mutex
	globalRunning int
	running       []int       // per-spec concurrent worker count
	slots         [][]bool    // per-spec slot occupancy; len == pool_size
	nextEligible  []time.Time // per-spec earliest next spawn time
	cursor        int         // round-robin start index for the next pass

	// Spend ledgers feed the rolling budgets: one per spec plus one across
	// all pipelines. A worker's reported cost is folded in when it exits.
	// held/resetAt mirror the last evaluation so transitions are logged
	// once and the loop can sleep until a budget frees up.
	ledgers      []*budget.Ledger
	globalLedger *budget.Ledger
	held         []bool
	resetAt      []time.Time
	globalHeld   bool
	globalReset  time.Time

	wakeup chan struct{}  // buffered(1); a finishing worker pokes it
	wg     sync.WaitGroup // tracks in-flight worker goroutines
}

func newScheduler(log *slog.Logger, stateDir, runnerPath string, maxWorkers int, specs []pipelinespec.Spec, workerEnv []string) *scheduler {
	s := &scheduler{
		log:          log,
		stateDir:     stateDir,
		maxWorkers:   maxWorkers,
		specs:        specs,
		running:      make([]int, len(specs)),
		slots:        make([][]bool, len(specs)),
		nextEligible: make([]time.Time, len(specs)),
		ledgers:      make([]*budget.Ledger, len(specs)),
		globalLedger: &budget.Ledger{},
		held:         make([]bool, len(specs)),
		resetAt:      make([]time.Time, len(specs)),
		wakeup:       make(chan struct{}, 1),
	}
	for i := range specs {
		s.slots[i] = make([]bool, specs[i].PoolSize)
		s.ledgers[i] = &budget.Ledger{}
	}
	s.spawn = func(ctx context.Context, pLog *slog.Logger, spec pipelinespec.Spec, slot int, workerID string) {
		spawnWorker(ctx, pLog, stateDir, runnerPath, spec, slot, workerID, workerEnv)
	}
	return s
}

// run drives the scheduling loop until ctx is cancelled, then waits for
// in-flight workers to drain before returning.
func (s *scheduler) run(ctx context.Context) {
	for i := range s.specs {
		s.log.Info("pipeline ready",
			"pipeline", s.specs[i].Name,
			"pool_size", s.specs[i].PoolSize,
			"tick_interval", s.specs[i].TickInterval,
			"step_timeout", s.specs[i].StepTimeout,
			"budget", s.specs[i].Budget.String(),
		)
	}
	s.log.Info("global pool ready", "max_workers", s.maxWorkers, "budget", s.globalBudget.String())
	s.seedLedgers(time.Now())

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	for {
		s.pass(ctx)

		var timerC <-chan time.Time
		if d, ok := s.nextWakeDelay(time.Now()); ok {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			s.log.Info("scheduler draining", "running", s.runningCount())
			s.wg.Wait()
			s.log.Info("scheduler drained")
			return
		case <-s.wakeup:
		case <-timerC:
		}
	}
}

// pass hands out every free global slot it can, round-robin, to
// eligible pipelines. It holds mu for the whole scan; spawnLocked only
// starts a goroutine, so the critical section stays short.
func (s *scheduler) pass(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for s.globalRunning < s.maxWorkers {
		idx, ok := s.pickLocked(now)
		if !ok {
			return
		}
		s.spawnLocked(ctx, idx, now)
	}
}

// pickLocked returns the next eligible pipeline in round-robin order
// starting from the cursor, or ok=false when none is eligible. A
// pipeline is eligible when it has a free pool slot and its tick_interval
// has elapsed since it last spawned.
func (s *scheduler) pickLocked(now time.Time) (int, bool) {
	n := len(s.specs)
	for i := 0; i < n; i++ {
		idx := (s.cursor + i) % n
		if s.running[idx] >= s.specs[idx].PoolSize {
			continue
		}
		if now.Before(s.nextEligible[idx]) {
			continue
		}
		if s.budgetHeldLocked(idx, now) {
			continue
		}
		return idx, true
	}
	return 0, false
}

// budgetHeldLocked reports whether pipeline idx must not launch right
// now because its own rolling budget or the global one is exhausted.
// It logs each transition once — a warning naming the reset instant when
// a cap starts holding launches, an info line when it lifts — so a held
// pipeline is visible without spamming every pass. Caller must hold mu.
func (s *scheduler) budgetHeldLocked(idx int, now time.Time) bool {
	held := false
	if !s.globalBudget.IsZero() {
		st := s.globalLedger.Status(s.globalBudget, now)
		s.logBudgetTransition("all pipelines", s.globalBudget, st, &s.globalHeld, &s.globalReset)
		held = st.Exceeded
	}
	if b := s.specs[idx].Budget; !b.IsZero() {
		st := s.ledgers[idx].Status(b, now)
		s.logBudgetTransition(s.specs[idx].Name, b, st, &s.held[idx], &s.resetAt[idx])
		held = held || st.Exceeded
	}
	return held
}

func (s *scheduler) logBudgetTransition(scope string, b pipelinespec.Budget, st budget.Status, held *bool, resetAt *time.Time) {
	*resetAt = st.ResetAt
	switch {
	case st.Exceeded && !*held:
		s.log.Warn("budget exceeded — holding launches",
			"scope", scope,
			"budget", b.String(),
			"spent_usd", fmt.Sprintf("%.2f", st.Spent),
			"resets_at", st.ResetAt.Local().Format(time.RFC3339),
			"resets_in", time.Until(st.ResetAt).Round(time.Second),
		)
	case !st.Exceeded && *held:
		s.log.Info("budget restored — launches resume",
			"scope", scope,
			"budget", b.String(),
			"spent_usd", fmt.Sprintf("%.2f", st.Spent),
		)
	}
	*held = st.Exceeded
}

// seedLedgers loads the cost workers reported inside the longest budget
// window from the journal, so a restarted orchestrator does not forget
// spend that should still count. No-op when no budget is configured.
func (s *scheduler) seedLedgers(now time.Time) {
	window := s.globalBudget.Window
	for _, spec := range s.specs {
		if spec.Budget.Window > window {
			window = spec.Budget.Window
		}
	}
	if window <= 0 {
		return
	}
	events, err := journal.Read(s.stateDir, now.Add(-window))
	if err != nil {
		s.log.Warn("seed budget ledgers from journal failed (starting empty)", "err", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.globalLedger.AddEvents(events, "")
	for i, spec := range s.specs {
		s.ledgers[i].AddEvents(events, spec.Name)
	}
}

// recordSpendLocked folds the cost a finished worker reported into its
// pipeline's ledger and the global one, reading the worker's own journal
// file. Ledgers are pruned to the longest window so they stay bounded.
// Caller must hold mu.
func (s *scheduler) recordSpendLocked(idx int, workerID string, now time.Time) {
	if s.globalBudget.IsZero() && s.specs[idx].Budget.IsZero() {
		return
	}
	f, err := os.Open(journal.FileFor(s.stateDir, s.specs[idx].Name, workerID))
	if err != nil {
		return // worker journaled nothing (idle no-op) — no spend to record
	}
	defer f.Close()
	events := journal.ReadFrom(f, time.Time{})
	s.globalLedger.AddEvents(events, "")
	s.ledgers[idx].AddEvents(events, s.specs[idx].Name)

	window := s.globalBudget.Window
	for _, spec := range s.specs {
		if spec.Budget.Window > window {
			window = spec.Budget.Window
		}
	}
	s.globalLedger.Prune(now.Add(-window))
	s.ledgers[idx].Prune(now.Add(-window))
}

// spawnLocked reserves a slot for pipeline idx, advances the round-robin
// cursor past it, and launches the worker goroutine. When the worker
// exits it releases the slot and pokes the loop so the freed global slot
// is refilled promptly. Caller must hold mu.
func (s *scheduler) spawnLocked(ctx context.Context, idx int, now time.Time) {
	spec := s.specs[idx]
	slot := s.takeSlotLocked(idx)
	s.running[idx]++
	s.globalRunning++
	s.nextEligible[idx] = now.Add(spec.TickInterval)
	s.cursor = (idx + 1) % len(s.specs)

	workerID := uuid.NewString()[:8]
	pLog := s.log.With("pipeline", spec.Name)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.spawn(ctx, pLog, spec, slot, workerID)
		s.mu.Lock()
		s.running[idx]--
		s.globalRunning--
		s.freeSlotLocked(idx, slot)
		s.recordSpendLocked(idx, workerID, time.Now())
		s.mu.Unlock()
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}()
}

// nextWakeDelay returns how long until the loop should wake to fill a
// slot that isn't fillable right now: the soonest tick_interval boundary
// of a pipeline that still has pool room. Returns ok=false when the
// global pool is full (a finishing worker will poke the loop) or no
// pipeline has room.
func (s *scheduler) nextWakeDelay(now time.Time) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.globalRunning >= s.maxWorkers {
		return 0, false
	}
	var soonest time.Time
	found := false
	for idx := range s.specs {
		if s.running[idx] >= s.specs[idx].PoolSize {
			continue
		}
		t := s.nextEligible[idx]
		// A held budget pushes the wake past its reset instant; the pass
		// re-evaluates then, when enough spend has aged out.
		if s.globalHeld && s.globalReset.After(t) {
			t = s.globalReset
		}
		if s.held[idx] && s.resetAt[idx].After(t) {
			t = s.resetAt[idx]
		}
		if !t.After(now) {
			return 0, true // eligible now — wake immediately
		}
		if !found || t.Before(soonest) {
			soonest = t
			found = true
		}
	}
	if !found {
		return 0, false
	}
	d := time.Until(soonest)
	if d < 0 {
		d = 0
	}
	return d, true
}

// takeSlotLocked reserves and returns the lowest free pool slot for
// pipeline idx (0..pool_size-1), exposed to the worker as
// AGENT_WORKER_INDEX. Caller must hold mu and have checked pool room.
func (s *scheduler) takeSlotLocked(idx int) int {
	for i, used := range s.slots[idx] {
		if !used {
			s.slots[idx][i] = true
			return i
		}
	}
	return 0 // unreachable: caller checked running < pool_size
}

func (s *scheduler) freeSlotLocked(idx, slot int) {
	if slot >= 0 && slot < len(s.slots[idx]) {
		s.slots[idx][slot] = false
	}
}

func (s *scheduler) runningCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.globalRunning
}

// spawnWorker execs the runner binary for one pool slot. Its stdout
// and stderr go to <state_dir>/workers/<pipeline>/<worker_id>.log;
// structured outcomes land in the journal via the runner.
//
// A sibling <worker_id>.pid file holds the runner PID for the
// monitor's liveness check, removed on exit so a stale file can't
// claim a now-recycled PID belongs to a worker.
func spawnWorker(ctx context.Context, log *slog.Logger, stateDir, runnerPath string, spec pipelinespec.Spec, slot int, workerID string, workerEnv []string) {
	wLog := log.With("worker", workerID, "slot", slot)

	workerDir := filepath.Join(stateDir, "workers", spec.Name)
	if err := os.MkdirAll(workerDir, 0o755); err != nil {
		wLog.Error("create worker dir", "err", err)
		return
	}
	logPath := filepath.Join(workerDir, workerID+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		wLog.Error("create worker log", "err", err)
		return
	}
	defer logFile.Close()

	args := []string{
		"-pipeline", spec.Name,
		"-worker-id", workerID,
		"-state-root", stateDir,
		"-command", spec.Command,
		"-timeout", spec.StepTimeout.String(),
		"-worker-index", fmt.Sprintf("%d", slot),
	}
	cmd := exec.CommandContext(ctx, runnerPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Overlay the config-declared env on top of brahma's inherited
	// process environment; config wins on key collisions. srishti
	// inherits this and passes it to the worker (then adds AGENT_*).
	cmd.Env = workerEnviron(workerEnv)

	start := time.Now()
	if err := cmd.Start(); err != nil {
		wLog.Error("spawn runner", "err", err)
		return
	}
	pidPath := filepath.Join(workerDir, workerID+".pid")
	if err := os.WriteFile(pidPath, []byte(fmt.Sprintf("%d\n", cmd.Process.Pid)), 0o644); err != nil {
		wLog.Warn("write pid file", "err", err)
	}
	defer func() {
		if err := os.Remove(pidPath); err != nil && !os.IsNotExist(err) {
			wLog.Warn("remove pid file", "err", err)
		}
	}()
	wLog.Info("worker spawned", "pid", cmd.Process.Pid, "log", logPath)

	err = cmd.Wait()
	elapsed := time.Since(start).Round(time.Millisecond)
	if err != nil {
		wLog.Warn("worker finished non-zero", "elapsed", elapsed, "err", err)
		return
	}
	wLog.Info("worker finished", "elapsed", elapsed)
}

// runtimeFile is the metadata the monitor reads to auto-discover what
// the running orchestrator is configured with — so operators don't
// have to pass flags or env to chitra separately. Written on
// startup, removed on graceful shutdown.
type runtimeFile struct {
	ConfigPath    string            `yaml:"config_path"`
	StateDir      string            `yaml:"state_dir"`
	RunnerBin     string            `yaml:"runner_bin"`
	WorktreesRoot string            `yaml:"worktrees_root"`
	ClaudeHome    string            `yaml:"claude_home"`
	MaxWorkers    int               `yaml:"max_workers"`
	Budget        string            `yaml:"budget,omitempty"`
	StartedAt     time.Time         `yaml:"started_at"`
	PID           int               `yaml:"pid"`
	Pipelines     []runtimePipeline `yaml:"pipelines"`
}

type runtimePipeline struct {
	Name         string        `yaml:"name"`
	PoolSize     int           `yaml:"pool_size"`
	TickInterval time.Duration `yaml:"tick_interval"`
	StepTimeout  time.Duration `yaml:"step_timeout"`
	Budget       string        `yaml:"budget,omitempty"`
}

func runtimeFilePath(stateDir string) string {
	return filepath.Join(stateDir, "runtime.yaml")
}

func writeRuntimeFile(cfg *pipelinespec.Config, configPath, runnerPath string) error {
	r := runtimeFile{
		ConfigPath:    configPath,
		StateDir:      cfg.StateDir,
		RunnerBin:     runnerPath,
		WorktreesRoot: strings.TrimSpace(os.Getenv("WORKTREES_ROOT")),
		ClaudeHome:    strings.TrimSpace(os.Getenv("CLAUDE_HOME")),
		MaxWorkers:    cfg.MaxWorkers,
		Budget:        cfg.Budget.String(),
		StartedAt:     time.Now().UTC(),
		PID:           os.Getpid(),
	}
	for _, p := range cfg.Pipelines {
		r.Pipelines = append(r.Pipelines, runtimePipeline{
			Name:         p.Name,
			PoolSize:     p.PoolSize,
			TickInterval: p.TickInterval,
			StepTimeout:  p.StepTimeout,
			Budget:       p.Budget.String(),
		})
	}
	body, err := yaml.Marshal(r)
	if err != nil {
		return err
	}
	return os.WriteFile(runtimeFilePath(cfg.StateDir), body, 0o644)
}

func removeRuntimeFile(stateDir string) {
	_ = os.Remove(runtimeFilePath(stateDir))
}

// applyConfigEnv exports the env_file-declared values into brahma's own
// process environment, so os.Getenv resolves them the same way workers
// do. Config values take precedence over the inherited shell environment,
// matching workerEnviron's overlay ordering. No-op when no env_file was
// configured.
func applyConfigEnv(cfg *pipelinespec.Config) error {
	for k, v := range cfg.Env {
		if err := os.Setenv(k, v); err != nil {
			return fmt.Errorf("set %s: %w", k, err)
		}
	}
	return nil
}

// workerEnviron returns brahma's process environment with extra (the
// config-declared KEY=value pairs) overlaid on top, so a config value
// wins over an inherited one. Deduped by key — glibc's getenv returns
// the first match, so a plain append wouldn't reliably override.
func workerEnviron(extra []string) []string {
	if len(extra) == 0 {
		return os.Environ()
	}
	idx := map[string]int{}
	var out []string
	add := func(pair string) {
		key, _, ok := strings.Cut(pair, "=")
		if !ok {
			return
		}
		if i, seen := idx[key]; seen {
			out[i] = pair
			return
		}
		idx[key] = len(out)
		out = append(out, pair)
	}
	for _, p := range os.Environ() {
		add(p)
	}
	for _, p := range extra {
		add(p)
	}
	return out
}

// resolveRunner finds the srishti runner binary. Priority: $SRISHTI_BIN,
// then a sibling next to the brahma binary, then $PATH lookup.
func resolveRunner() (string, error) {
	if p := strings.TrimSpace(os.Getenv("SRISHTI_BIN")); p != "" {
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("SRISHTI_BIN=%s not usable: %w", p, err)
		}
		return p, nil
	}
	if exe, err := os.Executable(); err == nil {
		sibling := filepath.Join(filepath.Dir(exe), "srishti")
		if _, err := os.Stat(sibling); err == nil {
			return sibling, nil
		}
	}
	if p, err := exec.LookPath("srishti"); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("srishti binary not found (set SRISHTI_BIN, place it as a sibling of the brahma binary, or put it on PATH)")
}
