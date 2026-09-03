package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
	"github.com/grasskode/brahmanda/internal/pipelinespec"
)

// poolProbe stands in for spawnWorker: it records how many workers are
// concurrently "running" (globally and per pipeline) so a test can
// assert the scheduler never exceeds the global cap or a pipeline's
// pool_size. Each fake worker blocks until ctx is cancelled.
type poolProbe struct {
	mu      sync.Mutex
	running int
	maxSeen int
	perName map[string]int
	perMax  map[string]int
}

func newPoolProbe() *poolProbe {
	return &poolProbe{perName: map[string]int{}, perMax: map[string]int{}}
}

func (p *poolProbe) spawn(ctx context.Context, _ *slog.Logger, spec pipelinespec.Spec, _ int, _ string) {
	p.enter(spec.Name)
	defer p.exit(spec.Name)
	<-ctx.Done()
}

func (p *poolProbe) enter(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running++
	if p.running > p.maxSeen {
		p.maxSeen = p.running
	}
	p.perName[name]++
	if p.perName[name] > p.perMax[name] {
		p.perMax[name] = p.perName[name]
	}
}

func (p *poolProbe) exit(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running--
	p.perName[name]--
}

func (p *poolProbe) snapshot() (running, maxSeen int, perMax map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := make(map[string]int, len(p.perMax))
	for k, v := range p.perMax {
		m[k] = v
	}
	return p.running, p.maxSeen, m
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// startScheduler runs s until the returned stop func is called (which
// cancels and waits for a clean drain).
func startScheduler(t *testing.T, s *scheduler) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.run(ctx)
		close(done)
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("scheduler did not drain after cancel")
		}
	}
}

// runProbe wires a probe into a scheduler and runs it until the returned
// stop func is called.
func runProbe(t *testing.T, maxWorkers int, specs []pipelinespec.Spec) (*poolProbe, func()) {
	t.Helper()
	s := newScheduler(discardLogger(), t.TempDir(), "srishti", maxWorkers, specs, nil)
	probe := newPoolProbe()
	s.spawn = probe.spawn
	return probe, startScheduler(t, s)
}

// The global pool caps total concurrency at max_workers even when every
// pipeline has spare pool_size and is perpetually eligible.
func TestScheduler_GlobalCapNeverExceeded(t *testing.T) {
	specs := []pipelinespec.Spec{
		{Name: "a", PoolSize: 5, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
		{Name: "b", PoolSize: 5, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
		{Name: "c", PoolSize: 5, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
	}
	const maxWorkers = 4
	probe, stop := runProbe(t, maxWorkers, specs)
	defer stop()

	waitFor(t, 2*time.Second, func() bool {
		running, _, _ := probe.snapshot()
		return running == maxWorkers
	})
	// Let more ticks fire; a broken cap would over-admit in this window.
	time.Sleep(50 * time.Millisecond)

	running, maxSeen, _ := probe.snapshot()
	if maxSeen > maxWorkers {
		t.Fatalf("global cap exceeded: maxSeen=%d, want <= %d", maxSeen, maxWorkers)
	}
	if running != maxWorkers {
		t.Fatalf("pool did not stay full: running=%d, want %d", running, maxWorkers)
	}
}

// Round-robin fills the global pool across pipelines without any pipeline
// exceeding its own pool_size, and without a large-pool pipeline starving
// a small-pool one.
func TestScheduler_PerPipelinePoolSizeAndFairness(t *testing.T) {
	specs := []pipelinespec.Spec{
		{Name: "small", PoolSize: 1, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
		{Name: "big", PoolSize: 3, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
	}
	const maxWorkers = 4 // == small(1) + big(3)
	probe, stop := runProbe(t, maxWorkers, specs)
	defer stop()

	waitFor(t, 2*time.Second, func() bool {
		running, _, _ := probe.snapshot()
		return running == maxWorkers
	})
	time.Sleep(50 * time.Millisecond)

	_, maxSeen, perMax := probe.snapshot()
	if maxSeen > maxWorkers {
		t.Fatalf("global cap exceeded: %d > %d", maxSeen, maxWorkers)
	}
	if perMax["small"] != 1 {
		t.Fatalf("small pipeline: perMax=%d, want exactly 1 (pool_size honoured and not starved)", perMax["small"])
	}
	if perMax["big"] != 3 {
		t.Fatalf("big pipeline: perMax=%d, want exactly 3 (its full pool_size)", perMax["big"])
	}
}

// spendProbe stands in for a worker that costs money: every spawn
// journals one agent event carrying costUSD under its worker id — the
// same file the scheduler reads back to fold spend into its ledgers —
// and returns immediately.
type spendProbe struct {
	stateDir string
	costUSD  float64
	mu       sync.Mutex
	spawns   map[string]int
}

func newSpendProbe(stateDir string, costUSD float64) *spendProbe {
	return &spendProbe{stateDir: stateDir, costUSD: costUSD, spawns: map[string]int{}}
}

func (p *spendProbe) spawn(_ context.Context, _ *slog.Logger, spec pipelinespec.Spec, _ int, workerID string) {
	p.mu.Lock()
	p.spawns[spec.Name]++
	p.mu.Unlock()
	_ = journal.Append(p.stateDir, journal.Event{
		TaskID:   "t",
		WorkerID: workerID,
		Pool:     spec.Name,
		Phase:    spec.Name,
		Outcome:  journal.OutcomeSucceeded,
		Agent:    &journal.AgentSession{Runtime: "claude", SessionID: workerID, CostUSD: p.costUSD},
	})
}

func (p *spendProbe) count(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.spawns[name]
}

func hourly(amount float64) pipelinespec.Budget {
	return pipelinespec.Budget{Amount: amount, Window: time.Hour}
}

// A pipeline whose reported spend reaches its budget stops being launched
// — without an error, and with the reset instant recorded for the
// monitor and the wake timer.
func TestScheduler_BudgetHoldsLaunches(t *testing.T) {
	stateDir := t.TempDir()
	specs := []pipelinespec.Spec{
		{Name: "paid", PoolSize: 1, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x", Budget: hourly(1)},
	}
	s := newScheduler(discardLogger(), stateDir, "srishti", 4, specs, nil)
	probe := newSpendProbe(stateDir, 0.6)
	s.spawn = probe.spawn
	stop := startScheduler(t, s)
	defer stop()

	// Two runs (1.2 USD) cross the 1 USD/h cap; a third must never start.
	waitFor(t, 2*time.Second, func() bool { return probe.count("paid") >= 2 })
	time.Sleep(50 * time.Millisecond)
	if got := probe.count("paid"); got != 2 {
		t.Fatalf("spawns after budget exhausted: got %d, want exactly 2", got)
	}

	s.mu.Lock()
	held, resetAt := s.held[0], s.resetAt[0]
	s.mu.Unlock()
	if !held {
		t.Fatal("pipeline should be recorded as budget-held")
	}
	if until := time.Until(resetAt); until <= 0 || until > time.Hour {
		t.Fatalf("resetAt should fall within the next hour, got %s from now", until)
	}
	// The loop must not spin while held: the next wake is the reset instant.
	if d, ok := s.nextWakeDelay(time.Now()); !ok || d < 50*time.Minute {
		t.Fatalf("nextWakeDelay while held = (%s, %v), want ~1h", d, ok)
	}
}

// Spend already in the journal counts on startup, so a restart cannot
// reset a budget that should still be holding.
func TestScheduler_BudgetSeedsFromJournal(t *testing.T) {
	stateDir := t.TempDir()
	if err := journal.Append(stateDir, journal.Event{
		Timestamp: time.Now().Add(-10 * time.Minute),
		TaskID:    "t", WorkerID: "old1", Pool: "paid", Phase: "paid", Outcome: journal.OutcomeSucceeded,
		Agent: &journal.AgentSession{Runtime: "claude", SessionID: "s", CostUSD: 2},
	}); err != nil {
		t.Fatal(err)
	}
	specs := []pipelinespec.Spec{
		{Name: "paid", PoolSize: 1, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x", Budget: hourly(1)},
		{Name: "free", PoolSize: 1, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
	}
	s := newScheduler(discardLogger(), stateDir, "srishti", 4, specs, nil)
	probe := newSpendProbe(stateDir, 0.1)
	s.spawn = probe.spawn
	stop := startScheduler(t, s)
	defer stop()

	// The unbudgeted pipeline runs freely; the seeded one never starts.
	waitFor(t, 2*time.Second, func() bool { return probe.count("free") >= 3 })
	if got := probe.count("paid"); got != 0 {
		t.Fatalf("budget-held pipeline launched %d time(s) after seeding from journal", got)
	}
}

// The global budget holds every pipeline once their combined spend
// reaches it, and lifts again once the spend ages out of the window.
func TestScheduler_GlobalBudgetAndReset(t *testing.T) {
	stateDir := t.TempDir()
	specs := []pipelinespec.Spec{
		{Name: "a", PoolSize: 1, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
		{Name: "b", PoolSize: 1, TickInterval: time.Millisecond, StepTimeout: time.Minute, Command: "x"},
	}
	s := newScheduler(discardLogger(), stateDir, "srishti", 4, specs, nil)
	s.globalBudget = hourly(1)
	probe := newSpendProbe(stateDir, 0.6)
	s.spawn = probe.spawn
	stop := startScheduler(t, s)

	waitFor(t, 2*time.Second, func() bool { return probe.count("a")+probe.count("b") >= 2 })
	time.Sleep(50 * time.Millisecond)
	if got := probe.count("a") + probe.count("b"); got != 2 {
		t.Fatalf("global budget: %d spawns across pipelines, want exactly 2", got)
	}
	stop()

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.globalHeld {
		t.Fatal("global budget should be recorded as held")
	}
	// Evaluating past the reset instant lifts the hold and logs the
	// restore transition.
	if s.budgetHeldLocked(0, s.globalReset.Add(time.Second)) {
		t.Fatal("global hold should lift once the spend has aged out")
	}
	if s.globalHeld {
		t.Fatal("held flag should clear after the hold lifts")
	}
}
