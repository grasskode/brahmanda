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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/grasskode/bramha/internal/lock"
	"github.com/grasskode/bramha/internal/pipelinespec"
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
		"runner", runnerPath,
		"env_file", cfg.EnvFile,
		"worker_env_vars", len(workerEnv),
		"pid", os.Getpid(),
	)

	// Reconcile workers stranded by a prior generation's hard kill before
	// seeding pools, so they read as `dead` rather than "in progress"
	// forever. Holding the lock guarantees we're the only orchestrator.
	reapOrphans(log, cfg.StateDir)

	var wg sync.WaitGroup
	for i := range cfg.Pipelines {
		spec := cfg.Pipelines[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			runPipeline(ctx, log, logOut, cfg.StateDir, runnerPath, spec, workerEnv)
		}()
	}
	wg.Wait()
	log.Info("orchestrator stopped")
	return nil
}

// runPipeline drives one pipeline's spawn loop. Fires once immediately
// at startup, then once per tick_interval. Each fire spawns one runner
// if running < pool_size; otherwise it's a no-op. On context cancel,
// stops firing and waits for in-flight runners to drain.
func runPipeline(ctx context.Context, log *slog.Logger, logOut io.Writer, stateDir, runnerPath string, spec pipelinespec.Spec, workerEnv []string) {
	pLog := log.With("pipeline", spec.Name)
	pLog.Info("pipeline ready",
		"pool_size", spec.PoolSize,
		"tick_interval", spec.TickInterval,
		"step_timeout", spec.StepTimeout,
	)

	var running atomic.Int64
	var workers sync.WaitGroup

	fire := func() {
		cur := running.Load()
		if cur >= int64(spec.PoolSize) {
			return
		}
		running.Add(1)
		workers.Add(1)
		slot := int(cur)
		go func() {
			defer workers.Done()
			defer running.Add(-1)
			spawnWorker(ctx, pLog, logOut, stateDir, runnerPath, spec, slot, workerEnv)
		}()
	}

	fire() // seed the pool so the first worker doesn't wait one full tick

	ticker := time.NewTicker(spec.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			pLog.Info("pipeline draining", "running", running.Load())
			workers.Wait()
			pLog.Info("pipeline drained")
			return
		case <-ticker.C:
			fire()
		}
	}
}

// spawnWorker execs the runner binary for one pool slot. Its stdout
// and stderr go to <state_dir>/workers/<pipeline>/<worker_id>.log;
// structured outcomes land in the journal via the runner.
//
// A sibling <worker_id>.pid file holds the runner PID for the
// monitor's liveness check, removed on exit so a stale file can't
// claim a now-recycled PID belongs to a worker.
func spawnWorker(ctx context.Context, log *slog.Logger, logOut io.Writer, stateDir, runnerPath string, spec pipelinespec.Spec, slot int, workerEnv []string) {
	workerID := uuid.NewString()[:8]
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
	StartedAt     time.Time         `yaml:"started_at"`
	PID           int               `yaml:"pid"`
	Pipelines     []runtimePipeline `yaml:"pipelines"`
}

type runtimePipeline struct {
	Name         string        `yaml:"name"`
	PoolSize     int           `yaml:"pool_size"`
	TickInterval time.Duration `yaml:"tick_interval"`
	StepTimeout  time.Duration `yaml:"step_timeout"`
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
		StartedAt:     time.Now().UTC(),
		PID:           os.Getpid(),
	}
	for _, p := range cfg.Pipelines {
		r.Pipelines = append(r.Pipelines, runtimePipeline{
			Name:         p.Name,
			PoolSize:     p.PoolSize,
			TickInterval: p.TickInterval,
			StepTimeout:  p.StepTimeout,
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
