// Package pipelinespec loads the orchestrator's YAML config — a single
// file describing where to keep state plus the list of pipelines to run.
// The file is the orchestrator's only declarative input; account
// secrets (Linear, Slack, GitHub) belong in the operator's shell env
// and propagate to workers automatically via the runner's os.Environ()
// inheritance.
//
// One top-level Config wraps a slice of Spec values. The runner
// (separate package) consumes one Spec per pool-fill tick; the
// orchestrator (eventually) consumes the whole Config.
package pipelinespec

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the orchestrator's complete declarative input. Loaded from
// one YAML file via LoadFile. Optional fields receive defaults.
type Config struct {
	// StateDir holds the journal (<dir>/journal.jsonl) and per-worker
	// state (<dir>/workers/<pipeline>/<id>/). Defaults to
	// $XDG_STATE_HOME/bramha (or ~/.local/state/bramha).
	StateDir string `yaml:"state_dir"`

	// LogFile is the orchestrator's own log destination. Empty = stderr.
	LogFile string `yaml:"log_file"`

	// Pipelines is the list of pipelines the orchestrator runs. Required;
	// at least one entry. Names must be unique.
	Pipelines []Spec `yaml:"pipelines"`

	// SourcePath records the file Config was loaded from. Set by
	// LoadFile; not part of the YAML schema.
	SourcePath string `yaml:"-"`
}

// Spec is one pipeline's declaration. Optional fields receive defaults
// from Config.applyDefaults.
type Spec struct {
	// Name identifies the pipeline in the journal, the TUI pool list,
	// and <state>/workers/<name>/. Must be kebab-case (lowercase letters,
	// digits, hyphens; starts with a letter) and unique across the file.
	Name string `yaml:"name"`

	// PoolSize caps concurrent worker invocations for this pipeline.
	PoolSize int `yaml:"pool_size"`

	// TickInterval is the minimum delay between pool-fill checks. On
	// each tick the orchestrator spawns one more worker if running <
	// PoolSize ("lazy" fill).
	TickInterval time.Duration `yaml:"tick_interval"`

	// StepTimeout bounds one worker invocation; exceeding it triggers
	// SIGTERM-then-SIGKILL.
	StepTimeout time.Duration `yaml:"step_timeout"`

	// Command is the shell snippet that runs the worker. The runner
	// passes it to `sh -c`, so any shell construct (env interpolation,
	// pipes, &&) is available. The orchestrator injects AGENT_PIPELINE,
	// AGENT_WORKER_ID, AGENT_STATE_ROOT, AGENT_WORKER_INDEX into the
	// environment before exec; the operator's shell env propagates too.
	Command string `yaml:"command"`
}

const (
	defaultPoolSize     = 1
	defaultTickInterval = 5 * time.Minute
	defaultStepTimeout  = 1 * time.Hour
	minTickInterval     = 1 * time.Second
	minStepTimeout      = 1 * time.Second
)

var nameRE = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// LoadFile reads, parses, defaults, and validates the YAML config at
// path. Returns the populated Config or the first error encountered.
// Pipelines are returned in the order they appear in the file so the
// monitor can render the pipelines table in the operator-meaningful
// order the config author chose (instead of alphabetical).
func LoadFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.SourcePath = path
	if err := cfg.applyDefaults(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() error {
	if c.StateDir == "" {
		dir := os.Getenv("XDG_STATE_HOME")
		if dir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return fmt.Errorf("resolve home dir: %w", err)
			}
			dir = filepath.Join(home, ".local", "state")
		}
		c.StateDir = filepath.Join(dir, "bramha")
	} else {
		c.StateDir = expandHome(c.StateDir)
	}
	if c.LogFile != "" {
		c.LogFile = expandHome(c.LogFile)
	}
	for i := range c.Pipelines {
		c.Pipelines[i].applyDefaults()
	}
	return nil
}

func (c *Config) validate() error {
	if len(c.Pipelines) == 0 {
		return fmt.Errorf("pipelines: at least one entry is required")
	}
	seen := map[string]bool{}
	for i := range c.Pipelines {
		if err := c.Pipelines[i].validate(); err != nil {
			return fmt.Errorf("pipelines[%d] (%q): %w", i, c.Pipelines[i].Name, err)
		}
		if seen[c.Pipelines[i].Name] {
			return fmt.Errorf("pipelines[%d]: name %q declared twice", i, c.Pipelines[i].Name)
		}
		seen[c.Pipelines[i].Name] = true
	}
	return nil
}

func (s *Spec) applyDefaults() {
	if s.PoolSize == 0 {
		s.PoolSize = defaultPoolSize
	}
	if s.TickInterval == 0 {
		s.TickInterval = defaultTickInterval
	}
	if s.StepTimeout == 0 {
		s.StepTimeout = defaultStepTimeout
	}
	s.Command = strings.TrimSpace(s.Command)
}

func (s *Spec) validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if !nameRE.MatchString(s.Name) {
		return fmt.Errorf("name %q must be kebab-case (lowercase letters, digits, hyphens; start with a letter)", s.Name)
	}
	if s.PoolSize < 1 {
		return fmt.Errorf("pool_size must be >= 1, got %d", s.PoolSize)
	}
	if s.TickInterval < minTickInterval {
		return fmt.Errorf("tick_interval must be >= %s, got %s", minTickInterval, s.TickInterval)
	}
	if s.StepTimeout < minStepTimeout {
		return fmt.Errorf("step_timeout must be >= %s, got %s", minStepTimeout, s.StepTimeout)
	}
	if s.Command == "" {
		return fmt.Errorf("command is required")
	}
	return nil
}

// expandHome rewrites a leading ~ or ~/ in p to the user's home dir.
// Other paths pass through unchanged.
func expandHome(p string) string {
	if p == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
		return p
	}
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, p[2:])
		}
	}
	return p
}
