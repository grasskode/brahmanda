package pipelinespec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFile_Success(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "config.yaml", `
state_dir: /tmp/test-state
log_file: /tmp/test-log
pipelines:
  - name: triage
    pool_size: 4
    tick_interval: 1m
    step_timeout: 10m
    command: |
      ./workers/triage
  - name: investigate
    pool_size: 2
    tick_interval: 5m
    step_timeout: 1h
    command: ./workers/investigate
`)
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.StateDir != "/tmp/test-state" {
		t.Errorf("StateDir = %q, want /tmp/test-state", cfg.StateDir)
	}
	if cfg.LogFile != "/tmp/test-log" {
		t.Errorf("LogFile = %q, want /tmp/test-log", cfg.LogFile)
	}
	if cfg.SourcePath != path {
		t.Errorf("SourcePath = %q, want %q", cfg.SourcePath, path)
	}
	if len(cfg.Pipelines) != 2 {
		t.Fatalf("got %d pipelines, want 2", len(cfg.Pipelines))
	}
	// Config order preserved: triage, investigate.
	if cfg.Pipelines[0].Name != "triage" || cfg.Pipelines[1].Name != "investigate" {
		t.Errorf("pipelines not in config order: %v", names(cfg))
	}
	tri := cfg.Pipelines[0]
	if tri.PoolSize != 4 || tri.TickInterval != time.Minute || tri.StepTimeout != 10*time.Minute {
		t.Errorf("triage fields wrong: %+v", tri)
	}
	if tri.Command != "./workers/triage" {
		t.Errorf("triage command = %q (trimmed?), want %q", tri.Command, "./workers/triage")
	}
}

func TestLoadFile_Defaults(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "config.yaml", `
pipelines:
  - name: minimal
    command: ./workers/minimal
`)
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.StateDir == "" {
		t.Error("StateDir default not applied")
	}
	if cfg.LogFile != "" {
		t.Errorf("LogFile default should stay empty, got %q", cfg.LogFile)
	}
	s := cfg.Pipelines[0]
	if s.PoolSize != 1 {
		t.Errorf("PoolSize default = %d, want 1", s.PoolSize)
	}
	if s.TickInterval != 5*time.Minute {
		t.Errorf("TickInterval default = %s, want 5m", s.TickInterval)
	}
	if s.StepTimeout != time.Hour {
		t.Errorf("StepTimeout default = %s, want 1h", s.StepTimeout)
	}
}

func TestLoadFile_StateDirTildeExpansion(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home dir")
	}
	path := writeConfig(t, t.TempDir(), "config.yaml", `
state_dir: ~/orch-state
pipelines:
  - name: p
    command: ./w
`)
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	want := filepath.Join(home, "orch-state")
	if cfg.StateDir != want {
		t.Errorf("StateDir = %q, want %q", cfg.StateDir, want)
	}
}

func TestLoadFile_DuplicatePipelineName(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "config.yaml", `
pipelines:
  - name: dup
    command: ./a
  - name: dup
    command: ./b
`)
	_, err := LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "declared twice") {
		t.Fatalf("expected duplicate-name error, got %v", err)
	}
}

func TestLoadFile_NoPipelines(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "config.yaml", `
state_dir: /tmp/x
`)
	_, err := LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "at least one entry") {
		t.Fatalf("expected empty-pipelines error, got %v", err)
	}
}

func TestLoadFile_MissingFile(t *testing.T) {
	_, err := LoadFile(filepath.Join(t.TempDir(), "nope.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadFile_ParseError(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "broken.yaml", "pipelines:\n  - name: ok\n   bad-indent: true\n")
	_, err := LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "broken.yaml") {
		t.Fatalf("expected parse error mentioning broken.yaml, got %v", err)
	}
}

func TestLoadFile_PipelineValidation(t *testing.T) {
	cases := []struct {
		label     string
		yaml      string
		wantError string
	}{
		{"missing-name", `pipelines: [{command: ./x}]`, "name is required"},
		{"bad-name-case", `pipelines: [{name: Triage, command: ./x}]`, "must be kebab-case"},
		{"bad-name-leading-digit", `pipelines: [{name: 9lives, command: ./x}]`, "must be kebab-case"},
		{"missing-command", `pipelines: [{name: ok}]`, "command is required"},
		{"whitespace-only-command", `pipelines: [{name: ok, command: "   "}]`, "command is required"},
		{"pool-size-zero-defaults-to-1", `pipelines: [{name: ok, command: ./x, pool_size: 0}]`, ""},
		{"pool-size-negative", `pipelines: [{name: ok, command: ./x, pool_size: -1}]`, "pool_size must be >= 1"},
		{"tick-too-small", `pipelines: [{name: ok, command: ./x, tick_interval: 100ms}]`, "tick_interval must be >= 1s"},
		{"timeout-too-small", `pipelines: [{name: ok, command: ./x, step_timeout: 100ms}]`, "step_timeout must be >= 1s"},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			path := writeConfig(t, t.TempDir(), "config.yaml", c.yaml+"\n")
			_, err := LoadFile(path)
			if c.wantError == "" {
				if err != nil {
					t.Fatalf("expected success, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantError) {
				t.Fatalf("expected error containing %q, got %v", c.wantError, err)
			}
		})
	}
}

// TestLoadFile_ExampleConfig is a smoke test: every committed example
// must parse against the current schema. No assertions on names or
// counts — the examples are documentation, free to evolve.
func TestLoadFile_ExampleConfig(t *testing.T) {
	for _, path := range []string{
		"../../examples/simple/config.yaml",
		"../../examples/pipeline/config.yaml",
	} {
		if _, err := LoadFile(path); err != nil {
			t.Fatalf("LoadFile(%s): %v", path, err)
		}
	}
}

func TestLoadFile_EnvFile(t *testing.T) {
	dir := t.TempDir()
	writeConfig(t, dir, "worker.env", `
# a comment
LOG_LEVEL=debug
export REGION="eu-west-1"
EMPTY=
QUOTED='spaced value'
`)
	path := writeConfig(t, dir, "config.yaml", `
state_dir: /tmp/test-state
env_file: worker.env
pipelines:
  - name: build
    command: ./build
`)
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	want := map[string]string{
		"LOG_LEVEL": "debug",
		"REGION":    "eu-west-1",
		"EMPTY":     "",
		"QUOTED":    "spaced value",
	}
	for k, v := range want {
		if cfg.Env[k] != v {
			t.Errorf("Env[%q] = %q, want %q", k, cfg.Env[k], v)
		}
	}
	// WorkerEnvPairs is sorted KEY=value.
	pairs := cfg.WorkerEnvPairs()
	if len(pairs) != len(want) {
		t.Fatalf("WorkerEnvPairs len = %d, want %d (%v)", len(pairs), len(want), pairs)
	}
	if pairs[0] != "EMPTY=" {
		t.Errorf("WorkerEnvPairs not sorted: %v", pairs)
	}
}

func TestLoadFile_EnvFileRejectsReservedAndMalformed(t *testing.T) {
	cases := map[string]string{
		"reserved AGENT_ prefix": "AGENT_PIPELINE=oops\n",
		"invalid identifier":     "1BAD=x\n",
		"missing equals":         "NOEQUALS\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeConfig(t, dir, "worker.env", body)
			path := writeConfig(t, dir, "config.yaml", `
state_dir: /tmp/test-state
env_file: worker.env
pipelines:
  - name: build
    command: ./build
`)
			if _, err := LoadFile(path); err == nil {
				t.Fatalf("LoadFile accepted bad env_file (%s)", name)
			}
		})
	}
}

func TestLoadFile_MissingEnvFile(t *testing.T) {
	path := writeConfig(t, t.TempDir(), "config.yaml", `
state_dir: /tmp/test-state
env_file: does-not-exist.env
pipelines:
  - name: build
    command: ./build
`)
	if _, err := LoadFile(path); err == nil {
		t.Fatal("LoadFile accepted a missing env_file")
	}
}

func names(cfg *Config) []string {
	out := make([]string, len(cfg.Pipelines))
	for i, p := range cfg.Pipelines {
		out[i] = p.Name
	}
	return out
}
