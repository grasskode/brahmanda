package main

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/grasskode/brahmanda/internal/pipelinespec"
)

// A worktrees root declared only in the config's env_file (never exported
// in brahma's shell) must reach runtime.yaml, and must win over an
// inherited shell value — the monitor reads runtime.yaml to locate
// worktrees, so this is the propagation the operator relies on.
func TestWriteRuntimeFile_UsesConfigEnv(t *testing.T) {
	t.Setenv("WORKTREES_ROOT", "/from/shell")
	t.Setenv("CLAUDE_HOME", "")

	cfg := &pipelinespec.Config{
		StateDir: t.TempDir(),
		Env: map[string]string{
			"WORKTREES_ROOT": "/from/config",
			"CLAUDE_HOME":    "/from/config/claude",
		},
	}

	if err := applyConfigEnv(cfg); err != nil {
		t.Fatalf("applyConfigEnv: %v", err)
	}
	if err := writeRuntimeFile(cfg, "config.yaml", "srishti"); err != nil {
		t.Fatalf("writeRuntimeFile: %v", err)
	}

	raw, err := os.ReadFile(runtimeFilePath(cfg.StateDir))
	if err != nil {
		t.Fatalf("read runtime.yaml: %v", err)
	}
	var rt runtimeFile
	if err := yaml.Unmarshal(raw, &rt); err != nil {
		t.Fatalf("unmarshal runtime.yaml: %v", err)
	}

	if rt.WorktreesRoot != "/from/config" {
		t.Errorf("worktrees_root = %q, want %q (config env_file must win over shell)", rt.WorktreesRoot, "/from/config")
	}
	if rt.ClaudeHome != "/from/config/claude" {
		t.Errorf("claude_home = %q, want %q", rt.ClaudeHome, "/from/config/claude")
	}
}
