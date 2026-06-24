package agentui

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrefs_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := Prefs{VisiblePanels: []string{"orchestrator", "tasks", "tokens"}}
	if err := SavePrefs(dir, want); err != nil {
		t.Fatalf("SavePrefs: %v", err)
	}
	got := LoadPrefs(dir)
	if len(got.VisiblePanels) != len(want.VisiblePanels) {
		t.Fatalf("round-trip length mismatch: got %v want %v", got.VisiblePanels, want.VisiblePanels)
	}
	for i, v := range want.VisiblePanels {
		if got.VisiblePanels[i] != v {
			t.Errorf("round-trip[%d] = %q, want %q", i, got.VisiblePanels[i], v)
		}
	}
}

func TestPrefs_MissingFileReturnsEmpty(t *testing.T) {
	got := LoadPrefs(t.TempDir())
	if len(got.VisiblePanels) != 0 {
		t.Errorf("want empty prefs for missing file, got %+v", got)
	}
}

func TestPrefs_MalformedYAMLReturnsEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, prefsFileName), []byte("not: [valid: yaml"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	got := LoadPrefs(dir)
	if len(got.VisiblePanels) != 0 {
		t.Errorf("want empty prefs for malformed yaml, got %+v", got)
	}
}

func TestPrefs_EmptyStateDirIsNoop(t *testing.T) {
	if err := SavePrefs("", Prefs{VisiblePanels: []string{"x"}}); err != nil {
		t.Errorf("SavePrefs with empty dir should be no-op, got err %v", err)
	}
	if got := LoadPrefs(""); len(got.VisiblePanels) != 0 {
		t.Errorf("LoadPrefs with empty dir should be no-op, got %+v", got)
	}
}

func TestVisibleIDsSorted_OmitsFalseAndStableOrder(t *testing.T) {
	vis := map[PanelID]bool{
		PanelTokens:       true,
		PanelOrchestrator: true,
		PanelBackoff:      false,
		PanelTasks:        true,
	}
	got := visibleIDsSorted(vis)
	want := []string{"orchestrator", "tasks", "tokens"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("got[%d] = %s, want %s", i, got[i], v)
		}
	}
}

func TestNewModel_RestoresPrefsFromDisk(t *testing.T) {
	dir := t.TempDir()
	if err := SavePrefs(dir, Prefs{VisiblePanels: []string{"backoff", "tokens"}}); err != nil {
		t.Fatalf("seed prefs: %v", err)
	}
	m := NewModel(CollectOptions{StateDir: dir}, 0)
	if m.visible[PanelOrchestrator] {
		t.Errorf("PanelOrchestrator should be off (not in saved set), got on")
	}
	if !m.visible[PanelBackoff] || !m.visible[PanelTokens] {
		t.Errorf("saved panels not restored: %+v", m.visible)
	}
}

func TestNewModel_FallsBackToDefaultsWhenNoPrefs(t *testing.T) {
	m := NewModel(CollectOptions{StateDir: t.TempDir()}, 0)
	for id, want := range defaultVisible {
		if m.visible[id] != want {
			t.Errorf("default visibility lost for %s: got %v want %v", id, m.visible[id], want)
		}
	}
}
