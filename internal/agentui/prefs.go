package agentui

import (
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

// prefsFileName is the basename of the operator preferences file chitra
// writes inside brahma's state dir. Co-locates with runtime.yaml /
// accounts.yaml so a single state dir holds everything.
const prefsFileName = "chitra.yaml"

// Prefs is the on-disk shape for monitor preferences that survive
// restarts. Today: which panels were visible at last exit and whether
// the operator wanted pruned worktrees hidden. Add new fields with
// `omitempty` so older monitors silently ignore them.
type Prefs struct {
	VisiblePanels []string `yaml:"visible_panels,omitempty"`
	ShowPruned    *bool    `yaml:"show_pruned,omitempty"`
}

// LoadPrefs reads the saved monitor prefs from stateDir. Missing /
// unreadable / malformed file returns a zero Prefs without error —
// the caller falls back to defaults.
func LoadPrefs(stateDir string) Prefs {
	if stateDir == "" {
		return Prefs{}
	}
	raw, err := os.ReadFile(filepath.Join(stateDir, prefsFileName))
	if err != nil {
		return Prefs{}
	}
	var p Prefs
	if err := yaml.Unmarshal(raw, &p); err != nil {
		return Prefs{}
	}
	return p
}

// SavePrefs atomically writes prefs to <stateDir>/chitra.yaml.
// Errors are returned so the caller can log them, but the monitor
// treats save failures as non-fatal — prefs are a UX nicety, not
// load-bearing state.
func SavePrefs(stateDir string, p Prefs) error {
	if stateDir == "" {
		return nil
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	body, err := yaml.Marshal(p)
	if err != nil {
		return err
	}
	target := filepath.Join(stateDir, prefsFileName)
	tmp, err := os.CreateTemp(stateDir, prefsFileName+".tmp.*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), target)
}

// visibleIDsSorted returns the keys of vis whose value is true, in
// stable order — keeps the on-disk yaml diff-friendly across saves.
func visibleIDsSorted(vis map[PanelID]bool) []string {
	out := make([]string, 0, len(vis))
	for id, on := range vis {
		if on {
			out = append(out, string(id))
		}
	}
	sort.Strings(out)
	return out
}
