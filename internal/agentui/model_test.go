package agentui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func keyMsg(s string) tea.KeyMsg {
	switch s {
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

// ctrlKeyMsg synthesises a "ctrl+<rune>" key message the way bubbletea's
// String() would produce it, so the switch in Update() matches.
func ctrlKeyMsg(rune string) tea.KeyMsg {
	// bubbletea encodes ctrl+x as KeyType with Runes=nil; constructing a
	// proper tea.KeyMsg for every letter is impractical so we synthesise
	// via the raw runes path and rely on String() output.
	switch rune {
	case "p":
		return tea.KeyMsg{Type: tea.KeyCtrlP}
	case "o":
		return tea.KeyMsg{Type: tea.KeyCtrlO}
	case "t":
		return tea.KeyMsg{Type: tea.KeyCtrlT}
	case "b":
		return tea.KeyMsg{Type: tea.KeyCtrlB}
	case "s":
		return tea.KeyMsg{Type: tea.KeyCtrlS}
	case "f":
		return tea.KeyMsg{Type: tea.KeyCtrlF}
	case "a":
		return tea.KeyMsg{Type: tea.KeyCtrlA}
	}
	panic("unsupported ctrl rune: " + rune)
}

func TestPanelForCtrlKey_ResolvesKnownPanels(t *testing.T) {
	cases := map[string]PanelID{
		"ctrl+o": PanelOrchestrator,
		"ctrl+p": PanelPipelines,
		"ctrl+t": PanelTasks,
		"ctrl+a": PanelAccounts,
	}
	for key, want := range cases {
		got, ok := panelForCtrlKey(key)
		if !ok || got != want {
			t.Errorf("panelForCtrlKey(%q) = (%v, %v); want (%v, true)", key, got, ok, want)
		}
	}
	if _, ok := panelForCtrlKey("ctrl+x"); ok {
		t.Errorf("panelForCtrlKey on unknown rune should return false")
	}
	if _, ok := panelForCtrlKey("p"); ok {
		t.Errorf("panelForCtrlKey without ctrl+ prefix should return false")
	}
}

func TestCtrlKey_AutoShowsHiddenPanel(t *testing.T) {
	m := NewModel(CollectOptions{StateDir: t.TempDir()}, 0)
	// Start with PanelTasks hidden so we can prove the ctrl+key turned
	// it on.
	delete(m.visible, PanelTasks)
	if m.visible[PanelTasks] {
		t.Fatalf("setup: PanelTasks should start hidden")
	}
	out, _ := m.Update(ctrlKeyMsg("t"))
	mm := out.(Model)
	if !mm.visible[PanelTasks] {
		t.Errorf("ctrl+t should auto-show PanelTasks; visible=%v", mm.visible)
	}
}

func TestTokensToggle_AutoShowsPipelines(t *testing.T) {
	m := NewModel(CollectOptions{StateDir: t.TempDir()}, 0)
	delete(m.visible, PanelPipelines)
	delete(m.visible, PanelTokens)

	out, _ := m.Update(keyMsg("k"))
	mm := out.(Model)
	if !mm.visible[PanelTokens] {
		t.Errorf("k should toggle tokens on; got %v", mm.visible[PanelTokens])
	}
	if !mm.visible[PanelPipelines] {
		t.Errorf("toggling tokens on must auto-show pipelines; got %v", mm.visible)
	}
}

func TestTokensToggle_Off_DoesNotChangePipelines(t *testing.T) {
	m := NewModel(CollectOptions{StateDir: t.TempDir()}, 0)
	m.visible[PanelTokens] = true
	m.visible[PanelPipelines] = true

	out, _ := m.Update(keyMsg("k"))
	mm := out.(Model)
	if mm.visible[PanelTokens] {
		t.Errorf("k should toggle tokens off")
	}
	if !mm.visible[PanelPipelines] {
		t.Errorf("toggling tokens off should leave pipelines alone; got %v", mm.visible)
	}
}

func TestPanelsView_MergesTokensIntoPipelinesBox(t *testing.T) {
	m := NewModel(CollectOptions{StateDir: t.TempDir()}, 0)
	m.visible = map[PanelID]bool{PanelPipelines: true, PanelTokens: true}
	m.width = 200
	m.height = 60
	// Prime a non-empty snapshot — NewModel reads from an empty state
	// dir so the default snapshot has zero pipelines, which short-
	// circuits the header path we're trying to assert against.
	m.snap = Snapshot{
		SinceLabel: "last 6h",
		Pipelines: []PipelineStat{
			{Name: "investigate", Ran: 1},
		},
		Tokens: TokensPanel{Available: true, Note: "last 6h"},
	}

	view := m.View()
	// The merged box must announce its enhancement in the title and
	// must NOT produce a separate "tokens" box / TOKEN BURN heading.
	if !strings.Contains(view, "pipelines + tokens") {
		t.Errorf("expected merged panel title; view:\n%s", view)
	}
	if strings.Contains(view, "TOKEN BURN") {
		t.Errorf("inline token columns must not render a separate TOKEN BURN heading; view:\n%s", view)
	}
	// The token columns appear in the pipelines header instead.
	if !strings.Contains(view, "SESS") || !strings.Contains(view, "INPUT") || !strings.Contains(view, "OUTPUT") {
		t.Errorf("expected inline SESS/INPUT/OUTPUT columns in pipelines header; view:\n%s", view)
	}
}

func TestRenderPanelLines_HidesTokensWhenToggleOff(t *testing.T) {
	m := NewModel(CollectOptions{StateDir: t.TempDir()}, 0)
	m.visible = map[PanelID]bool{PanelPipelines: true}
	m.width = 100

	out := strings.Join(m.renderPanelLines(), "\n")
	if strings.Contains(out, "TOKEN BURN") || strings.Contains(out, "INPUT") {
		t.Errorf("tokens overlay should be hidden when PanelTokens=false; got:\n%s", out)
	}
}

func TestPanelsScroll_ClampedToContent(t *testing.T) {
	m := NewModel(CollectOptions{StateDir: t.TempDir()}, 0)
	// Make every panel visible to maximise rendered lines.
	for _, p := range allPanels {
		m.visible[p.ID] = true
	}
	m.width = 100
	m.height = 20 // small viewport so scrolling is meaningful

	// Send "end" — scroll to the bottom; clamp must keep it within bounds.
	out, _ := m.Update(keyMsg("end"))
	mm := out.(Model)
	total := len(mm.renderPanelLines())
	if mm.scrollLine > total {
		t.Errorf("scrollLine=%d should be clamped to <= %d", mm.scrollLine, total)
	}
	if mm.scrollLine < 0 {
		t.Errorf("scrollLine should not go negative; got %d", mm.scrollLine)
	}
}
