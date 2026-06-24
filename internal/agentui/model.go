package agentui

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/grasskode/bramha/internal/journal"
)

// PanelID identifies the toggleable sections in the --watch TUI.
type PanelID string

const (
	PanelOrchestrator   PanelID = "orchestrator"
	PanelBackoff        PanelID = "backoff"
	PanelPipelines      PanelID = "pipelines"
	PanelTasks          PanelID = "tasks" // merged tasks+worktrees view; absorbs stale-lock surfacing
	PanelSilentFailures PanelID = "silent-failures"
	PanelAccounts       PanelID = "accounts"
	PanelTokens         PanelID = "tokens"
)

// allPanels is the ordered render list. The key (single rune) is the
// keystroke that toggles the panel visibility; ctrl+<key> focuses the
// panel (scrolling the viewport so it sits at the top, and auto-showing
// it if it was hidden).
//
// PanelTokens has no standalone renderer entry: toggling `k` enhances
// the pipelines panel with token-burn data rather than producing a new
// box. The footer chip still shows so the toggle is discoverable.
var allPanels = []struct {
	ID     PanelID
	Label  string
	Key    string
	Render func(io.Writer, Snapshot)
}{
	{PanelOrchestrator, "orchestrator", "o", RenderOrchestrator},
	{PanelBackoff, "backoff", "b", RenderBackoff},
	{PanelPipelines, "pipelines", "p", RenderPipelines},
	{PanelTasks, "tasks", "t", RenderTasks},
	{PanelSilentFailures, "silent-failures", "f", RenderSilentFailures},
	{PanelAccounts, "accounts", "a", RenderAccounts},
}

// tokensToggleKey is the keystroke that flips the tokens overlay on
// the pipelines panel. Listed separately from allPanels because tokens
// has no standalone renderer slot.
const tokensToggleKey = "k"

// defaultVisible is the panel set visible at startup. Operator priority:
// surface health (orchestrator + backoff), what's queued (pipelines),
// and what's blocked (stale locks + worktrees).
var defaultVisible = map[PanelID]bool{
	PanelOrchestrator: true,
	PanelPipelines:    true,
}

// viewMode selects which screen the TUI shows. Panels is the default
// snapshot-of-panels view; Logs is the worker-log browser.
type viewMode int

const (
	viewModePanels viewMode = iota
	viewModeLogs
)

// logsLevel selects which slice of the worker-log hierarchy is
// currently being browsed. Drill-down is tasks → phases → runs:
// pick a task id, then a phase within that task, then one specific
// worker invocation whose log you want to tail.
type logsLevel int

const (
	logsLevelTasks logsLevel = iota
	logsLevelPhases
	logsLevelRuns
)

// logsView is the per-mode state for the logs browser. entries is the
// full discovery snapshot (refreshed on tick); level + selPhase +
// selTask drive what's currently shown; selected/scrollOffset is the
// cursor and viewport for the current level; tail is the trailing
// slice of the currently-selected run's log file (only set at
// logsLevelRuns).
type logsView struct {
	entries      []LogEntry
	level        logsLevel
	selPhase     string // breadcrumb context once user drilled past phases
	selTask      string // breadcrumb context once user drilled past tasks
	selected     int    // index into the current level's list
	scrollOffset int    // first row of the windowed list panel
	tail         string
	// showIdle controls whether the IdleTaskBucket is included in the
	// tasks list. Hidden by default — idle polls dominate the list and
	// add nothing for operators looking for in-flight work.
	showIdle bool
}

// logListPanelRows is the fixed height of the list panel at each
// drill-down level, counting the column header, entry rows, and (when
// clipping) the "↑ N more" / "↓ N more" indicators. Tweak here to
// trade screen real estate between the list and the tail.
const logListPanelRows = 20

// Model is the bubbletea state. Snapshot is replaced on each tick;
// visible is the user's toggle state.
type Model struct {
	opts       CollectOptions
	tick       time.Duration
	visible    map[PanelID]bool
	snap       Snapshot
	width      int
	height     int
	err        error
	mode       viewMode
	logs       logsView
	scrollLine int // top line of the panels viewport (0 = no scroll)
	// showPruned controls whether prune-ok worktrees are listed in
	// the tasks panel. Hidden by default — pruned rows are done work
	// and outnumber active rows once the orchestrator has been
	// running for a while.
	showPruned bool
}

// NewModel constructs a Model. tick controls how often the snapshot is
// re-collected. Snapshot collection runs synchronously inside Update,
// so set tick generously (5–10s) if the worktree scan is slow.
//
// Panel visibility is restored from <state_dir>/chitra.yaml when
// present so toggles survive monitor restarts; missing / unreadable
// prefs fall back to defaultVisible.
func NewModel(opts CollectOptions, tick time.Duration) Model {
	vis := make(map[PanelID]bool, len(defaultVisible))
	prefs := LoadPrefs(opts.StateDir)
	if len(prefs.VisiblePanels) > 0 {
		valid := map[PanelID]bool{PanelTokens: true}
		for _, p := range allPanels {
			valid[p.ID] = true
		}
		for _, id := range prefs.VisiblePanels {
			pid := PanelID(id)
			if valid[pid] {
				vis[pid] = true
			}
		}
	} else {
		for k, v := range defaultVisible {
			vis[k] = v
		}
	}
	showPruned := false
	if prefs.ShowPruned != nil {
		showPruned = *prefs.ShowPruned
	}
	return Model{
		opts:       opts,
		tick:       tick,
		visible:    vis,
		showPruned: showPruned,
		snap:       Collect(context.Background(), opts),
	}
}

type tickMsg time.Time

func (m Model) Init() tea.Cmd {
	return tickEvery(m.tick)
}

func tickEvery(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case tea.KeyMsg:
		if m.mode == viewModeLogs {
			return m.updateLogs(msg)
		}
		key := msg.String()
		switch key {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "L":
			m.mode = viewModeLogs
			m.refreshLogs()
		case "r":
			m.snap = Collect(context.Background(), m.opts)
		case "A":
			for _, p := range allPanels {
				m.visible[p.ID] = true
			}
			m.visible[PanelTokens] = true
			m.persistPrefs()
		case "z":
			m.visible = map[PanelID]bool{}
			for k, v := range defaultVisible {
				m.visible[k] = v
			}
			m.persistPrefs()
		case "down":
			m.scrollLine++
			m.clampScroll()
		case "up":
			if m.scrollLine > 0 {
				m.scrollLine--
			}
		case "pgdown", "ctrl+d":
			m.scrollLine += m.panelsViewportHeight() / 2
			m.clampScroll()
		case "pgup", "ctrl+u":
			m.scrollLine -= m.panelsViewportHeight() / 2
			if m.scrollLine < 0 {
				m.scrollLine = 0
			}
		case "home":
			m.scrollLine = 0
		case "end":
			m.scrollLine = len(m.renderPanelLines())
			m.clampScroll()
		case tokensToggleKey:
			// Tokens enhances the pipelines panel; auto-show pipelines
			// when turning tokens on so the operator never toggles into
			// an invisible enhancement.
			m.visible[PanelTokens] = !m.visible[PanelTokens]
			if m.visible[PanelTokens] {
				m.visible[PanelPipelines] = true
				m.focusPanel(PanelPipelines)
			}
			m.persistPrefs()
		case "P":
			// Toggle pruned-worktree visibility on the tasks panel.
			// Auto-show tasks when turning pruned on so the operator
			// never toggles into an invisible enhancement.
			m.showPruned = !m.showPruned
			if m.showPruned {
				m.visible[PanelTasks] = true
				m.focusPanel(PanelTasks)
			}
			m.clampScroll()
			m.persistPrefs()
		default:
			// ctrl+<key> focuses (and auto-shows) the matching panel.
			if pid, ok := panelForCtrlKey(key); ok {
				m.visible[pid] = true
				m.focusPanel(pid)
				m.persistPrefs()
				break
			}
			for _, p := range allPanels {
				if key == p.Key {
					m.visible[p.ID] = !m.visible[p.ID]
					m.persistPrefs()
				}
			}
		}
	case tickMsg:
		if m.mode == viewModeLogs {
			m.refreshLogs()
		} else {
			m.snap = Collect(context.Background(), m.opts)
			m.clampScroll()
		}
		return m, tickEvery(m.tick)
	}
	return m, nil
}

// panelForCtrlKey maps a "ctrl+<rune>" keystroke to the panel id whose
// toggle key matches the rune. Returns false when key isn't a control
// combination or matches no known panel.
func panelForCtrlKey(key string) (PanelID, bool) {
	const prefix = "ctrl+"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	rune := strings.TrimPrefix(key, prefix)
	for _, p := range allPanels {
		if p.Key == rune {
			return p.ID, true
		}
	}
	return "", false
}

// updateLogs handles key events while the logs view is active. The
// view is a three-level drill-down: tasks → phases → runs.
//   - j/k or arrows move the cursor within the current level
//   - enter / l / right drills into the highlighted task or phase
//   - esc / h / left / backspace drills out one level, or exits the
//     logs view altogether when already at the tasks level
//   - L always exits the logs view (matches the entry shortcut)
//   - g/G jump to the top/bottom of the current level's list
//   - r rediscovers logs from disk
func (m Model) updateLogs(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "L":
		m.exitLogsView()
	case "esc", "h", "left", "backspace":
		if m.logs.level == logsLevelTasks {
			m.exitLogsView()
		} else {
			m.drillOut()
		}
	case "enter", "l", "right":
		m.drillIn()
	case "r":
		m.refreshLogs()
	case "i":
		// Toggle the idle bucket on/off at the tasks level. Clamp the
		// cursor since the list may have just grown or shrunk.
		m.logs.showIdle = !m.logs.showIdle
		if m.logs.level == logsLevelTasks {
			n := m.currentLevelLen()
			if m.logs.selected >= n {
				if n > 0 {
					m.logs.selected = n - 1
				} else {
					m.logs.selected = 0
				}
			}
			m.ensureSelectedVisible()
		}
	case "j", "down":
		m.moveSelection(+1)
	case "k", "up":
		m.moveSelection(-1)
	case "g", "home":
		m.logs.selected = 0
		m.logs.scrollOffset = 0
		m.refreshTailIfRun()
	case "G", "end":
		if n := m.currentLevelLen(); n > 0 {
			m.logs.selected = n - 1
			m.ensureSelectedVisible()
			m.refreshTailIfRun()
		}
	}
	return m, nil
}

// persistPrefs serialises the current panel-visibility set to
// <state_dir>/chitra.yaml so toggles survive a monitor restart.
// Best-effort: write failures are surfaced on stderr but don't
// disrupt the TUI.
func (m *Model) persistPrefs() {
	showPruned := m.showPruned
	if err := SavePrefs(m.opts.StateDir, Prefs{
		VisiblePanels: visibleIDsSorted(m.visible),
		ShowPruned:    &showPruned,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "chitra: save prefs: %v\n", err)
	}
}

// exitLogsView returns to the panels view and re-collects the snapshot
// so panel data is fresh on the way back. Drill-down state (level,
// breadcrumbs, selection, scroll offset) is preserved so the next L
// puts the user back where they were; only the tail buffer is
// dropped (it'll be re-read on the next refresh).
func (m *Model) exitLogsView() {
	m.mode = viewModePanels
	m.logs.tail = ""
	m.snap = Collect(context.Background(), m.opts)
}

// drillIn moves one level deeper using the currently highlighted item
// as the breadcrumb. At runs level there is nowhere to drill — the
// run is already selected and its log is tailed in the lower panel.
func (m *Model) drillIn() {
	switch m.logs.level {
	case logsLevelTasks:
		tasks := TaskStatsFor(m.logs.entries, m.logs.showIdle)
		if m.logs.selected >= len(tasks) {
			return
		}
		m.logs.selTask = tasks[m.logs.selected].TaskID
		m.logs.level = logsLevelPhases
		m.logs.selected = 0
		m.logs.scrollOffset = 0
	case logsLevelPhases:
		phases := PhaseStatsFor(m.logs.entries, m.logs.selTask)
		if m.logs.selected >= len(phases) {
			return
		}
		m.logs.selPhase = phases[m.logs.selected].Phase
		m.logs.level = logsLevelRuns
		m.logs.selected = 0
		m.logs.scrollOffset = 0
		m.refreshTailIfRun()
	}
}

// drillOut pops one breadcrumb level. Clears the breadcrumb field for
// the level we're leaving so a later refresh doesn't re-enter it stale.
func (m *Model) drillOut() {
	switch m.logs.level {
	case logsLevelRuns:
		m.logs.level = logsLevelPhases
		m.logs.selPhase = ""
		m.logs.tail = ""
	case logsLevelPhases:
		m.logs.level = logsLevelTasks
		m.logs.selTask = ""
	}
	m.logs.selected = 0
	m.logs.scrollOffset = 0
}

// moveSelection shifts the cursor by step within the current level's
// list bounds and re-tails the log when we're at the runs level.
func (m *Model) moveSelection(step int) {
	n := m.currentLevelLen()
	next := m.logs.selected + step
	if next < 0 || next >= n {
		return
	}
	m.logs.selected = next
	m.ensureSelectedVisible()
	m.refreshTailIfRun()
}

// currentLevelLen returns how many items the current level lists,
// used by bounds checks for navigation.
func (m *Model) currentLevelLen() int {
	switch m.logs.level {
	case logsLevelTasks:
		return len(TaskStatsFor(m.logs.entries, m.logs.showIdle))
	case logsLevelPhases:
		return len(PhaseStatsFor(m.logs.entries, m.logs.selTask))
	case logsLevelRuns:
		return len(RunsFor(m.logs.entries, m.logs.selPhase, m.logs.selTask))
	}
	return 0
}

// ensureSelectedVisible nudges scrollOffset so the selected row sits
// inside the fixed-height visible window. Rows are 1-to-1 with the
// current level's list at all three levels (no group headers), so the
// row index equals m.logs.selected.
func (m *Model) ensureSelectedVisible() {
	maxRows := logListPanelRows
	end := m.logs.scrollOffset + maxRows
	if m.logs.selected < m.logs.scrollOffset {
		m.logs.scrollOffset = m.logs.selected
	} else if m.logs.selected >= end {
		m.logs.scrollOffset = m.logs.selected - maxRows + 1
	}
	if m.logs.scrollOffset < 0 {
		m.logs.scrollOffset = 0
	}
}

// refreshLogs rediscovers worker logs from disk and rebinds the
// current level's selection. Run on entry, on `r`, and on each tick
// while in logs view. Breadcrumb context (selPhase / selTask) is
// preserved across refreshes so a new spawn at runs level doesn't
// kick the user back to phases.
func (m *Model) refreshLogs() {
	m.logs.entries = DiscoverLogs(m.opts.StateDir, DefaultLiveAfter)
	// Clamp selected into the new bounds — list shape can change on
	// each tick (workers come and go) so the cursor index may now
	// point past the end.
	if n := m.currentLevelLen(); m.logs.selected >= n {
		if n > 0 {
			m.logs.selected = n - 1
		} else {
			m.logs.selected = 0
		}
	}
	m.ensureSelectedVisible()
	m.refreshTailIfRun()
}

// refreshTailIfRun re-reads the trailing slice of the currently
// selected run's log file. No-op at phases / tasks levels (no log
// file is selected there) and when the runs list is empty.
func (m *Model) refreshTailIfRun() {
	if m.logs.level != logsLevelRuns {
		m.logs.tail = ""
		return
	}
	runs := RunsFor(m.logs.entries, m.logs.selPhase, m.logs.selTask)
	if len(runs) == 0 || m.logs.selected >= len(runs) {
		m.logs.tail = ""
		return
	}
	tail, err := TailFile(runs[m.logs.selected].Path, DefaultTailBytes)
	if err != nil {
		m.logs.tail = "(read error: " + err.Error() + ")"
		return
	}
	m.logs.tail = tail
}

func (m Model) View() string {
	if m.mode == viewModeLogs {
		return m.viewLogs()
	}
	var b strings.Builder
	bannerHue := colorInfo
	switch {
	case !m.snap.Orchestrator.Alive:
		bannerHue = colorErr
	case m.snap.Backoff.Active:
		bannerHue = colorBusy
	}
	bannerStyle := lipgloss.NewStyle().Bold(true).Foreground(bannerHue)
	b.WriteString(bannerStyle.Render(fmt.Sprintf("◆ chitra @ %s", m.snap.CapturedAt.Format("15:04:05"))))
	b.WriteByte('\n')
	subStyle := lipgloss.NewStyle().Faint(true)
	parts := []string{"state=" + ShortenHome(m.snap.StateDir)}
	if m.snap.WorktreesRoot != "" {
		parts = append(parts, "worktrees="+ShortenHome(m.snap.WorktreesRoot))
	}
	if label := sinceHeaderLabel(m.snap); label != "" {
		parts = append(parts, "since="+label)
	}
	b.WriteString(subStyle.Render(strings.Join(parts, "  ")))
	b.WriteByte('\n')
	b.WriteByte('\n')

	lines := m.renderPanelLines()
	height := m.panelsViewportHeight()
	start := m.scrollLine
	if start > len(lines) {
		start = len(lines)
	}
	end := start + height
	if end > len(lines) {
		end = len(lines)
	}
	b.WriteString(strings.Join(lines[start:end], "\n"))
	if start > 0 || end < len(lines) {
		b.WriteByte('\n')
		b.WriteString(subStyle.Render(fmt.Sprintf("(viewport line %d-%d / %d — j/k scroll, g/G jump, ctrl+<key> focus)",
			start+1, end, len(lines))))
	}
	b.WriteByte('\n')

	b.WriteString(m.footer())
	return b.String()
}

// renderPanelLines builds the full panels area as a flat list of lines,
// then the View() slices it according to the scroll viewport. Token
// data lives inside the pipelines box when both are toggled on.
func (m Model) renderPanelLines() []string {
	var b strings.Builder
	for _, p := range allPanels {
		if !m.visible[p.ID] {
			continue
		}
		var section strings.Builder
		m.renderPanelInto(&section, p.ID, p.Render)
		b.WriteString(panelBoxColored(m.panelTitle(p.ID, p.Label),
			strings.TrimRight(section.String(), "\n"), m.width, m.panelBorderHue(p.ID)))
		b.WriteByte('\n')
	}
	out := strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
	if len(out) == 1 && out[0] == "" {
		return nil
	}
	return out
}

// renderPanelInto delegates to the right rendering function for id,
// applying overlay/filter state that lives on the model rather than
// on the Snapshot (tokens overlay, pruned-worktree hide). Used by
// both renderPanelLines and focusPanel so they stay aligned.
func (m Model) renderPanelInto(section *strings.Builder, id PanelID, fallback func(io.Writer, Snapshot)) {
	switch {
	case id == PanelPipelines && m.visible[PanelTokens]:
		RenderPipelinesWithTokens(section, m.snap)
	case id == PanelTasks:
		renderTasks(section, m.snap, !m.showPruned)
	default:
		fallback(section, m.snap)
	}
}

// panelTitle returns the box title for a panel — usually the label,
// except the pipelines panel grows a "+ tokens" suffix when the tokens
// overlay is on so the operator can tell at a glance.
func (m Model) panelTitle(id PanelID, label string) string {
	if id == PanelPipelines && m.visible[PanelTokens] {
		return label + " + tokens"
	}
	return label
}

// panelsViewportHeight is the height available for the panels area
// after subtracting the banner, the sub-header, blank, and the footer.
// Falls back to a sensible large default when the terminal height
// isn't known (one-shot render or tests).
func (m Model) panelsViewportHeight() int {
	if m.height <= 0 {
		return 9999
	}
	reserved := 4 // banner + sub-header + blank + footer
	avail := m.height - reserved
	if avail < 5 {
		return 5
	}
	return avail
}

// clampScroll keeps the viewport top within [0, totalLines]. Called
// after re-renders that may have changed the line count.
func (m *Model) clampScroll() {
	lines := m.renderPanelLines()
	maxStart := len(lines) - m.panelsViewportHeight()
	if maxStart < 0 {
		maxStart = 0
	}
	if m.scrollLine > maxStart {
		m.scrollLine = maxStart
	}
	if m.scrollLine < 0 {
		m.scrollLine = 0
	}
}

// focusPanel scrolls the viewport so the box for id sits at the top.
// Caller is responsible for ensuring visible[id] is true first; a panel
// with visible=false produces no line range so the scroll snaps to 0.
func (m *Model) focusPanel(id PanelID) {
	var b strings.Builder
	for _, p := range allPanels {
		if !m.visible[p.ID] {
			continue
		}
		if p.ID == id {
			m.scrollLine = lineCount(b.String())
			m.clampScroll()
			return
		}
		var section strings.Builder
		m.renderPanelInto(&section, p.ID, p.Render)
		b.WriteString(panelBoxColored(m.panelTitle(p.ID, p.Label),
			strings.TrimRight(section.String(), "\n"), m.width, m.panelBorderHue(p.ID)))
		b.WriteByte('\n')
	}
	// Target wasn't found in the visible loop (e.g. caller asked to focus
	// a panel that's still hidden). Leave scroll where it was.
}

// panelBorderHue returns the border colour for a panel based on the
// current snapshot state — red when something is wrong, yellow when
// something needs attention, otherwise the default dim grey.
func (m Model) panelBorderHue(id PanelID) lipgloss.Color {
	switch id {
	case PanelOrchestrator:
		if !m.snap.Orchestrator.Alive {
			return colorErr
		}
		return colorOK
	case PanelBackoff:
		if m.snap.Backoff.Active {
			return colorErr
		}
	case PanelPipelines:
		for _, p := range m.snap.Pipelines {
			if p.Errors > 0 {
				return colorBusy
			}
		}
	case PanelSilentFailures:
		if len(m.snap.SilentFailures) > 0 {
			return colorErr
		}
	case PanelTasks:
		for _, t := range m.snap.Tasks {
			if len(t.Chain) == 0 {
				continue
			}
			switch t.Chain[len(t.Chain)-1].Outcome {
			case journal.OutcomeFailed, journal.OutcomeTimedOut, journal.OutcomeDead:
				return colorBusy
			}
		}
	}
	return lipgloss.Color("241")
}

// lineCount returns the number of '\n'-separated lines in s, with no
// trailing-newline subtraction — used to map a prefix string back to a
// line offset.
func lineCount(s string) int {
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n")
}

// viewLogs renders the worker-log browser. Three drill-down levels —
// phases, tasks, runs — share the same fixed-height list panel and
// breadcrumb; the tail panel is populated only at the runs level when
// one specific worker invocation is selected.
func (m Model) viewLogs() string {
	var b strings.Builder
	headerStyle := lipgloss.NewStyle().Bold(true)
	b.WriteString(headerStyle.Render(fmt.Sprintf("chitra — logs @ %s", time.Now().Format("15:04:05"))))
	b.WriteByte('\n')
	subStyle := lipgloss.NewStyle().Faint(true)
	b.WriteString(subStyle.Render(fmt.Sprintf("state=%s  workers=%d  %s",
		ShortenHome(m.opts.StateDir), len(m.logs.entries), m.logsBreadcrumb())))
	b.WriteByte('\n')
	b.WriteByte('\n')

	listTitle, listBody := m.renderLogsLevel()
	b.WriteString(panelBox(listTitle, strings.TrimRight(listBody, "\n"), m.width))
	b.WriteByte('\n')

	if m.logs.level == logsLevelRuns {
		tailTitle := "tail"
		runs := RunsFor(m.logs.entries, m.logs.selPhase, m.logs.selTask)
		if len(runs) > 0 && m.logs.selected < len(runs) {
			e := runs[m.logs.selected]
			tailTitle = fmt.Sprintf("tail — %s/%s/%s", e.Pool, e.TaskID, e.WorkerID)
		}
		tailBody := m.logs.tail
		if tailBody == "" {
			tailBody = "(empty)"
		} else {
			tailBody = trimToLines(tailBody, m.tailLineBudget())
		}
		b.WriteString(panelBox(tailTitle, strings.TrimRight(tailBody, "\n"), m.width))
		b.WriteByte('\n')
	}

	b.WriteString(m.logsFooter())
	return b.String()
}

// logsBreadcrumb returns the "tasks > QUA-345 > implement" trail for
// the header line. Helps the operator see where in the drill-down
// they are without inferring it from the list title.
func (m Model) logsBreadcrumb() string {
	parts := []string{"tasks"}
	if m.logs.selTask != "" {
		parts = append(parts, m.logs.selTask)
	}
	if m.logs.selPhase != "" {
		parts = append(parts, m.logs.selPhase)
	}
	return strings.Join(parts, " > ")
}

// renderLogsLevel renders the appropriate list for the current
// drill-down level, returning the panel title (which includes the
// breadcrumb context) and the windowed body.
func (m Model) renderLogsLevel() (title, body string) {
	win := LogListWindow{
		Selected:     m.logs.selected,
		ScrollOffset: m.logs.scrollOffset,
		MaxRows:      logListPanelRows,
	}
	var buf strings.Builder
	switch m.logs.level {
	case logsLevelTasks:
		tasks := TaskStatsFor(m.logs.entries, m.logs.showIdle)
		RenderTaskList(&buf, tasks, win)
		idleLabel := "hidden — press i"
		if m.logs.showIdle {
			idleLabel = "shown — press i"
		}
		title = fmt.Sprintf("tasks (%d) [idle %s]", len(tasks), idleLabel)
	case logsLevelPhases:
		phases := PhaseStatsFor(m.logs.entries, m.logs.selTask)
		RenderPhaseList(&buf, phases, win)
		title = fmt.Sprintf("%s — phases (%d)", m.logs.selTask, len(phases))
	case logsLevelRuns:
		runs := RunsFor(m.logs.entries, m.logs.selPhase, m.logs.selTask)
		RenderRunList(&buf, runs, win)
		title = fmt.Sprintf("%s / %s — runs (%d)", m.logs.selTask, m.logs.selPhase, len(runs))
	}
	return title, buf.String()
}

// tailLineBudget is the number of trailing lines of the selected log
// that fit under the fixed-height list at the current terminal height.
// Falls back to 20 when the height isn't known (one-shot render or
// tests).
func (m Model) tailLineBudget() int {
	if m.height <= 0 {
		return 20
	}
	// banner + state line + blank + list box + blank + footer = roughly
	// 6 lines around the list panel, plus the list panel itself.
	reserved := 6 + logListPanelRows + 2 // +2 for the tail panel's own border
	avail := m.height - reserved
	if avail < 5 {
		return 5
	}
	return avail
}

// trimToLines keeps the trailing n lines of s.
func trimToLines(s string, n int) string {
	if n <= 0 {
		return ""
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

func (m Model) logsFooter() string {
	dim := lipgloss.NewStyle().Faint(true)
	return dim.Render("j/k:select  enter:drill in  esc/h:drill out  g/G:jump  i:idle  r:refresh  L:exit  q:quit")
}

func panelBox(title, body string, width int) string {
	return panelBoxColored(title, body, width, lipgloss.Color("241"))
}

// panelBoxColored is panelBox with an operator-supplied border colour
// so we can highlight panels in unhealthy states (red border = look
// here first). Title row is rendered in the same hue so a quick scan
// down the layout reads as a chromatic health summary.
func panelBoxColored(title, body string, width int, hue lipgloss.Color) string {
	if width < 10 {
		width = 80
	}
	border := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(hue).
		Padding(0, 1).
		Width(width - 2)
	t := lipgloss.NewStyle().Bold(true).Foreground(hue).Render(title)
	return border.Render(t + "\n" + body)
}

func (m Model) footer() string {
	var chips []string
	dim := lipgloss.NewStyle().Foreground(colorDim)
	// "ON" chip in green, with the key letter underlined for shortcut
	// discoverability. "OFF" chip in dim grey.
	for _, p := range allPanels {
		chips = append(chips, m.styledChip(p.Key, p.Label, m.visible[p.ID]))
	}
	chips = append(chips, m.styledChip(tokensToggleKey, "tokens", m.visible[PanelTokens]))
	chips = append(chips, m.styledChip("P", "pruned", m.showPruned))
	chips = append(chips, dim.Render("ctrl+<key>:focus  ↑/↓ pg:scroll  r:refresh  A:all  z:default  L:logs  q:quit"))
	return strings.Join(chips, "  ")
}

// styledChip renders one footer toggle chip — green-bold when the
// panel is visible, dim grey otherwise. Key letter is rendered
// underlined to advertise the keystroke.
func (m Model) styledChip(key, label string, on bool) string {
	if on {
		k := lipgloss.NewStyle().Foreground(colorOK).Bold(true).Underline(true).Render(key)
		l := lipgloss.NewStyle().Foreground(colorOK).Render(":" + label)
		return k + l
	}
	k := lipgloss.NewStyle().Foreground(colorDim).Underline(true).Render(key)
	l := lipgloss.NewStyle().Foreground(colorDim).Render(":" + label)
	return k + l
}
