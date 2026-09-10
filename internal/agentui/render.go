package agentui

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
	"github.com/grasskode/brahmanda/internal/tokens"
)

// Render writes a plain-text snapshot to w. Used by the one-shot mode
// (`chitra` without --watch) and by piped-output scripts.
func Render(w io.Writer, s Snapshot) {
	fmt.Fprintf(w, "chitra @ %s\n", s.CapturedAt.Format(time.RFC3339))
	fmt.Fprintf(w, "state-dir:     %s\n", s.StateDir)
	if s.WorktreesRoot != "" {
		fmt.Fprintf(w, "worktrees:     %s\n", s.WorktreesRoot)
	}
	if label := sinceHeaderLabel(s); label != "" {
		fmt.Fprintf(w, "since:         %s\n", label)
	}
	fmt.Fprintln(w)
	RenderOrchestrator(w, s)
	fmt.Fprintln(w)
	RenderBackoff(w, s)
	fmt.Fprintln(w)
	RenderAccounts(w, s)
	fmt.Fprintln(w)
	RenderPipelines(w, s)
	fmt.Fprintln(w)
	RenderTasks(w, s)
	fmt.Fprintln(w)
	RenderSilentFailures(w, s)
	fmt.Fprintln(w)
	RenderTokens(w, s)
}

// ---- per-panel renderers (also called from the TUI) ----------------------

func RenderOrchestrator(w io.Writer, s Snapshot) {
	fmt.Fprintln(w, "ORCHESTRATOR")
	if !s.Orchestrator.Alive {
		fmt.Fprintf(w, "  %s %s\n",
			styleErr.Render("STATUS:"),
			styleErr.Render("not running (no live flock on brahma.lock)"))
		return
	}
	fmt.Fprintf(w, "  %s %s  PID %s  since %s\n",
		styleOK.Render("STATUS:"),
		styleOK.Render("running"),
		nonEmpty(s.Orchestrator.PID, "?"),
		styleDim.Render(s.Orchestrator.Since.Format(time.RFC3339)))
	if s.MaxWorkers > 0 {
		busy := 0
		for _, p := range s.Pipelines {
			busy += p.Running
		}
		if busy > s.MaxWorkers {
			busy = s.MaxWorkers // global cap is enforced in-memory; clamp display too
		}
		fmt.Fprintf(w, "  %s %d / %d workers busy (round-robin across pipelines)\n",
			styleDim.Render("POOL:"), busy, s.MaxWorkers)
	}
	for _, note := range budgetHeldNotes(s) {
		fmt.Fprintf(w, "  %s %s\n", styleErr.Render("BUDGET:"), styleErr.Render(note))
	}
}

func RenderBackoff(w io.Writer, s Snapshot) {
	fmt.Fprintln(w, "CLAUDE BACKOFF")
	if !s.Backoff.Active {
		fmt.Fprintln(w, "  "+styleOK.Render("healthy (no agent-errors/claude marker)"))
		return
	}
	age := time.Since(s.Backoff.WrittenAt).Round(time.Second)
	fmt.Fprintf(w, "  %s  written %s ago (mtime: %s)\n",
		styleErr.Render("ACTIVE"),
		age,
		styleDim.Render(s.Backoff.WrittenAt.Format(time.RFC3339)))
	fmt.Fprintln(w, "  "+styleDim.Render("claude-using workers will silently skip until any worker's claude call succeeds."))
}

func RenderAccounts(w io.Writer, s Snapshot) {
	fmt.Fprintln(w, "ACCOUNTS")
	a := s.Accounts
	if a.GitHubLogin == "" && a.AnthropicWho == "" && a.LinearEmail == "" {
		fmt.Fprintln(w, "  (none recorded — brahma writes accounts.yaml at startup)")
		return
	}
	if a.GitHubLogin != "" {
		fmt.Fprintf(w, "  github:    %s\n", a.GitHubLogin)
	}
	if a.AnthropicWho != "" {
		fmt.Fprintf(w, "  anthropic: %s\n", a.AnthropicWho)
	}
	if a.LinearEmail != "" {
		fmt.Fprintf(w, "  linear:    %s\n", a.LinearEmail)
	}
	if a.WrittenAt != "" {
		fmt.Fprintf(w, "  recorded:  %s\n", a.WrittenAt)
	}
}

func RenderPipelines(w io.Writer, s Snapshot) {
	renderPipelines(w, s, false)
}

// RenderPipelinesWithTokens renders the pipelines table with the
// token-burn columns (SESS / INPUT / CACHE_R / OUTPUT / COST) joined
// inline per pipeline by matching ByPhase.Key to PipelineStat.Name.
// Pipelines with no token data show "-" in the token columns. A
// trailing TOTAL row sums every visible pipeline.
func RenderPipelinesWithTokens(w io.Writer, s Snapshot) {
	renderPipelines(w, s, true)
}

func renderPipelines(w io.Writer, s Snapshot, withTokens bool) {
	label := s.SinceLabel
	if label == "" {
		label = "last " + s.Since.String()
	}
	fmt.Fprintf(w, "PIPELINES (%s)\n", label)
	if len(s.Pipelines) == 0 {
		fmt.Fprintln(w, "  no journal entries in window — either no pipelines configured yet or none have fired")
		return
	}

	tokensByPhase := map[string]tokens.Rollup{}
	costAvail := false
	if withTokens && s.Tokens.Available {
		for _, r := range s.Tokens.ByPhase {
			tokensByPhase[r.Key] = r
		}
		costAvail = s.Tokens.CostAvailable
	}

	// All fields strings so the TOTAL row can use dashes for slots
	// that don't sum (RAN/RUN/CAP/ERR/LAST ERR/LAST OK).
	baseHdr := "  %-22s %6s %5s %8s %5s %10s %10s"
	baseRow := "  %-22s %6s %5s %8s %5s %10s %10s"
	hdrArgs := []any{"PIPELINE", "TICK", "RAN", "RUN/CAP", "ERR", "LAST ERR", "LAST OK"}
	withBudget := len(s.PipelineBudgets) > 0 || s.GlobalBudget != nil
	if withBudget {
		baseHdr += " %16s"
		baseRow += " %16s"
		hdrArgs = append(hdrArgs, "BUDGET")
	}
	if withTokens {
		baseHdr += " %5s %7s %8s %7s"
		baseRow += " %5s %7s %8s %7s"
		hdrArgs = append(hdrArgs, "SESS", "INPUT", "CACHE_R", "OUTPUT")
		if costAvail {
			baseHdr += " %8s"
			baseRow += " %8s"
			hdrArgs = append(hdrArgs, "COST")
		}
	}
	baseHdr += "\n"
	baseRow += "\n"
	fmt.Fprintf(w, baseHdr, hdrArgs...)

	var (
		totSess                  int
		totIn, totCacheR, totOut int64
		totCost                  float64
	)
	for _, p := range s.Pipelines {
		tick := "-"
		if d := s.PipelineTicks[p.Name]; d > 0 {
			tick = compactDuration(d)
		}
		runCap := "-"
		if c := s.PipelineCapacities[p.Name]; c > 0 {
			running := p.Running
			if running > c {
				running = c // orchestrator caps concurrency in memory; clamp display too
			}
			runCap = fmt.Sprintf("%d/%d", running, c)
		}
		// Pad each value to its column width FIRST, then wrap in
		// colour — ANSI escape bytes don't print but still count toward
		// %Ns padding, so colouring before padding skews the layout.
		paddedErr := fmt.Sprintf("%5d", p.Errors)
		if p.Errors > 0 {
			paddedErr = styleErr.Render(paddedErr)
		} else {
			paddedErr = styleDim.Render(paddedErr)
		}
		lastErr := fmt.Sprintf("%10s", "-")
		if !p.LastError.IsZero() {
			lastErr = styleErr.Render(fmt.Sprintf("%10s", HumanAgo(p.LastError)+" ago"))
		}
		lastOK := fmt.Sprintf("%10s", "-")
		if !p.LastSuccess.IsZero() {
			lastOK = styleOK.Render(fmt.Sprintf("%10s", HumanAgo(p.LastSuccess)+" ago"))
		}
		// runCap column highlighted when the pool has live workers.
		paddedRunCap := fmt.Sprintf("%8s", runCap)
		if p.Running > 0 {
			paddedRunCap = styleBusy.Render(paddedRunCap)
		}
		row := []any{p.Name, tick, strconv.Itoa(p.Ran), paddedRunCap, paddedErr, lastErr, lastOK}
		if withBudget {
			row = append(row, formatBudgetCell(s.PipelineBudgets[p.Name]))
		}
		if withTokens {
			r, ok := tokensByPhase[p.Name]
			sess, in, cacheR, out, cost := "-", "-", "-", "-", "-"
			if ok {
				sess = strconv.Itoa(r.Sessions)
				in = FormatTokens(r.Usage.InputTokens)
				cacheR = FormatTokens(r.Usage.CacheReadTokens)
				out = FormatTokens(r.Usage.OutputTokens)
				if costAvail {
					cost = fmt.Sprintf("$%.2f", r.Cost)
				}
				totSess += r.Sessions
				totIn += r.Usage.InputTokens
				totCacheR += r.Usage.CacheReadTokens
				totOut += r.Usage.OutputTokens
				totCost += r.Cost
			}
			row = append(row, sess, in, cacheR, out)
			if costAvail {
				row = append(row, cost)
			}
		}
		fmt.Fprintf(w, baseRow, row...)
	}

	if withTokens && len(s.Tokens.ByPhase) > 0 {
		totalArgs := []any{"TOTAL", "-", "-", "-", "-", "-", "-"}
		if withBudget {
			// The global cap spans every pipeline, so it lives on the TOTAL
			// row. A plain cell here (spent/cap) keeps its colour out of the
			// way; when exceeded the whole row is tainted red below.
			totalArgs = append(totalArgs, plainBudgetCell(s.GlobalBudget))
		}
		totalArgs = append(totalArgs, strconv.Itoa(totSess), FormatTokens(totIn), FormatTokens(totCacheR), FormatTokens(totOut))
		if costAvail {
			totalArgs = append(totalArgs, fmt.Sprintf("$%.2f", totCost))
		}
		line := fmt.Sprintf(baseRow, totalArgs...)
		if s.GlobalBudget != nil && s.GlobalBudget.Exceeded {
			line = styleErr.Render(strings.TrimRight(line, "\n")) + "\n"
		}
		fmt.Fprint(w, line)
	}
}

// formatBudgetCell renders "$<spent>/<cap>" padded to the BUDGET column,
// highlighted when the cap is currently holding launches. "-" for a
// pipeline without a budget.
func formatBudgetCell(b BudgetStat) string {
	if b.Limit.IsZero() {
		return fmt.Sprintf("%16s", "-")
	}
	cell := fmt.Sprintf("%16s", fmt.Sprintf("$%.2f/%s", b.Spent, b.Limit.String()))
	if b.Exceeded {
		return styleErr.Render(cell)
	}
	return cell
}

// plainBudgetCell is formatBudgetCell without the exceeded colour, for
// rows that carry their own row-level highlight. b may be nil (no cap).
func plainBudgetCell(b *BudgetStat) string {
	if b == nil || b.Limit.IsZero() {
		return fmt.Sprintf("%16s", "-")
	}
	return fmt.Sprintf("%16s", fmt.Sprintf("$%.2f/%s", b.Spent, b.Limit.String()))
}

// budgetHeldNotes returns one line per budget currently holding launches,
// each saying what is capped and when it resets. Empty when nothing is
// held. Surfaced on the operator-health panel so a hold reads as a health
// signal rather than table chrome.
func budgetHeldNotes(s Snapshot) []string {
	var notes []string
	add := func(scope string, b BudgetStat) {
		if !b.Exceeded {
			return
		}
		reset := "-"
		if !b.ResetAt.IsZero() {
			reset = fmt.Sprintf("in %s (%s)", compactDuration(time.Until(b.ResetAt).Round(time.Minute)), b.ResetAt.Local().Format("15:04"))
		}
		notes = append(notes, fmt.Sprintf("budget: %s exceeded %s ($%.2f spent) — launches held, resets %s", scope, b.Limit.String(), b.Spent, reset))
	}
	if s.GlobalBudget != nil {
		add("all pipelines", *s.GlobalBudget)
	}
	names := make([]string, 0, len(s.PipelineBudgets))
	for name := range s.PipelineBudgets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		add(name, s.PipelineBudgets[name])
	}
	return notes
}

// compactDuration formats a tick interval as a short human label
// (15s, 5m, 1h). Falls back to time.Duration.String() for odd values.
func compactDuration(d time.Duration) string {
	switch {
	case d >= 24*time.Hour && d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	case d >= time.Hour && d%time.Hour == 0:
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d >= time.Minute && d%time.Minute == 0:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d >= time.Second && d%time.Second == 0:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	return d.String()
}

// RenderTasks prints one block per task id, projected from the journal:
// the phase chain (what ran) plus the latest phase's outcome and detail
// note. Sorted by task id ascending.
func RenderTasks(w io.Writer, s Snapshot) {
	renderTasks(w, s, false)
}

// renderTasks is the parameterised core. When hidePruned is true, rows
// whose chain ends in `prune succeeded` are filtered out and the title
// gains a hint that the toggle is live.
func renderTasks(w io.Writer, s Snapshot, hidePruned bool) {
	label := s.SinceLabel
	if label == "" {
		label = "last " + s.Since.String()
	}
	rows := rowsFromTasks(s.Tasks)
	prunedCount := 0
	if hidePruned {
		kept := rows[:0]
		for _, r := range rows {
			if isPruned(r) {
				prunedCount++
				continue
			}
			kept = append(kept, r)
		}
		rows = kept
	}
	title := fmt.Sprintf("TASKS (%s, %d tasks)", label, len(s.Tasks))
	if hidePruned && prunedCount > 0 {
		title += " " + styleDim.Render(fmt.Sprintf("[%d pruned hidden — press P]", prunedCount))
	}
	fmt.Fprintln(w, title)
	if len(rows) == 0 {
		fmt.Fprintln(w, "  none in window")
		return
	}
	for _, r := range rows {
		stage := stageForRow(r)
		head := styleTitle.Render(r.TaskID)
		if stage != "" {
			head += "  " + stylizeStage(stage)
		}
		if !r.LastUpdate.IsZero() {
			head += "  " + styleDim.Render(HumanAgo(r.LastUpdate)+" ago")
		}
		fmt.Fprintf(w, "  %s\n", head)
		for _, line := range r.Logs {
			fmt.Fprintf(w, "    %s\n", formatTaskLog(line))
		}
		if len(r.Chain) > 0 {
			fmt.Fprintf(w, "    %s\n", formatChain(r.Chain))
			if note, isErr := lastPhaseNote(r.Chain); note != "" {
				style := styleDim
				if isErr {
					style = styleErr
				}
				fmt.Fprintf(w, "      %s %s\n", style.Render("↳"), style.Render(note))
			}
		}
	}
}

// stylizeStage returns a coloured rendering of a stage label so the
// task row's state pops at a glance: green for done/PR/prune-ok,
// yellow for in-progress, red for failure states, blue for verdicts.
func stylizeStage(s string) string {
	switch {
	case strings.HasSuffix(s, " in progress"):
		return styleBusy.Render(s)
	case strings.HasSuffix(s, " failed"), strings.HasSuffix(s, " timed out"), strings.HasSuffix(s, " dead"):
		return styleErr.Render(s)
	case strings.HasSuffix(s, " done"):
		return styleOK.Render(s)
	default:
		return s
	}
}

// latestStep returns the chain step whose most recent event is newest —
// a task's current stage by real recency, not by chain position. Ties
// (equal or zero timestamps) resolve to the later step, which — because
// the chain is in pipeline order — is the furthest-progressed stage.
// Returns false for an empty chain.
func latestStep(chain []PhaseStep) (PhaseStep, bool) {
	if len(chain) == 0 {
		return PhaseStep{}, false
	}
	best := 0
	for i := 1; i < len(chain); i++ {
		if !chain[i].LastAt.Before(chain[best].LastAt) {
			best = i
		}
	}
	return chain[best], true
}

// stageForRow returns the operator-facing one-line stage label for a
// task: its most recently active pipeline phase tagged with that phase's
// outcome. It is a pure projection of the journal chain — no
// worktree/marker inputs, no per-phase semantics. The phase name and any
// detail note (rendered separately) carry the meaning; the monitor only
// reports what ran and how it ended.
func stageForRow(r TaskStatusRow) string {
	last, ok := latestStep(r.Chain)
	if !ok {
		return ""
	}
	switch last.Outcome {
	case journal.OutcomeStarted:
		return last.Phase + " in progress"
	case journal.OutcomeFailed:
		return last.Phase + " failed"
	case journal.OutcomeTimedOut:
		return last.Phase + " timed out"
	case journal.OutcomeDead:
		return last.Phase + " dead"
	case journal.OutcomeSucceeded:
		return last.Phase + " done"
	}
	return last.Phase
}

// isPruned reports whether a row's most recent phase is `prune
// succeeded` — the journal evidence that the terminal pipeline has
// shipped this task. Keyed on the journal's own phase/outcome vocabulary
// (the orchestrator's contract), not on any worktree marker.
func isPruned(r TaskStatusRow) bool {
	last, ok := latestStep(r.Chain)
	return ok && last.Phase == "prune" && last.Outcome == journal.OutcomeSucceeded
}

// TaskStatusRow is one task's status as projected from the journal: the
// ordered phase chain (what ran) plus the outcome of each phase (what
// succeeded / failed). Built entirely from orchestrator-owned journal
// events — the monitor assigns no meaning to specific phase names or
// note text, so it stays decoupled from whatever pipeline the workers
// implement.
type TaskStatusRow struct {
	TaskID     string
	Chain      []PhaseStep
	Logs       []TaskLogLine // task-scoped lines shown under the header, ahead of the chain
	LastUpdate time.Time     // timestamp of the task's most recent journal event
}

// rowsFromTasks converts the journal-derived task chains into render
// rows, sorted by task id for output that's stable across ticks.
func rowsFromTasks(tasks []TaskRow) []TaskStatusRow {
	out := make([]TaskStatusRow, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, TaskStatusRow{TaskID: t.TaskID, Chain: t.Chain, Logs: t.Logs, LastUpdate: t.LastUpdate})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out
}

// formatChain renders a phase list as "investigate ✓ → implement ⏳"
// with phase names slightly dimmed so the coloured outcome symbols
// carry the visual weight.
func formatChain(chain []PhaseStep) string {
	parts := make([]string, 0, len(chain))
	for _, step := range chain {
		parts = append(parts, fmt.Sprintf("%s %s", styleDim.Render(step.Phase), OutcomeSymbol(step.Outcome)))
	}
	return strings.Join(parts, styleDim.Render(" → "))
}

// formatTaskLog renders one task-scoped log line as a bulleted line for
// display under the task header. Error outcomes colour the whole line
// red; anything else stays neutral so these lines read as status, not
// alarms.
func formatTaskLog(line TaskLogLine) string {
	switch line.Outcome {
	case journal.OutcomeFailed, journal.OutcomeTimedOut, journal.OutcomeDead:
		return styleErr.Render("• " + line.Text)
	default:
		return styleInfo.Render("•") + " " + line.Text
	}
}

// lastPhaseNote returns the most recently active pipeline phase's detail
// note as a single trimmed line, plus whether that phase ended in a
// failure (failed/timed_out/dead) so the caller can colour it. The note
// is the journal Note field verbatim — the monitor never parses it, just
// surfaces whatever the worker reported ("PR opened: …", "verdict=…",
// "awaiting prod deploy: …", a failure reason). Any "stderr tail" block
// is trimmed and newlines flattened so it fits one panel row.
func lastPhaseNote(chain []PhaseStep) (string, bool) {
	last, ok := latestStep(chain)
	if !ok {
		return "", false
	}
	isErr := false
	switch last.Outcome {
	case journal.OutcomeFailed, journal.OutcomeTimedOut, journal.OutcomeDead:
		isErr = true
	}
	note := last.Note
	if i := strings.Index(note, "--- stderr tail ---"); i >= 0 {
		note = note[:i]
	}
	note = strings.TrimSpace(strings.ReplaceAll(note, "\n", " "))
	if note == "" {
		return "", isErr
	}
	const max = 140
	if len(note) > max {
		note = note[:max-1] + "…"
	}
	return note, isErr
}

// RenderSilentFailures shows journal events the runner synthesised for
// workers that exited non-zero before claiming any task. This is the
// "I can't tail every log" panel — env errors, broken commands, missing
// binaries, etc. show up here with their stderr tail.
func RenderSilentFailures(w io.Writer, s Snapshot) {
	fmt.Fprintf(w, "SILENT FAILURES %s\n",
		styleDim.Render("(workers that exited non-zero without claiming a task)"))
	if len(s.SilentFailures) == 0 {
		fmt.Fprintln(w, "  "+styleOK.Render("none"))
		return
	}
	const showFull = 5
	for i, f := range s.SilentFailures {
		if i >= showFull {
			fmt.Fprintf(w, "  %s\n", styleDim.Render(fmt.Sprintf("… and %d more (use --since to widen the window)", len(s.SilentFailures)-showFull)))
			break
		}
		fmt.Fprintf(w, "  %s  pipeline=%-18s worker=%s\n",
			styleErr.Render(HumanAgo(f.When)+" ago"), f.Pipeline, f.WorkerID)
		for _, line := range strings.Split(strings.TrimRight(f.Note, "\n"), "\n") {
			fmt.Fprintf(w, "    %s\n", styleDim.Render(line))
		}
	}
}

// FormatTaskLine renders one task's phase chain like
// "QUA-345  prepare-worktree ✓ → investigate ✓ → implement ⏳"
func FormatTaskLine(t TaskRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-14s ", t.TaskID)
	for i, step := range t.Chain {
		if i > 0 {
			b.WriteString(" → ")
		}
		fmt.Fprintf(&b, "%s %s", step.Phase, OutcomeSymbol(step.Outcome))
	}
	return b.String()
}

func OutcomeSymbol(o journal.Outcome) string {
	switch o {
	case journal.OutcomeSucceeded:
		return styleOK.Render("✓")
	case journal.OutcomeFailed:
		return styleErr.Render("✗")
	case journal.OutcomeTimedOut:
		return styleMagenta.Render("⏱")
	case journal.OutcomeDead:
		return styleErr.Render("☠")
	case journal.OutcomeStarted:
		return styleBusy.Render("⏳")
	default:
		return styleDim.Render("?")
	}
}

// LogListWindow controls a windowed render of any of the drill-down
// lists (phases, tasks, runs): which row is selected, where the scroll
// viewport starts, and how many rows of output to emit. The first
// emitted row is always a column header — the cursor never lands on it.
type LogListWindow struct {
	Selected     int // index into the level's list (0-based, excludes column header)
	ScrollOffset int // first entry to render (0 = first entry, NOT column header)
	MaxRows      int // hard cap on rows printed; 0 = unlimited
}

// PhaseStat is one pool's aggregate at the phases drill-down level.
// Live is the count of entries whose log file mtime is fresh; Total
// is the count of every invocation log file we found in that pool.
type PhaseStat struct {
	Phase   string
	Live    int
	Total   int
	Newest  time.Time // mtime of the most-recent entry in this phase
	HasLive bool
}

// TaskStat is one task id's aggregate within a single phase. Same
// shape as PhaseStat with a TaskID instead of a phase.
type TaskStat struct {
	TaskID  string
	Live    int
	Total   int
	Newest  time.Time
	HasLive bool
}

// IdleTaskBucket is the synthetic task-id stand-in for log entries whose
// worker spawned, polled for work, found nothing, and exited 0 — those
// runs never write a journal event so we can't attribute them to a
// task. They're useful for spotting "is the orchestrator alive?" but
// dominate the list, so they're hidden by default and pinned to the
// bottom when shown.
const IdleTaskBucket = "(idle)"

// TaskStatsFor groups entries by task id across every phase. Sorted
// by Newest descending so the task with the freshest activity floats
// to the top, with one exception: the IdleTaskBucket sinks to the
// bottom regardless of its newest entry, since it represents idle
// polling rather than work. When showIdle is false the bucket is
// dropped entirely.
func TaskStatsFor(entries []LogEntry, showIdle bool) []TaskStat {
	idx := map[string]*TaskStat{}
	for _, e := range entries {
		key := e.TaskID
		if key == "" {
			if !showIdle {
				continue
			}
			key = IdleTaskBucket
		}
		t, ok := idx[key]
		if !ok {
			t = &TaskStat{TaskID: key}
			idx[key] = t
		}
		t.Total++
		if e.Live {
			t.Live++
			t.HasLive = true
		}
		if e.ModTime.After(t.Newest) {
			t.Newest = e.ModTime
		}
	}
	out := make([]TaskStat, 0, len(idx))
	for _, t := range idx {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool {
		iIdle := out[i].TaskID == IdleTaskBucket
		jIdle := out[j].TaskID == IdleTaskBucket
		if iIdle != jIdle {
			return !iIdle // idle bucket always sinks to the bottom
		}
		return out[i].Newest.After(out[j].Newest)
	})
	return out
}

// PhaseStatsFor groups entries belonging to one task by phase (pool)
// and rolls up live/total counts. Sorted by Newest descending.
func PhaseStatsFor(entries []LogEntry, task string) []PhaseStat {
	idx := map[string]*PhaseStat{}
	for _, e := range entries {
		key := e.TaskID
		if key == "" {
			key = IdleTaskBucket
		}
		if key != task {
			continue
		}
		p, ok := idx[e.Pool]
		if !ok {
			p = &PhaseStat{Phase: e.Pool}
			idx[e.Pool] = p
		}
		p.Total++
		if e.Live {
			p.Live++
			p.HasLive = true
		}
		if e.ModTime.After(p.Newest) {
			p.Newest = e.ModTime
		}
	}
	out := make([]PhaseStat, 0, len(idx))
	for _, p := range idx {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Newest.After(out[j].Newest) })
	return out
}

// RunsFor returns the worker-log entries that match phase + task,
// sorted by ModTime descending (live + most-recent first) so the
// freshest run is at the top — operators reaching this level usually
// want the most recent invocation.
func RunsFor(entries []LogEntry, phase, task string) []LogEntry {
	out := make([]LogEntry, 0, len(entries))
	for _, e := range entries {
		if e.Pool != phase {
			continue
		}
		key := e.TaskID
		if key == "" {
			key = IdleTaskBucket
		}
		if key != task {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModTime.After(out[j].ModTime) })
	return out
}

// RenderPhaseList prints the phases drill-down: PHASE, LIVE/TOTAL,
// NEWEST. Cursor row is marked with "> "; all others align to the
// same indent. Window-aware: clipped rows are replaced with "↑/↓ N
// more" indicators.
func RenderPhaseList(w io.Writer, phases []PhaseStat, win LogListWindow) {
	header := fmt.Sprintf("  %-20s %12s %8s", "PHASE", "LIVE/TOTAL", "NEWEST")
	if len(phases) == 0 {
		fmt.Fprintln(w, header)
		fmt.Fprintln(w, "  "+styleDim.Render("no worker logs under <state>/workers/"))
		return
	}
	rows := make([]string, 0, len(phases))
	for i, p := range phases {
		liveMark := ""
		if p.HasLive {
			liveMark = " " + styleOK.Render("●")
		}
		rows = append(rows, renderListRow(
			i == win.Selected,
			fmt.Sprintf("%-20s %12s %8s%s",
				p.Phase, fmt.Sprintf("%d / %d", p.Live, p.Total),
				HumanAgo(p.Newest), liveMark),
		))
	}
	writeWindowed(w, header, rows, win)
}

// RenderTaskList prints the tasks drill-down for one phase: TASK,
// LIVE/TOTAL, NEWEST. Same windowing rules as RenderPhaseList.
func RenderTaskList(w io.Writer, tasks []TaskStat, win LogListWindow) {
	header := fmt.Sprintf("  %-20s %12s %8s", "TASK", "LIVE/TOTAL", "NEWEST")
	if len(tasks) == 0 {
		fmt.Fprintln(w, header)
		fmt.Fprintln(w, "  "+styleDim.Render("no runs for this phase"))
		return
	}
	rows := make([]string, 0, len(tasks))
	for i, t := range tasks {
		liveMark := ""
		if t.HasLive {
			liveMark = " " + styleOK.Render("●")
		}
		// Idle bucket gets a muted treatment regardless of cursor.
		body := fmt.Sprintf("%-20s %12s %8s%s",
			t.TaskID, fmt.Sprintf("%d / %d", t.Live, t.Total),
			HumanAgo(t.Newest), liveMark)
		if t.TaskID == IdleTaskBucket {
			body = styleDim.Render(body)
		}
		rows = append(rows, renderListRow(i == win.Selected, body))
	}
	writeWindowed(w, header, rows, win)
}

// RenderRunList prints the runs drill-down for one (phase, task) — the
// individual worker invocations, sorted live + newest first. Each row
// shows worker id, age, size, and a live dot.
func RenderRunList(w io.Writer, runs []LogEntry, win LogListWindow) {
	header := fmt.Sprintf("  %-12s %8s %8s %s", "WORKER", "AGE", "SIZE", "LIVE")
	if len(runs) == 0 {
		fmt.Fprintln(w, header)
		fmt.Fprintln(w, "  "+styleDim.Render("no runs for this task"))
		return
	}
	rows := make([]string, 0, len(runs))
	for i, e := range runs {
		live := " "
		if e.Live {
			live = styleOK.Render("●")
		}
		rows = append(rows, renderListRow(
			i == win.Selected,
			fmt.Sprintf("%-12s %8s %8s   %s",
				e.WorkerID, HumanAgo(e.ModTime), humanBytes(e.Size), live),
		))
	}
	writeWindowed(w, header, rows, win)
}

// renderListRow prepends either "> " (cursor) or "  " (idle) to body
// and applies the cursor highlight so the selected row stands out
// without disrupting column widths.
func renderListRow(selected bool, body string) string {
	if selected {
		return styleInfo.Render("> ") + styleInfo.Render(body)
	}
	return "  " + body
}

// writeWindowed prints header + a windowed slice of rows with clip
// indicators. Header counts as one row at the top of the budget so
// it stays visible.
func writeWindowed(w io.Writer, header string, rows []string, win LogListWindow) {
	fmt.Fprintln(w, header)
	budget := win.MaxRows - 1 // header consumed one row
	if budget <= 0 {
		budget = len(rows)
	}
	start, end := clampWindow(len(rows), win.ScrollOffset, budget)
	for i := start; i < end; i++ {
		switch {
		case i == start && start > 0:
			fmt.Fprintf(w, "  ↑ %d more above\n", start)
		case i == end-1 && end < len(rows):
			fmt.Fprintf(w, "  ↓ %d more below\n", len(rows)-end)
		default:
			fmt.Fprintln(w, rows[i])
		}
	}
}

// clampWindow returns the [start, end) row range to render given the
// total row count, the requested offset, and the max rows. Offset is
// clamped to [0, total - maxRows]; end is bounded by total.
func clampWindow(total, offset, maxRows int) (int, int) {
	if maxRows <= 0 || maxRows >= total {
		return 0, total
	}
	if offset < 0 {
		offset = 0
	}
	if offset > total-maxRows {
		offset = total - maxRows
	}
	return offset, offset + maxRows
}

// humanBytes renders a file size with K/M suffix and one decimal of
// precision in the 1k–10k / 1M–10M ranges. Matches the formatting of
// the token panel's FormatTokens for visual consistency.
func humanBytes(n int64) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%dB", n)
	case n < 10*1024:
		return strings.TrimSuffix(fmt.Sprintf("%.1fK", float64(n)/1024), ".0K") + "K"
	case n < 1024*1024:
		return fmt.Sprintf("%dK", n/1024)
	case n < 10*1024*1024:
		return strings.TrimSuffix(fmt.Sprintf("%.1fM", float64(n)/(1024*1024)), ".0M") + "M"
	default:
		return fmt.Sprintf("%dM", n/(1024*1024))
	}
}

func RenderTokens(w io.Writer, s Snapshot) {
	fmt.Fprintf(w, "TOKEN BURN (%s)\n", s.Tokens.Note)
	if !s.Tokens.Available {
		return
	}
	header := "  %-22s %5s %7s %8s %8s %8s"
	rowFmt := "  %-22s %5d %7s %8s %8s %8s"
	if s.Tokens.CostAvailable {
		header += " %9s"
		rowFmt += " %9s"
	}
	header += "\n"
	rowFmt += "\n"

	args := []any{"PHASE", "SESS", "INPUT", "CACHE_W", "CACHE_R", "OUTPUT"}
	if s.Tokens.CostAvailable {
		args = append(args, "COST")
	}
	fmt.Fprintf(w, header, args...)

	var (
		totSess                             int
		totIn, totCacheW, totCacheR, totOut int64
		totCost                             float64
	)
	for _, r := range s.Tokens.ByPhase {
		fields := []any{
			r.Key, r.Sessions,
			FormatTokens(r.Usage.InputTokens),
			FormatTokens(r.Usage.CacheCreationTokens),
			FormatTokens(r.Usage.CacheReadTokens),
			FormatTokens(r.Usage.OutputTokens),
		}
		if s.Tokens.CostAvailable {
			fields = append(fields, fmt.Sprintf("$%.2f", r.Cost))
		}
		fmt.Fprintf(w, rowFmt, fields...)

		totSess += r.Sessions
		totIn += r.Usage.InputTokens
		totCacheW += r.Usage.CacheCreationTokens
		totCacheR += r.Usage.CacheReadTokens
		totOut += r.Usage.OutputTokens
		totCost += r.Cost
	}
	if len(s.Tokens.ByPhase) == 0 {
		return
	}
	totalFields := []any{
		"TOTAL", totSess,
		FormatTokens(totIn),
		FormatTokens(totCacheW),
		FormatTokens(totCacheR),
		FormatTokens(totOut),
	}
	if s.Tokens.CostAvailable {
		totalFields = append(totalFields, fmt.Sprintf("$%.2f", totCost))
	}
	fmt.Fprintf(w, rowFmt, totalFields...)
}

// ---- helpers -------------------------------------------------------------

// FormatTokens renders large counts in compact human form: "936",
// "12k", "1.7M". One decimal of precision in the 1k–10k and 1M–10M
// ranges; no decimal above 10k / 10M.
func FormatTokens(n int64) string {
	if n == 0 {
		return "-"
	}
	abs := n
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs < 1000:
		return strconv.FormatInt(n, 10)
	case abs < 10_000:
		return strings.TrimSuffix(fmt.Sprintf("%.1fk", float64(n)/1000), ".0k") + suffixIfDropped(n, "k")
	case abs < 1_000_000:
		return fmt.Sprintf("%dk", n/1000)
	case abs < 10_000_000:
		return strings.TrimSuffix(fmt.Sprintf("%.1fM", float64(n)/1_000_000), ".0M") + suffixIfDropped(n, "M")
	case abs < 1_000_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	default:
		return strings.TrimSuffix(fmt.Sprintf("%.1fB", float64(n)/1_000_000_000), ".0B") + suffixIfDropped(n, "B")
	}
}

func suffixIfDropped(n int64, unit string) string {
	mod := map[string]int64{"k": 1000, "M": 1_000_000, "B": 1_000_000_000}[unit]
	if mod > 0 && n%mod == 0 {
		return unit
	}
	return ""
}

func HumanAgo(t time.Time) string {
	return HumanDuration(time.Since(t))
}

// HumanDuration renders a duration as a short, scannable label:
// "42s" / "12m" / "3h" / "5d".
func HumanDuration(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// sinceHeaderLabel returns the human-readable lookback descriptor for
// the header line. Prefers the explicit SinceLabel (which already
// covers "today" / "last 6h" / "all time"); falls back to the raw
// Since duration when only that's populated.
func sinceHeaderLabel(s Snapshot) string {
	if s.SinceLabel != "" {
		return s.SinceLabel
	}
	if s.Since > 0 {
		return "last " + s.Since.String()
	}
	return ""
}

func ShortenHome(p string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return p
	}
	if strings.HasPrefix(p, home) {
		return "~" + strings.TrimPrefix(p, home)
	}
	return p
}

func nonEmpty(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}
