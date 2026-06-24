package agentui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grasskode/bramha/internal/journal"
	"github.com/grasskode/bramha/internal/tokens"
)

func TestResolveSinceMovesWithClock(t *testing.T) {
	t1 := time.Date(2026, 6, 19, 9, 0, 0, 0, time.UTC)
	t2 := t1.Add(3 * time.Hour) // a later refresh

	// Relative window slides: cutoff stays 6h behind the clock.
	c1, dur, label, err := ResolveSince("6h", t1)
	if err != nil {
		t.Fatalf("ResolveSince(6h): %v", err)
	}
	if !c1.Equal(t1.Add(-6*time.Hour)) || dur != 6*time.Hour || label != "last 6h" {
		t.Fatalf("ResolveSince(6h) = (%v, %v, %q), want (%v, 6h, \"last 6h\")", c1, dur, label, t1.Add(-6*time.Hour))
	}
	c2, _, _, _ := ResolveSince("6h", t2)
	if !c2.After(c1) {
		t.Fatalf("cutoff did not advance with the clock: %v then %v", c1, c2)
	}

	// "today" tracks the supplied now's local midnight; duration is zero.
	cToday, todayDur, todayLabel, err := ResolveSince("today", t2)
	if err != nil {
		t.Fatalf("ResolveSince(today): %v", err)
	}
	y, m, d := t2.Local().Date()
	wantMidnight := time.Date(y, m, d, 0, 0, 0, 0, t2.Location())
	if !cToday.Equal(wantMidnight) || todayDur != 0 || todayLabel != "today" {
		t.Fatalf("ResolveSince(today) = (%v, %v, %q), want (%v, 0, \"today\")", cToday, todayDur, todayLabel, wantMidnight)
	}

	// "all" means no cutoff.
	cAll, allDur, allLabel, err := ResolveSince("all", t2)
	if err != nil {
		t.Fatalf("ResolveSince(all): %v", err)
	}
	if !cAll.IsZero() || allDur != 0 || allLabel != "all time" {
		t.Fatalf("ResolveSince(all) = (%v, %v, %q), want (zero, 0, \"all time\")", cAll, allDur, allLabel)
	}

	if _, _, _, err := ResolveSince("nonsense", t2); err == nil {
		t.Fatal("ResolveSince(nonsense) = nil error, want parse error")
	}
}

func TestCollectTasks_LastUpdateIsNewestEvent(t *testing.T) {
	base := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	latest := base.Add(5 * time.Minute)
	events := []journal.Event{
		{Timestamp: base, TaskID: "QUA-1", Pool: "investigate", Phase: "investigate", Outcome: journal.OutcomeSucceeded},
		{Timestamp: latest, TaskID: "QUA-1", Pool: "implement", Phase: "implement", Outcome: journal.OutcomeStarted},
		// Older-timestamped event written last must not pull LastUpdate back.
		{Timestamp: base.Add(-time.Minute), TaskID: "QUA-1", Pool: "investigate", Phase: "investigate", Outcome: journal.OutcomeStarted},
	}
	var s Snapshot
	s.collectTasks(events)

	if len(s.Tasks) != 1 {
		t.Fatalf("got %d task rows, want 1", len(s.Tasks))
	}
	if !s.Tasks[0].LastUpdate.Equal(latest) {
		t.Fatalf("LastUpdate = %v, want %v", s.Tasks[0].LastUpdate, latest)
	}
}

func TestRenderTokens_EmitsTotalRowAfterByPhase(t *testing.T) {
	s := Snapshot{
		Tokens: TokensPanel{
			Available: true,
			Note:      "last 6h",
			ByPhase: []tokens.Rollup{
				{Key: "investigate", Sessions: 2, Usage: tokens.Usage{InputTokens: 1000, OutputTokens: 200}},
				{Key: "implement", Sessions: 3, Usage: tokens.Usage{InputTokens: 4000, OutputTokens: 800}},
			},
		},
	}
	var b strings.Builder
	RenderTokens(&b, s)
	out := b.String()

	if !strings.Contains(out, "TOTAL") {
		t.Fatalf("expected TOTAL row in:\n%s", out)
	}
	// TOTAL must come after both phase rows so a quick scan reaches the
	// rollup at the bottom.
	idxInv := strings.Index(out, "investigate")
	idxImp := strings.Index(out, "implement")
	idxTot := strings.Index(out, "TOTAL")
	if idxInv < 0 || idxImp < 0 || idxTot < 0 {
		t.Fatalf("missing rows in:\n%s", out)
	}
	if idxTot < idxInv || idxTot < idxImp {
		t.Errorf("TOTAL must appear after phase rows; got investigate=%d implement=%d TOTAL=%d", idxInv, idxImp, idxTot)
	}
	// 2 + 3 = 5 sessions total — render formats as %5d so look for the
	// padded number after "TOTAL".
	totalLine := out[idxTot:]
	totalLine = totalLine[:strings.Index(totalLine, "\n")]
	if !strings.Contains(totalLine, "5") {
		t.Errorf("TOTAL row should sum sessions to 5, got %q", totalLine)
	}
}

func TestRenderTokens_NoRowsSkipsTotal(t *testing.T) {
	s := Snapshot{Tokens: TokensPanel{Available: true, Note: "last 6h"}}
	var b strings.Builder
	RenderTokens(&b, s)
	if strings.Contains(b.String(), "TOTAL") {
		t.Errorf("empty rollup should not render TOTAL: %q", b.String())
	}
}

func TestCollectTasks_SkipsExecSubevents(t *testing.T) {
	// Mirrors a real QUA-213 prune sequence: the worker emitted a
	// failed exec:git:branch-delete sub-event before its own
	// pipeline-level success. Only the pipeline-level outcome should
	// land in the chain.
	events := []journal.Event{
		{TaskID: "QUA-213", Pool: "prune", Phase: "prune", Outcome: journal.OutcomeStarted},
		{TaskID: "QUA-213", Pool: "prune", Phase: "exec:git:worktree-remove", Outcome: journal.OutcomeSucceeded},
		{TaskID: "QUA-213", Pool: "prune", Phase: "exec:git:branch-delete", Outcome: journal.OutcomeFailed, Note: "fatal: not a git repository"},
		{TaskID: "QUA-213", Pool: "prune", Phase: "prune", Outcome: journal.OutcomeSucceeded, Note: "worktree removed"},
	}
	var s Snapshot
	s.collectTasks(events)

	if len(s.Tasks) != 1 || s.Tasks[0].TaskID != "QUA-213" {
		t.Fatalf("expected one task row for QUA-213, got %+v", s.Tasks)
	}
	chain := s.Tasks[0].Chain
	if len(chain) != 1 {
		t.Fatalf("chain should hold one pipeline-level step; got %+v", chain)
	}
	if chain[0].Phase != "prune" || chain[0].Outcome != journal.OutcomeSucceeded {
		t.Errorf("chain head wrong: %+v", chain[0])
	}
}

func TestLastPhaseNote_FormatsAndTruncates(t *testing.T) {
	chain := []PhaseStep{
		{Phase: "investigate", Outcome: journal.OutcomeSucceeded, Note: "verdict=clear"},
		{Phase: "implement", Outcome: journal.OutcomeFailed, Note: "exit 1: tests broken\n--- stderr tail ---\nlots of noise here"},
	}
	got, isErr := lastPhaseNote(chain)
	if got == "" {
		t.Fatalf("want a note for trailing failed step")
	}
	if !isErr {
		t.Errorf("trailing failed step should report isErr=true")
	}
	if strings.Contains(got, "stderr tail") {
		t.Errorf("stderr tail block should be stripped; got %q", got)
	}
	if strings.Contains(got, "\n") {
		t.Errorf("note must be flattened to a single line; got %q", got)
	}
}

func TestRenderPipelinesWithTokens_InlineColumnsAndTotal(t *testing.T) {
	now := time.Now()
	s := Snapshot{
		Since:      6 * time.Hour,
		SinceLabel: "last 6h",
		Pipelines: []PipelineStat{
			{Name: "investigate", Ran: 3, Running: 1, Errors: 1, LastError: now.Add(-2 * time.Minute), LastSuccess: now.Add(-1 * time.Minute)},
			{Name: "implement", Ran: 5, Running: 0, LastSuccess: now.Add(-1 * time.Minute)},
			{Name: "prune", Ran: 10, Running: 0, Errors: 8, LastError: now.Add(-45 * time.Second), LastSuccess: now.Add(-2 * time.Minute)},
		},
		PipelineOrder:      []string{"investigate", "implement", "prune"},
		PipelineTicks:      map[string]time.Duration{"investigate": 5 * time.Minute, "implement": 5 * time.Minute, "prune": 15 * time.Minute},
		PipelineCapacities: map[string]int{"investigate": 2, "implement": 2, "prune": 1},
		Tokens: TokensPanel{
			Available:      true,
			CcusageEnabled: true,
			Note:           "last 6h",
			ByPhase: []tokens.Rollup{
				{Key: "investigate", Sessions: 3, Usage: tokens.Usage{InputTokens: 1200, OutputTokens: 300, CacheReadTokens: 5000}, Cost: 0.42},
				{Key: "implement", Sessions: 5, Usage: tokens.Usage{InputTokens: 8000, OutputTokens: 2000, CacheReadTokens: 50000}, Cost: 3.10},
				// no entry for prune — should render as dashes
			},
		},
	}

	var b strings.Builder
	RenderPipelinesWithTokens(&b, s)
	out := b.String()

	// New baseline columns.
	for _, want := range []string{"PIPELINE", "TICK", "RAN", "RUN/CAP", "ERR", "LAST ERR", "LAST OK"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing pipeline header %q in:\n%s", want, out)
		}
	}
	// Inline token headers must appear in the pipelines table header.
	for _, want := range []string{"SESS", "INPUT", "CACHE_R", "OUTPUT", "COST"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing inline token header %q in:\n%s", want, out)
		}
	}
	// The standalone TOKEN BURN heading must not appear.
	if strings.Contains(out, "TOKEN BURN") {
		t.Errorf("inline rendering must not emit TOKEN BURN heading:\n%s", out)
	}
	// investigate row carries its RUN/CAP, ERR count, and cost.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "  investigate") {
			if !strings.Contains(line, "1/2") {
				t.Errorf("investigate row should show 1/2 for RUN/CAP: %q", line)
			}
			if !strings.Contains(line, "$0.42") {
				t.Errorf("investigate row missing cost on its line: %q", line)
			}
		}
	}
	// TOTAL row at the bottom summing tokens.
	if !strings.Contains(out, "TOTAL") {
		t.Errorf("expected TOTAL row at the bottom:\n%s", out)
	}
}

func TestStageForRow_FollowsLatestChainOutcome(t *testing.T) {
	r := TaskStatusRow{
		TaskID: "QUA-418",
		Chain: []PhaseStep{
			{Phase: "prepare-worktree", Outcome: journal.OutcomeSucceeded},
			{Phase: "investigate", Outcome: journal.OutcomeSucceeded},
			{Phase: "implement", Outcome: journal.OutcomeStarted},
		},
	}
	if got := stageForRow(r); got != "implement in progress" {
		t.Errorf("stage = %q, want \"implement in progress\"", got)
	}
}

func TestStageForRow_GenericPhasePlusOutcome(t *testing.T) {
	// No worker semantics: the stage is just the latest phase + its
	// outcome. A merged-PR-awaiting-deploy task reads as "finalize done"
	// (with the detail note carrying the specifics), not a bespoke
	// "PR open" / "merged · awaiting prod deploy" label.
	cases := []struct {
		name  string
		chain []PhaseStep
		want  string
	}{
		{"succeeded", []PhaseStep{{Phase: "github-notify", Outcome: journal.OutcomeSucceeded}}, "github-notify done"},
		{"failed", []PhaseStep{{Phase: "review", Outcome: journal.OutcomeFailed}}, "review failed"},
		{"timed out", []PhaseStep{{Phase: "implement", Outcome: journal.OutcomeTimedOut}}, "implement timed out"},
		{"dead", []PhaseStep{{Phase: "finalize", Outcome: journal.OutcomeDead}}, "finalize dead"},
		{"finalize done", []PhaseStep{{Phase: "finalize", Outcome: journal.OutcomeSucceeded}}, "finalize done"},
		{"empty", nil, ""},
	}
	for _, c := range cases {
		if got := stageForRow(TaskStatusRow{Chain: c.chain}); got != c.want {
			t.Errorf("%s: stage = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestCollectPipelines_FoldsErrorsAndLastTimes(t *testing.T) {
	now := time.Now().UTC()
	events := []journal.Event{
		{Timestamp: now.Add(-10 * time.Minute), Pool: "implement", Phase: "implement", WorkerID: "w1", TaskID: "QUA-1", Outcome: journal.OutcomeStarted},
		{Timestamp: now.Add(-9 * time.Minute), Pool: "implement", Phase: "implement", WorkerID: "w1", TaskID: "QUA-1", Outcome: journal.OutcomeFailed, Note: "boom"},
		// exec sub-event — must be ignored.
		{Timestamp: now.Add(-8 * time.Minute), Pool: "implement", Phase: "exec:claude:/implement", WorkerID: "w1", TaskID: "QUA-1", Outcome: journal.OutcomeFailed, Note: "retry"},
		{Timestamp: now.Add(-5 * time.Minute), Pool: "implement", Phase: "implement", WorkerID: "w2", TaskID: "QUA-2", Outcome: journal.OutcomeStarted},
		{Timestamp: now.Add(-1 * time.Minute), Pool: "implement", Phase: "implement", WorkerID: "w2", TaskID: "QUA-2", Outcome: journal.OutcomeSucceeded},
	}
	var s Snapshot
	s.collectPipelines(events)
	if len(s.Pipelines) != 1 {
		t.Fatalf("expected one pipeline row, got %+v", s.Pipelines)
	}
	p := s.Pipelines[0]
	if p.Ran != 2 {
		t.Errorf("Ran = %d, want 2 unique started events", p.Ran)
	}
	if p.Errors != 1 {
		t.Errorf("Errors = %d, want 1 (exec:* sub-event must be skipped)", p.Errors)
	}
	if p.LastError.IsZero() {
		t.Errorf("LastError must be populated when a pipeline-level error occurred")
	}
	if p.LastSuccess.IsZero() {
		t.Errorf("LastSuccess must be populated for w2's terminal success")
	}
	if !p.LastSuccess.After(p.LastError) {
		t.Errorf("LastSuccess (%s) should be after LastError (%s)", p.LastSuccess, p.LastError)
	}
}

func TestCollectRunningFromLogs_CountsOnlyWorkersWithLivePID(t *testing.T) {
	// PID-based liveness: only workers whose .pid file points to a
	// live process count. A missing pid file (clean exit removed it)
	// or a dead pid both mean "not running".
	root := t.TempDir()
	mustWritePID(t, root, "implement", "live", os.Getpid()) // this test process — guaranteed alive
	mustWritePID(t, root, "implement", "dead", deadPID(t))  // never-allocated pid
	// "exited" has no pid file at all — the orchestrator removed it
	// on clean exit; just a leftover log file remains.
	mustWriteLog(t, root, "implement", "exited", "completed cleanly")

	s := Snapshot{
		StateDir:      root,
		Pipelines:     []PipelineStat{{Name: "implement"}},
		PipelineOrder: []string{"implement"},
	}
	s.collectRunningFromLogs()

	if s.Pipelines[0].Running != 1 {
		t.Errorf("Running = %d, want 1 (only the worker with a live pid counts)", s.Pipelines[0].Running)
	}
}

func TestRenderTasks_HidePrunedFiltersChainsEndingInPruneSucceeded(t *testing.T) {
	// The only "shipped" signal is journal evidence: a chain ending in
	// `prune succeeded`. QUA-300 is shipped; QUA-100 (mid-flight) and
	// QUA-200 (finalize done, not yet pruned) stay visible.
	s := Snapshot{
		SinceLabel: "last 6h",
		Tasks: []TaskRow{
			{TaskID: "QUA-100", Chain: []PhaseStep{
				{Phase: "investigate", Outcome: journal.OutcomeStarted},
			}},
			{TaskID: "QUA-200", Chain: []PhaseStep{
				{Phase: "github-notify", Outcome: journal.OutcomeSucceeded},
				{Phase: "finalize", Outcome: journal.OutcomeSucceeded},
			}},
			{TaskID: "QUA-300", Chain: []PhaseStep{
				{Phase: "investigate", Outcome: journal.OutcomeSucceeded},
				{Phase: "implement", Outcome: journal.OutcomeSucceeded},
				{Phase: "review", Outcome: journal.OutcomeSucceeded},
				{Phase: "prune", Outcome: journal.OutcomeSucceeded},
			}},
		},
	}

	var visible strings.Builder
	renderTasks(&visible, s, false)
	for _, want := range []string{"QUA-100", "QUA-200", "QUA-300"} {
		if !strings.Contains(visible.String(), want) {
			t.Errorf("default render must include %s; got:\n%s", want, visible.String())
		}
	}

	var hidden strings.Builder
	renderTasks(&hidden, s, true)
	if strings.Contains(hidden.String(), "QUA-300") {
		t.Errorf("hidePruned=true must drop chain-pruned QUA-300; got:\n%s", hidden.String())
	}
	for _, kept := range []string{"QUA-100", "QUA-200"} {
		if !strings.Contains(hidden.String(), kept) {
			t.Errorf("hidePruned must keep %s; got:\n%s", kept, hidden.String())
		}
	}
	if !strings.Contains(hidden.String(), "1 pruned hidden") {
		t.Errorf("title must annotate hidden count; got:\n%s", hidden.String())
	}
}

func TestCollectRunningFromLogs_IgnoresMalformedPIDFile(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "workers", "implement"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "workers", "implement", "garbage.pid"), []byte("not-a-number\n"), 0o644); err != nil {
		t.Fatalf("write pid: %v", err)
	}
	s := Snapshot{
		StateDir:      root,
		Pipelines:     []PipelineStat{{Name: "implement"}},
		PipelineOrder: []string{"implement"},
	}
	s.collectRunningFromLogs()
	if s.Pipelines[0].Running != 0 {
		t.Errorf("Running = %d, want 0 (malformed pid file must not register)", s.Pipelines[0].Running)
	}
}

func TestRenderPipelines_RunCapClampedToCapacity(t *testing.T) {
	s := Snapshot{
		SinceLabel:         "last 6h",
		Pipelines:          []PipelineStat{{Name: "implement", Ran: 5, Running: 99}}, // upstream over-count
		PipelineCapacities: map[string]int{"implement": 2},
		PipelineOrder:      []string{"implement"},
	}
	var b strings.Builder
	RenderPipelines(&b, s)
	out := b.String()
	if !strings.Contains(out, "2/2") {
		t.Errorf("expected Running clamped to capacity (2/2); got:\n%s", out)
	}
	if strings.Contains(out, "99/2") {
		t.Errorf("over-count should never leak to display; got:\n%s", out)
	}
}

func TestLastPhaseNote_SurfacesSucceededNoteWithoutErrorFlag(t *testing.T) {
	// A succeeded phase's note is still shown (verbatim) — that's how PR
	// URLs / verdicts / "awaiting prod deploy" reach the operator — but
	// it must not be flagged as an error.
	chain := []PhaseStep{
		{Phase: "implement", Outcome: journal.OutcomeSucceeded, Note: "PR opened: https://x/pull/1"},
	}
	got, isErr := lastPhaseNote(chain)
	if got != "PR opened: https://x/pull/1" {
		t.Errorf("succeeded note should surface verbatim; got %q", got)
	}
	if isErr {
		t.Errorf("succeeded phase must not be flagged as error")
	}
}
