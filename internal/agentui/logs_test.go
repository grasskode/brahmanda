package agentui

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
)

func TestDiscoverLogs_ResolvesTaskIDAndOrdersLiveFirst(t *testing.T) {
	root := t.TempDir()

	mustWriteLog(t, root, "implement", "old-worker", "stale")
	mustBackdate(t, filepath.Join(root, "workers", "implement", "old-worker.log"), 10*time.Minute)
	mustJournalAppend(t, root, "implement", "old-worker", "QUA-100", journal.OutcomeStarted)
	mustJournalAppend(t, root, "implement", "old-worker", "QUA-100", journal.OutcomeFailed)

	mustWriteLog(t, root, "review", "fresh-worker", "live")
	mustJournalAppend(t, root, "review", "fresh-worker", "QUA-200", journal.OutcomeStarted)

	// A pool with no journal file — task id should resolve to "".
	mustWriteLog(t, root, "finalize", "orphan", "no-journal")

	entries := DiscoverLogs(root, 30*time.Second)
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d (%+v)", len(entries), entries)
	}

	// Live entries sort first. We wrote two live ones (review/fresh-worker
	// and finalize/orphan) and one stale (implement/old-worker).
	if entries[0].Live == false || entries[1].Live == false {
		t.Fatalf("expected first two entries live, got %+v", entries)
	}
	if entries[2].Live == true {
		t.Fatalf("expected last entry stale, got %+v", entries[2])
	}
	if entries[2].WorkerID != "old-worker" {
		t.Fatalf("expected stale entry to be old-worker, got %s", entries[2].WorkerID)
	}
	if entries[2].TaskID != "QUA-100" {
		t.Fatalf("expected task id QUA-100 from journal, got %q", entries[2].TaskID)
	}
	// Resolve the fresh-worker entry regardless of position.
	for _, e := range entries {
		if e.WorkerID == "fresh-worker" && e.TaskID != "QUA-200" {
			t.Fatalf("fresh-worker task id = %q, want QUA-200", e.TaskID)
		}
		if e.WorkerID == "orphan" && e.TaskID != "" {
			t.Fatalf("orphan task id = %q, want empty (no journal)", e.TaskID)
		}
	}
}

func TestDiscoverLogs_EmptyStateReturnsNil(t *testing.T) {
	root := t.TempDir()
	if got := DiscoverLogs(root, 30*time.Second); got != nil {
		t.Fatalf("want nil for missing workers/ dir, got %+v", got)
	}
}

func TestTailFile_TrimsLeadingPartialLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.log")
	// 5 lines of 100 bytes each = 500 bytes. Tail 200 bytes: the
	// 200-byte window will start mid-line; the function must drop the
	// partial leading line.
	var b strings.Builder
	for i := 0; i < 5; i++ {
		b.WriteString(strings.Repeat("abcdefghij", 9))
		b.WriteString("\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := TailFile(path, 200)
	if err != nil {
		t.Fatalf("TailFile: %v", err)
	}
	for _, line := range strings.Split(strings.TrimRight(got, "\n"), "\n") {
		if len(line) != 90 {
			t.Fatalf("partial line in tail: len=%d, content=%q", len(line), line)
		}
	}
}

func TestTailFile_MissingFileReturnsEmpty(t *testing.T) {
	got, err := TailFile(filepath.Join(t.TempDir(), "absent.log"), 1024)
	if err != nil {
		t.Fatalf("want nil error for missing file, got %v", err)
	}
	if got != "" {
		t.Fatalf("want empty string, got %q", got)
	}
}

func TestTailFile_SmallerThanWindowReturnsAll(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "small.log")
	content := "line one\nline two\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := TailFile(path, 1024)
	if err != nil {
		t.Fatalf("TailFile: %v", err)
	}
	if got != content {
		t.Fatalf("want full content, got %q", got)
	}
}

func TestTaskStatsFor_RollsUpAcrossPhasesAndOrdersByNewest(t *testing.T) {
	now := time.Now()
	entries := []LogEntry{
		{Pool: "investigate", WorkerID: "i1", TaskID: "QUA-1", ModTime: now.Add(-10 * time.Minute)},
		{Pool: "implement", WorkerID: "i2", TaskID: "QUA-1", ModTime: now.Add(-2 * time.Second), Live: true},
		{Pool: "implement", WorkerID: "i3", TaskID: "QUA-2", ModTime: now.Add(-5 * time.Minute)},
		{Pool: "review", WorkerID: "r1", TaskID: "QUA-2", ModTime: now.Add(-30 * time.Minute)},
		// Idle entry with the FRESHEST mtime — would float to the top
		// under a naive sort, but the idle bucket is pinned to the
		// bottom so this acts as the regression guard.
		{Pool: "finalize", WorkerID: "f1", TaskID: "", ModTime: now, Live: true},
	}
	got := TaskStatsFor(entries, true)
	if len(got) != 3 {
		t.Fatalf("want 3 task rows (QUA-1, QUA-2, idle), got %d", len(got))
	}
	if got[0].TaskID != "QUA-1" {
		t.Fatalf("expected newest non-idle task QUA-1 first, got %s", got[0].TaskID)
	}
	if got[0].Live != 1 || got[0].Total != 2 || !got[0].HasLive {
		t.Fatalf("QUA-1 stats wrong: %+v", got[0])
	}
	if got[2].TaskID != IdleTaskBucket {
		t.Fatalf("idle bucket must sink to the bottom even when fresh; got order %+v", got)
	}
}

func TestTaskStatsFor_HideIdleDropsBucket(t *testing.T) {
	now := time.Now()
	entries := []LogEntry{
		{Pool: "implement", WorkerID: "i1", TaskID: "QUA-1", ModTime: now},
		{Pool: "prune", WorkerID: "p1", TaskID: "", ModTime: now},
		{Pool: "prepare-worktree", WorkerID: "p2", TaskID: "", ModTime: now},
	}
	got := TaskStatsFor(entries, false)
	if len(got) != 1 {
		t.Fatalf("want 1 row (idle bucket dropped), got %d: %+v", len(got), got)
	}
	if got[0].TaskID == IdleTaskBucket {
		t.Errorf("idle bucket should be dropped when showIdle=false; got %+v", got[0])
	}
}

func TestPhaseStatsFor_FiltersByTask(t *testing.T) {
	now := time.Now()
	entries := []LogEntry{
		{Pool: "investigate", WorkerID: "i1", TaskID: "QUA-1", ModTime: now.Add(-10 * time.Minute)},
		{Pool: "implement", WorkerID: "i2", TaskID: "QUA-1", ModTime: now.Add(-2 * time.Minute)},
		{Pool: "implement", WorkerID: "i3", TaskID: "QUA-1", ModTime: now.Add(-1 * time.Minute)},
		{Pool: "review", WorkerID: "r1", TaskID: "QUA-2", ModTime: now},
	}
	got := PhaseStatsFor(entries, "QUA-1")
	if len(got) != 2 {
		t.Fatalf("want 2 phase rows under QUA-1, got %d: %+v", len(got), got)
	}
	// implement is newer than investigate, so it should sort first.
	if got[0].Phase != "implement" {
		t.Fatalf("expected newest phase implement first, got %s", got[0].Phase)
	}
	if got[0].Total != 2 {
		t.Fatalf("implement should have 2 runs for QUA-1, got %d", got[0].Total)
	}
	// review belongs to QUA-2 and must not appear.
	for _, p := range got {
		if p.Phase == "review" {
			t.Fatalf("review should not appear for QUA-1: %+v", got)
		}
	}
}

func TestRunsFor_FilteredAndOrderedByNewest(t *testing.T) {
	now := time.Now()
	entries := []LogEntry{
		{Pool: "implement", WorkerID: "i1", TaskID: "QUA-1", ModTime: now.Add(-5 * time.Minute)},
		{Pool: "implement", WorkerID: "i2", TaskID: "QUA-1", ModTime: now.Add(-1 * time.Minute), Live: true},
		{Pool: "implement", WorkerID: "i3", TaskID: "QUA-2", ModTime: now},
		{Pool: "review", WorkerID: "r1", TaskID: "QUA-1", ModTime: now},
	}
	got := RunsFor(entries, "implement", "QUA-1")
	if len(got) != 2 {
		t.Fatalf("want 2 runs for implement/QUA-1, got %d", len(got))
	}
	if got[0].WorkerID != "i2" {
		t.Fatalf("expected newest run i2 first, got %s", got[0].WorkerID)
	}
}

func TestRenderRunList_ShowsClipIndicators(t *testing.T) {
	now := time.Now()
	runs := []LogEntry{
		{Pool: "implement", WorkerID: "w1", ModTime: now.Add(-1 * time.Minute)},
		{Pool: "implement", WorkerID: "w2", ModTime: now.Add(-2 * time.Minute)},
		{Pool: "implement", WorkerID: "w3", ModTime: now.Add(-3 * time.Minute)},
		{Pool: "implement", WorkerID: "w4", ModTime: now.Add(-4 * time.Minute)},
	}
	var b strings.Builder
	// MaxRows=3 means header + 2 rows of budget → one clipped.
	RenderRunList(&b, runs, LogListWindow{Selected: 0, ScrollOffset: 0, MaxRows: 3})
	out := b.String()
	if !strings.Contains(out, "more below") {
		t.Errorf("expected clip indicator at bottom\n---\n%s", out)
	}
}

func TestRowsFromTasks_SortsByTaskID(t *testing.T) {
	tasks := []TaskRow{
		{TaskID: "QUA-3", Chain: []PhaseStep{{Phase: "investigate"}}},
		{TaskID: "QUA-1", Chain: []PhaseStep{{Phase: "investigate"}, {Phase: "implement"}}},
		{TaskID: "QUA-2", Chain: []PhaseStep{{Phase: "finalize"}}},
	}
	got := rowsFromTasks(tasks)
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
	if got[0].TaskID != "QUA-1" || got[1].TaskID != "QUA-2" || got[2].TaskID != "QUA-3" {
		t.Fatalf("rows must be sorted by task id ascending; got %+v", got)
	}
	if len(got[0].Chain) != 2 {
		t.Fatalf("QUA-1 should preserve its chain; got %+v", got[0])
	}
}

func TestExitLogsView_PreservesDrillState(t *testing.T) {
	m := Model{
		opts:    CollectOptions{StateDir: t.TempDir()},
		visible: map[PanelID]bool{PanelOrchestrator: true},
	}
	m.mode = viewModeLogs
	m.logs = logsView{
		entries:      []LogEntry{{Pool: "implement", WorkerID: "w1", TaskID: "QUA-1"}},
		level:        logsLevelRuns,
		selTask:      "QUA-1",
		selPhase:     "implement",
		selected:     3,
		scrollOffset: 1,
		tail:         "old tail content",
	}
	m.exitLogsView()
	if m.mode != viewModePanels {
		t.Fatalf("expected mode to switch to panels, got %v", m.mode)
	}
	// Drill-down state must survive so a subsequent L lands the user
	// exactly where they left off.
	if m.logs.level != logsLevelRuns {
		t.Errorf("level reset: want logsLevelRuns, got %v", m.logs.level)
	}
	if m.logs.selTask != "QUA-1" || m.logs.selPhase != "implement" {
		t.Errorf("breadcrumb reset: %+v", m.logs)
	}
	if m.logs.selected != 3 || m.logs.scrollOffset != 1 {
		t.Errorf("selection/scroll reset: selected=%d scrollOffset=%d", m.logs.selected, m.logs.scrollOffset)
	}
	if m.logs.tail != "" {
		t.Errorf("tail buffer should be cleared on exit (re-read on next refresh); got %q", m.logs.tail)
	}
	// Panel visibility is untouched by exit (sanity check — exit must
	// not stomp on the user's toggled panel set).
	if !m.visible[PanelOrchestrator] {
		t.Errorf("panel visibility lost on exit: %+v", m.visible)
	}
}

func TestRefreshLogs_PreservesLevelAndBreadcrumb(t *testing.T) {
	root := t.TempDir()
	mustWriteLog(t, root, "implement", "w1", "log")
	mustJournalAppend(t, root, "implement", "w1", "QUA-1", journal.OutcomeStarted)

	m := Model{opts: CollectOptions{StateDir: root}}
	m.mode = viewModeLogs
	m.logs.level = logsLevelRuns
	m.logs.selTask = "QUA-1"
	m.logs.selPhase = "implement"
	m.logs.selected = 0

	m.refreshLogs()

	if m.mode != viewModeLogs {
		t.Errorf("refresh changed mode away from logs: %v", m.mode)
	}
	if m.logs.level != logsLevelRuns {
		t.Errorf("refresh reset level: got %v", m.logs.level)
	}
	if m.logs.selTask != "QUA-1" || m.logs.selPhase != "implement" {
		t.Errorf("refresh cleared breadcrumb: %+v", m.logs)
	}
}

func TestClampWindow_BoundsOffset(t *testing.T) {
	cases := []struct {
		total, offset, max int
		wantStart, wantEnd int
	}{
		{total: 10, offset: 0, max: 5, wantStart: 0, wantEnd: 5},
		{total: 10, offset: 3, max: 5, wantStart: 3, wantEnd: 8},
		{total: 10, offset: 100, max: 5, wantStart: 5, wantEnd: 10}, // pinned to end
		{total: 10, offset: -2, max: 5, wantStart: 0, wantEnd: 5},   // pinned to start
		{total: 3, offset: 0, max: 10, wantStart: 0, wantEnd: 3},    // window larger than total
		{total: 3, offset: 0, max: 0, wantStart: 0, wantEnd: 3},     // 0 = unlimited
	}
	for _, c := range cases {
		s, e := clampWindow(c.total, c.offset, c.max)
		if s != c.wantStart || e != c.wantEnd {
			t.Errorf("clampWindow(%d,%d,%d) = (%d,%d), want (%d,%d)",
				c.total, c.offset, c.max, s, e, c.wantStart, c.wantEnd)
		}
	}
}

func mustWriteLog(t *testing.T, root, pool, worker, body string) {
	t.Helper()
	dir := filepath.Join(root, "workers", pool)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, worker+".log"), []byte(body), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
}

func mustBackdate(t *testing.T, path string, ago time.Duration) {
	t.Helper()
	mt := time.Now().Add(-ago)
	if err := os.Chtimes(path, mt, mt); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}

func mustJournalAppend(t *testing.T, root, pool, worker, task string, outcome journal.Outcome) {
	t.Helper()
	mustJournalAppendPhase(t, root, pool, worker, task, pool, outcome)
}

func mustWritePID(t *testing.T, root, pool, worker string, pid int) {
	t.Helper()
	dir := filepath.Join(root, "workers", pool)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, worker+".pid"), []byte(fmt.Sprintf("%d\n", pid)), 0o644); err != nil {
		t.Fatalf("write pid: %v", err)
	}
}

// deadPID returns a PID that's guaranteed not to be alive: spawn a
// short-lived /bin/true, wait for it, then return its pid. By the
// time the test consults it via syscall.Kill(pid, 0), the process is
// gone (reaped). Avoids hard-coding magic numbers that might collide
// on a busy system.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("/bin/true")
	if err := cmd.Run(); err != nil {
		t.Fatalf("/bin/true: %v", err)
	}
	return cmd.Process.Pid
}

// mustJournalAppendPhase is the lower-level variant that lets a test
// specify a phase distinct from the pool — useful for asserting that
// callers ignore sub-events (Phase != Pool).
func mustJournalAppendPhase(t *testing.T, root, pool, worker, task, phase string, outcome journal.Outcome) {
	t.Helper()
	ev := journal.Event{
		TaskID:   task,
		WorkerID: worker,
		Pool:     pool,
		Phase:    phase,
		Outcome:  outcome,
	}
	if err := journal.Append(root, ev); err != nil {
		t.Fatalf("journal.Append: %v", err)
	}
}
