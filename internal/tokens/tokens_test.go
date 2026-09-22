package tokens

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
)

// writeJSONL writes an agent session usage log with a single assistant
// message carrying the given token counts, at the path the ClaudeHome
// index expects: <home>/projects/<anything>/<sessionID>.jsonl.
func writeJSONL(t *testing.T, home, sessionID string, in, out int64) {
	t.Helper()
	dir := filepath.Join(home, "projects", "proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	line := `{"type":"assistant","message":{"usage":{"input_tokens":` +
		strconv.FormatInt(in, 10) + `,"output_tokens":` + strconv.FormatInt(out, 10) + `}}}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, sessionID+".jsonl"), []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Collect attributes token burn to the phase/task recorded on the
// journal's agent-tagged events, joins by session id to the usage log,
// and counts a session that appears on several events exactly once.
func TestCollectAttributesFromJournal(t *testing.T) {
	stateRoot := t.TempDir()
	home := t.TempDir()

	base := time.Now().Add(-time.Hour).UTC()
	appendEvt := func(pool, worker, task, phase, sid string, ts time.Time) {
		ev := journal.Event{
			Timestamp: ts,
			TaskID:    task,
			WorkerID:  worker,
			Pool:      pool,
			Phase:     phase,
			Outcome:   journal.OutcomeStarted,
		}
		if sid != "" {
			ev.Agent = &journal.AgentSession{Runtime: "claude", SessionID: sid}
		}
		if err := journal.Append(stateRoot, ev); err != nil {
			t.Fatal(err)
		}
	}

	// implement session, emitted twice (re-emit) under one task.
	appendEvt("implement", "w1", "QUA-1", "implement", "sess-a", base)
	appendEvt("implement", "w1", "QUA-1", "implement", "sess-a", base.Add(time.Minute))
	// gh-code-review session — no worktree, task id == pool.
	appendEvt("gh-code-review", "w2", "gh-code-review", "gh-code-review", "sess-b", base.Add(2*time.Minute))
	// a terminal event carrying no agent — must not create a phantom session.
	appendEvt("implement", "w1", "QUA-1", "implement", "", base.Add(3*time.Minute))

	writeJSONL(t, home, "sess-a", 100, 20)
	writeJSONL(t, home, "sess-b", 5, 1)

	res, err := Collect(context.Background(), Options{StateRoot: stateRoot, ClaudeHome: home})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Sessions) != 2 {
		t.Fatalf("want 2 sessions (deduped), got %d: %+v", len(res.Sessions), res.Sessions)
	}

	byPhase := map[string]Rollup{}
	for _, r := range res.ByPhase {
		byPhase[r.Key] = r
	}
	impl, ok := byPhase["implement"]
	if !ok {
		t.Fatalf("no implement phase in %+v", res.ByPhase)
	}
	if impl.Sessions != 1 {
		t.Errorf("implement: want 1 session (sess-a counted once), got %d", impl.Sessions)
	}
	if impl.Usage.InputTokens != 100 || impl.Usage.OutputTokens != 20 {
		t.Errorf("implement usage = %+v, want in=100 out=20", impl.Usage)
	}
	if _, ok := byPhase["gh-code-review"]; !ok {
		t.Errorf("gh-code-review phase missing — worktree-less worker not attributed: %+v", res.ByPhase)
	}
}

// TaskID filters to a single task's sessions.
func TestCollectTaskIDFilter(t *testing.T) {
	stateRoot := t.TempDir()
	home := t.TempDir()
	ts := time.Now().Add(-time.Hour).UTC()

	for _, s := range []struct{ task, sid string }{{"QUA-1", "sess-a"}, {"gh-code-review", "sess-b"}} {
		ev := journal.Event{
			Timestamp: ts, TaskID: s.task, WorkerID: "w", Pool: "implement",
			Phase: "implement", Outcome: journal.OutcomeStarted,
			Agent: &journal.AgentSession{Runtime: "claude", SessionID: s.sid},
		}
		if err := journal.Append(stateRoot, ev); err != nil {
			t.Fatal(err)
		}
	}
	writeJSONL(t, home, "sess-a", 100, 20)
	writeJSONL(t, home, "sess-b", 5, 1)

	res, err := Collect(context.Background(), Options{StateRoot: stateRoot, ClaudeHome: home, TaskID: "QUA-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Sessions) != 1 || res.Sessions[0].SessionID != "sess-a" {
		t.Fatalf("TaskID filter failed: %+v", res.Sessions)
	}
}

// Cost and usage come from what the worker reported on its journal
// events: several runs of one session sum, no ClaudeHome is required,
// and CostAvailable flips on as soon as any session carries a cost.
func TestCollectReportedCostAndUsage(t *testing.T) {
	stateRoot := t.TempDir()
	ts := time.Now().Add(-time.Hour).UTC()
	emit := func(sid string, cost float64, usage *journal.Usage, offset time.Duration) {
		ev := journal.Event{
			Timestamp: ts.Add(offset), TaskID: "QUA-9", WorkerID: "w", Pool: "implement",
			Phase: "implement", Outcome: journal.OutcomeStarted,
			Agent: &journal.AgentSession{Runtime: "claude", SessionID: sid, CostUSD: cost, Usage: usage},
		}
		if err := journal.Append(stateRoot, ev); err != nil {
			t.Fatal(err)
		}
	}
	emit("sess-a", 0, nil, 0)                                                                 // pre-run breadcrumb
	emit("sess-a", 1.25, &journal.Usage{InputTokens: 10, OutputTokens: 100}, time.Minute)     // first run
	emit("sess-a", 0.75, &journal.Usage{InputTokens: 5, CacheReadTokens: 900}, 2*time.Minute) // resumed run

	res, err := Collect(context.Background(), Options{StateRoot: stateRoot})
	if err != nil {
		t.Fatal(err)
	}
	if !res.CostAvailable {
		t.Fatal("CostAvailable should be true once a session reports cost")
	}
	if len(res.Sessions) != 1 {
		t.Fatalf("want 1 session, got %+v", res.Sessions)
	}
	s := res.Sessions[0]
	if s.Cost != 2.0 || !s.Reported {
		t.Errorf("cost = %v reported=%v, want 2.0 true", s.Cost, s.Reported)
	}
	if s.Usage != (Usage{InputTokens: 15, OutputTokens: 100, CacheReadTokens: 900}) {
		t.Errorf("usage = %+v, want summed runs", s.Usage)
	}
	if res.ByPhase[0].Cost != 2.0 {
		t.Errorf("phase rollup cost = %v, want 2.0", res.ByPhase[0].Cost)
	}
}

func TestCollectByPhaseModel(t *testing.T) {
	stateRoot := t.TempDir()
	ts := time.Now().Add(-time.Hour).UTC()
	emit := func(phase, sid, model string, usage *journal.Usage, offset time.Duration) {
		ev := journal.Event{
			Timestamp: ts.Add(offset), TaskID: "QUA-1", WorkerID: "w", Pool: phase,
			Phase: phase, Outcome: journal.OutcomeStarted,
			Agent: &journal.AgentSession{Runtime: "claude", Model: model, SessionID: sid, Usage: usage},
		}
		if err := journal.Append(stateRoot, ev); err != nil {
			t.Fatal(err)
		}
	}
	// finalize spans two models (a before/after downgrade); implement has one;
	// one untagged session must still show under its phase as "(unknown)". A
	// post-run report with an empty model must not wipe the breadcrumb's model.
	emit("finalize", "f1", "claude-opus-4-8", &journal.Usage{InputTokens: 100}, 0)
	emit("finalize", "f1", "", &journal.Usage{OutputTokens: 50}, time.Minute)
	emit("finalize", "f2", "claude-sonnet-5", &journal.Usage{InputTokens: 60}, 2*time.Minute)
	emit("implement", "i1", "claude-opus-4-8", &journal.Usage{InputTokens: 200}, 0)
	emit("implement", "i2", "", &journal.Usage{InputTokens: 5}, time.Minute)

	res, err := Collect(context.Background(), Options{StateRoot: stateRoot})
	if err != nil {
		t.Fatal(err)
	}

	fin := map[string]Rollup{}
	for _, r := range res.ByPhaseModel["finalize"] {
		fin[r.Key] = r
	}
	if len(fin) != 2 {
		t.Fatalf("finalize should split into 2 models, got %+v", res.ByPhaseModel["finalize"])
	}
	if fin["claude-opus-4-8"].Sessions != 1 || fin["claude-sonnet-5"].Sessions != 1 {
		t.Errorf("finalize model split wrong: %+v", fin)
	}
	// f1 keeps opus despite the later empty-model report.
	if got := fin["claude-opus-4-8"].Usage; got.InputTokens != 100 || got.OutputTokens != 50 {
		t.Errorf("f1 opus usage = %+v, want input 100 / output 50 (empty report must not reassign model)", got)
	}
	impl := map[string]Rollup{}
	for _, r := range res.ByPhaseModel["implement"] {
		impl[r.Key] = r
	}
	if _, ok := impl["(unknown)"]; !ok {
		t.Errorf("untagged implement session must show as (unknown): %+v", res.ByPhaseModel["implement"])
	}
}

// A session that never reported falls back to its jsonl for tokens only,
// counting each API message once even though the log repeats the same
// usage on every content-block line of a response.
func TestSumSessionTokensDedupesByMessageID(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "projects", "proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	lines := `{"type":"assistant","message":{"id":"m1","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":100}}}
{"type":"assistant","message":{"id":"m1","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":100}}}
{"type":"assistant","message":{"id":"m1","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":100}}}
{"type":"user","message":{}}
{"type":"assistant","message":{"id":"m2","usage":{"input_tokens":1,"output_tokens":2,"cache_read_input_tokens":200}}}
`
	if err := os.WriteFile(filepath.Join(dir, "sess-x.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	stateRoot := t.TempDir()
	if err := journal.Append(stateRoot, journal.Event{
		Timestamp: time.Now().Add(-time.Minute), TaskID: "t", WorkerID: "w", Pool: "p", Phase: "p",
		Outcome: journal.OutcomeStarted, Agent: &journal.AgentSession{Runtime: "claude", SessionID: "sess-x"},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := Collect(context.Background(), Options{StateRoot: stateRoot, ClaudeHome: home})
	if err != nil {
		t.Fatal(err)
	}
	if res.CostAvailable {
		t.Error("fallback sessions carry no cost; CostAvailable must stay false")
	}
	got := res.Sessions[0].Usage
	if got != (Usage{InputTokens: 11, OutputTokens: 7, CacheReadTokens: 300}) {
		t.Errorf("usage = %+v, want deduped totals in=11 out=7 cache_r=300", got)
	}
}
