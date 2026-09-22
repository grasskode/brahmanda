// Package tokens attributes agent spend — dollars and tokens — to
// orchestrator phases and tasks.
//
// The journal is the source of truth: a worker that drives an agent
// session emits the runtime's own accounting (claude's result envelope
// carries total_cost_usd and usage) on an agent-tagged event once the
// run returns, and every such event is attributed to the phase and task
// it was journaled under. No pricing table lives here — the runtime
// prices its own runs.
//
// A session whose worker never got to report (killed on step_timeout,
// orchestrator restart) has no cost, but its tokens can still be
// recovered from the Claude CLI's session jsonl when ClaudeHome is set.
// That fallback is tokens-only; such a session's cost reads as zero and
// renders as "—".
package tokens

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
)

// Usage is the per-session token totals. It is the journal's wire type
// so workers, the scheduler's budget ledger and this package agree on
// one shape.
type Usage = journal.Usage

// Session is one row of attributed spend.
type Session struct {
	SessionID string
	Runtime   string    // agent runtime that ran the session, e.g. "claude"
	Model     string    // model the session ran (from the journal); empty if unknown
	Phase     string    // phase from the journal: investigate, implement, ...
	TaskID    string    // identifier the session ran under (e.g. CHG-42 / user--CHG-42)
	StartedAt time.Time // timestamp of the earliest journal event for this session
	Usage     Usage
	Cost      float64 // USD summed over every run the worker reported for this session
	Reported  bool    // true when at least one journal event carried cost or usage for it
}

// Rollup is one row of an aggregate view. Same shape for the
// by-phase and by-task slices — they only differ in what Key means.
type Rollup struct {
	Key      string // phase name (investigate/...) or task id (QUA-345/...)
	Sessions int
	Usage    Usage
	Cost     float64
}

// Options controls what Collect reads.
type Options struct {
	StateRoot  string    // orchestrator state dir; its journal is the session source
	ClaudeHome string    // optional: ~/.claude, for the tokens-only jsonl fallback
	Since      time.Time // journal events before this are dropped
	TaskID     string    // when non-empty, only sessions with this TaskID are included
}

// Result bundles the three views callers typically want at once.
// ByPhase groups across all (matching) tasks; ByTask groups across all
// phases; Sessions is the raw join for callers that want full control.
type Result struct {
	Sessions []Session // per-session detail, sorted oldest-first
	ByPhase  []Rollup  // grouped + sorted by total token burn desc
	ByTask   []Rollup  // grouped + sorted by total token burn desc
	// ByPhaseModel breaks each phase down by the model its sessions ran,
	// so burn can be attributed to a model within the pipeline that spent
	// it (a phase that changed models shows one row per model). Keyed by
	// phase; each slice sorted by burn desc, "(unknown)" for untagged.
	ByPhaseModel  map[string][]Rollup
	CostAvailable bool // true when at least one session reported its cost
}

// Collect reads the orchestrator journal for agent-tagged events, sums
// the cost and usage workers reported per session, and fills in tokens
// from the session jsonl for sessions that reported nothing. It is safe
// to call repeatedly; nothing is cached between invocations.
func Collect(ctx context.Context, opts Options) (Result, error) {
	_ = ctx // kept for API stability; collection is local file I/O
	events, err := journal.Read(opts.StateRoot, opts.Since)
	if err != nil {
		return Result{}, err
	}

	// One session id can appear on several journal events: the pre-run
	// breadcrumb, the post-run report, one report per resumed run. The
	// earliest event fixes the attribution; cost and usage sum across
	// all of them since each report covers one invocation.
	byID := map[string]*Session{}
	for _, e := range events {
		if e.Agent == nil || e.Agent.SessionID == "" {
			continue
		}
		if opts.TaskID != "" && e.TaskID != opts.TaskID {
			continue
		}
		s, ok := byID[e.Agent.SessionID]
		if !ok {
			s = &Session{SessionID: e.Agent.SessionID, StartedAt: e.Timestamp}
			byID[e.Agent.SessionID] = s
		}
		if !e.Timestamp.After(s.StartedAt) || s.Phase == "" {
			s.Runtime, s.Phase, s.TaskID, s.StartedAt = e.Agent.Runtime, e.Phase, e.TaskID, e.Timestamp
		}
		// Model rides the pre-run breadcrumb; a later post-run report may
		// omit it, so only overwrite from an event that actually carries one.
		if e.Agent.Model != "" {
			s.Model = e.Agent.Model
		}
		if e.Agent.CostUSD > 0 || (e.Agent.Usage != nil && !e.Agent.Usage.IsZero()) {
			s.Reported = true
			s.Cost += e.Agent.CostUSD
			if e.Agent.Usage != nil {
				s.Usage.Add(*e.Agent.Usage)
			}
		}
	}

	var out Result
	var jsonlIndex map[string]string
	for _, s := range byID {
		if s.Reported {
			out.CostAvailable = out.CostAvailable || s.Cost > 0
		} else if opts.ClaudeHome != "" {
			if jsonlIndex == nil {
				jsonlIndex = indexClaudeProjects(opts.ClaudeHome)
			}
			if path, ok := jsonlIndex[s.SessionID]; ok {
				if u, err := sumSessionTokens(path); err == nil {
					s.Usage = u
				}
			}
		}
		out.Sessions = append(out.Sessions, *s)
	}
	sort.Slice(out.Sessions, func(i, j int) bool {
		if !out.Sessions[i].StartedAt.Equal(out.Sessions[j].StartedAt) {
			return out.Sessions[i].StartedAt.Before(out.Sessions[j].StartedAt)
		}
		return out.Sessions[i].SessionID < out.Sessions[j].SessionID
	})

	out.ByPhase = rollupBy(out.Sessions, func(s Session) string { return s.Phase })
	out.ByTask = rollupBy(out.Sessions, func(s Session) string { return s.TaskID })

	// Per-phase model breakdown: group each phase's sessions by model.
	byPhase := map[string][]Session{}
	for _, s := range out.Sessions {
		byPhase[s.Phase] = append(byPhase[s.Phase], s)
	}
	out.ByPhaseModel = make(map[string][]Rollup, len(byPhase))
	for phase, ss := range byPhase {
		out.ByPhaseModel[phase] = rollupBy(ss, modelKey)
	}
	return out, nil
}

// modelKey buckets a session by the model it ran, folding untagged
// sessions into "(unknown)" rather than dropping them (an empty key
// would be skipped by rollupBy).
func modelKey(s Session) string {
	if s.Model == "" {
		return "(unknown)"
	}
	return s.Model
}

// rollupBy groups sessions by the key returned by keyFn and sorts the
// result by total token burn descending, with the key as a stable
// tiebreaker.
func rollupBy(sessions []Session, keyFn func(Session) string) []Rollup {
	idx := map[string]*Rollup{}
	for _, s := range sessions {
		k := keyFn(s)
		if k == "" {
			continue
		}
		r, ok := idx[k]
		if !ok {
			r = &Rollup{Key: k}
			idx[k] = r
		}
		r.Sessions++
		r.Usage.Add(s.Usage)
		r.Cost += s.Cost
	}
	out := make([]Rollup, 0, len(idx))
	for _, r := range idx {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool {
		ai, bi := out[i].Usage.Total(), out[j].Usage.Total()
		if ai != bi {
			return ai > bi
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// indexClaudeProjects builds session_id → jsonl path by walking
// ~/.claude/projects/*/*.jsonl. Basename minus .jsonl is the id. A
// missing or unreadable tree yields an empty index.
func indexClaudeProjects(claudeHome string) map[string]string {
	root := filepath.Join(claudeHome, "projects")
	idx := map[string]string{}
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".jsonl") {
			return nil
		}
		id := strings.TrimSuffix(filepath.Base(p), ".jsonl")
		idx[id] = p
		return nil
	})
	return idx
}

// jsonlMessage is the per-line shape we care about in a session jsonl.
// Claude emits a lot more — we only read message.id and message.usage
// from assistant messages.
type jsonlMessage struct {
	Type    string `json:"type"`
	Message struct {
		ID    string `json:"id"`
		Usage struct {
			InputTokens              int64 `json:"input_tokens"`
			OutputTokens             int64 `json:"output_tokens"`
			CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
			CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

// sumSessionTokens parses one session jsonl line-by-line and sums the
// usage on assistant messages. One API response is written as several
// lines — one per content block (thinking, tool_use, text) — all
// carrying the same message id and usage, so each id is counted once.
// Streaming so a multi-MB jsonl doesn't balloon memory.
func sumSessionTokens(path string) (Usage, error) {
	f, err := os.Open(path)
	if err != nil {
		return Usage{}, err
	}
	defer f.Close()
	var u Usage
	seen := map[string]bool{}
	dec := json.NewDecoder(f)
	for {
		var m jsonlMessage
		if err := dec.Decode(&m); err != nil {
			break
		}
		if m.Type != "assistant" {
			continue
		}
		if m.Message.ID != "" {
			if seen[m.Message.ID] {
				continue
			}
			seen[m.Message.ID] = true
		}
		u.InputTokens += m.Message.Usage.InputTokens
		u.OutputTokens += m.Message.Usage.OutputTokens
		u.CacheCreationTokens += m.Message.Usage.CacheCreationInputTokens
		u.CacheReadTokens += m.Message.Usage.CacheReadInputTokens
	}
	return u, nil
}
