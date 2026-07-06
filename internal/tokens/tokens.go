// Package tokens attributes Claude API token burn to orchestrator
// phases. It joins our own sessions.yaml records (which know which
// phase each session belonged to) with the Claude CLI's session
// jsonls (which carry the per-message token usage) to produce a
// per-phase breakdown.
//
// Cost is optional and comes from ccusage (https://github.com/ryoppippi/ccusage)
// when it's on PATH; otherwise the Cost column reads as zero and
// callers display "—". We deliberately don't ship our own pricing
// table — Anthropic updates rates and cache discounts often enough
// that a stale table is worse than no number.
package tokens

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Usage is the per-session token totals, summed across every message
// in the session's jsonl. Cache fields are kept separate from the
// other two so callers can show them — cache reads are an order of
// magnitude cheaper than input tokens and reading them as plain input
// would misrepresent burn.
type Usage struct {
	InputTokens         int64
	OutputTokens        int64
	CacheCreationTokens int64
	CacheReadTokens     int64
}

// Add accumulates other into u.
func (u *Usage) Add(other Usage) {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.CacheCreationTokens += other.CacheCreationTokens
	u.CacheReadTokens += other.CacheReadTokens
}

// Session is one row of attributed usage.
type Session struct {
	SessionID string
	Phase     string    // step name from sessions.yaml: investigate, implement, ...
	TaskID    string    // identifier the session ran under (e.g. CHG-42 / user--CHG-42)
	StartedAt time.Time // timestamp recorded in sessions.yaml
	Usage     Usage
	Cost      float64 // USD, 0 when ccusage unavailable or session unknown to ccusage
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
	WorktreesRoot string    // <root>/*/.agent/sessions.yaml — live worktrees
	ArchiveDir    string    // <archive>/*/*/sessions.yaml — finished workers
	ClaudeHome    string    // ~/.claude/projects/<encoded-cwd>/<session>.jsonl
	Since         time.Time // sessions with StartedAt before this are dropped
	TaskID        string    // when non-empty, only sessions with this TaskID are included
}

// Result bundles the three views callers typically want at once.
// ByPhase groups across all (matching) tasks; ByTask groups across all
// phases; Sessions is the raw join for callers that want full control.
type Result struct {
	Sessions       []Session // per-session detail, sorted oldest-first
	ByPhase        []Rollup  // grouped + sorted by total token burn desc
	ByTask         []Rollup  // grouped + sorted by total token burn desc
	CcusageEnabled bool      // true when ccusage produced a non-empty cost map
}

// Collect walks sessions.yaml files, joins with session jsonls for
// token totals, and (optionally) overlays ccusage's cost data. It is
// safe to call repeatedly; nothing is cached between invocations.
func Collect(ctx context.Context, opts Options) (Result, error) {
	sessionsYAMLs, err := findSessionsYAMLs(opts)
	if err != nil {
		return Result{}, err
	}
	entries, err := loadSessionEntries(sessionsYAMLs, opts.Since)
	if err != nil {
		return Result{}, err
	}

	jsonlIndex, err := indexClaudeProjects(opts.ClaudeHome)
	if err != nil {
		return Result{}, err
	}

	costs, ccusageOK := loadCcusageCosts(ctx)

	out := Result{CcusageEnabled: ccusageOK}
	for _, e := range entries {
		if opts.TaskID != "" && e.TaskID != opts.TaskID {
			continue
		}
		s := Session{
			SessionID: e.SessionID,
			Phase:     e.Step,
			TaskID:    e.TaskID,
			StartedAt: e.Timestamp,
			Cost:      costs[e.SessionID],
		}
		if path, ok := jsonlIndex[e.SessionID]; ok {
			u, err := sumSessionTokens(path)
			if err == nil {
				s.Usage = u
			}
		}
		out.Sessions = append(out.Sessions, s)
	}
	sort.Slice(out.Sessions, func(i, j int) bool {
		return out.Sessions[i].StartedAt.Before(out.Sessions[j].StartedAt)
	})

	out.ByPhase = rollupBy(out.Sessions, func(s Session) string { return s.Phase })
	out.ByTask = rollupBy(out.Sessions, func(s Session) string { return s.TaskID })
	return out, nil
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
		ai, bi := totalTokens(out[i].Usage), totalTokens(out[j].Usage)
		if ai != bi {
			return ai > bi
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func totalTokens(u Usage) int64 {
	return u.InputTokens + u.OutputTokens + u.CacheCreationTokens + u.CacheReadTokens
}

// findSessionsYAMLs locates every sessions.yaml under the live
// worktrees root and the archive dir. We treat both identically —
// finished workers in archive are just as relevant to "tokens burnt
// in the last N hours" as live ones.
func findSessionsYAMLs(opts Options) ([]string, error) {
	var paths []string
	for _, root := range []string{opts.WorktreesRoot, opts.ArchiveDir} {
		if root == "" {
			continue
		}
		_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				return nil
			}
			if filepath.Base(p) == "sessions.yaml" {
				paths = append(paths, p)
			}
			return nil
		})
	}
	return paths, nil
}

// sessionEntry mirrors the YAML shape pipeline.appendSession writes.
type sessionEntry struct {
	Step      string    `yaml:"step"`
	Timestamp time.Time `yaml:"timestamp"`
	SessionID string    `yaml:"session_id"`
	TaskID    string    `yaml:"-"` // derived from path, not stored
}

// loadSessionEntries reads every sessions.yaml and merges them into one
// flat list keyed by session_id. sessions.yaml is append-only inside a
// worktree, and each archive captures a snapshot — so the same session
// id appears in every archive recorded after that session ran. Without
// dedup, a session that produced N archive snapshots would be counted
// N times, inflating both token totals and ccusage-derived cost. Keep
// the earliest occurrence (oldest archive that has it) so the
// StartedAt timestamp is the original, not the snapshot capture time.
func loadSessionEntries(paths []string, since time.Time) ([]sessionEntry, error) {
	seen := make(map[string]sessionEntry)
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var entries []sessionEntry
		if err := yaml.Unmarshal(raw, &entries); err != nil {
			continue
		}
		taskID := taskIDFromSessionsPath(p)
		for _, e := range entries {
			if e.SessionID == "" {
				continue
			}
			if !since.IsZero() && e.Timestamp.Before(since) {
				continue
			}
			existing, ok := seen[e.SessionID]
			if ok && !e.Timestamp.Before(existing.Timestamp) {
				continue // keep the earliest
			}
			e.TaskID = taskID
			seen[e.SessionID] = e
		}
	}
	out := make([]sessionEntry, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	return out, nil
}

// taskIDFromSessionsPath recovers the task / worktree identifier from
// either layout. Live: <worktrees_root>/<id>/.agent/sessions.yaml →
// returns <id>. Archive: <archive>/<id>/<ts>/sessions.yaml → also
// returns <id>. We don't care which, just that it's the same key
// downstream consumers (PR list, chitra) recognise.
func taskIDFromSessionsPath(p string) string {
	dir := filepath.Dir(p)
	base := filepath.Base(dir)
	if base == ".agent" {
		return filepath.Base(filepath.Dir(dir))
	}
	// archive layout: parent of <ts> is the id
	return filepath.Base(filepath.Dir(dir))
}

// indexClaudeProjects builds session_id → jsonl path by walking
// ~/.claude/projects/*/*.jsonl. Basename minus .jsonl is the id.
func indexClaudeProjects(claudeHome string) (map[string]string, error) {
	if claudeHome == "" {
		return nil, fmt.Errorf("tokens: ClaudeHome is empty")
	}
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
	return idx, nil
}

// jsonlMessage is the per-line shape we care about in a session jsonl.
// Claude emits a lot more — we only read message.usage from assistant
// messages.
type jsonlMessage struct {
	Type    string `json:"type"`
	Message struct {
		Usage struct {
			InputTokens              int64 `json:"input_tokens"`
			OutputTokens             int64 `json:"output_tokens"`
			CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
			CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

// sumSessionTokens parses one session jsonl line-by-line and sums the
// usage on every assistant message. Streaming so a multi-MB jsonl
// doesn't balloon memory.
func sumSessionTokens(path string) (Usage, error) {
	f, err := os.Open(path)
	if err != nil {
		return Usage{}, err
	}
	defer f.Close()
	var u Usage
	dec := json.NewDecoder(f)
	for {
		var m jsonlMessage
		if err := dec.Decode(&m); err != nil {
			break
		}
		if m.Type != "assistant" {
			continue
		}
		u.InputTokens += m.Message.Usage.InputTokens
		u.OutputTokens += m.Message.Usage.OutputTokens
		u.CacheCreationTokens += m.Message.Usage.CacheCreationInputTokens
		u.CacheReadTokens += m.Message.Usage.CacheReadInputTokens
	}
	return u, nil
}

// loadCcusageCosts calls `ccusage session --json` if the binary is on
// PATH and returns session_id → USD cost. Best-effort: missing binary,
// non-zero exit, or unrecognised JSON all return an empty map and
// false. The caller falls back to "—" in the cost column.
func loadCcusageCosts(ctx context.Context) (map[string]float64, bool) {
	bin, err := exec.LookPath("ccusage")
	if err != nil {
		return nil, false
	}
	cmd := exec.CommandContext(ctx, bin, "session", "--json")
	raw, err := cmd.Output()
	if err != nil {
		return nil, false
	}
	// ccusage's output shape (as of v0.x):
	//   {"session": [{"period": "<uuid>", "totalCost": 3.42, ...}, ...]}
	// `period` is the Claude session id; key is singular `session`.
	var envelope struct {
		Session []struct {
			Period    string  `json:"period"`
			TotalCost float64 `json:"totalCost"`
		} `json:"session"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, false
	}
	if len(envelope.Session) == 0 {
		return nil, false
	}
	out := make(map[string]float64, len(envelope.Session))
	for _, s := range envelope.Session {
		if s.Period == "" {
			continue
		}
		out[s.Period] = s.TotalCost
	}
	return out, true
}
