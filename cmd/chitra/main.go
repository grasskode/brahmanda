// Command chitra is the monitor — the record-keeper that reads what
// every worker did and reports it. One-shot text output by default; add
// --watch <interval> for the bubbletea TUI.
//
// Operator-facing UX: nothing has to be passed. chitra reads
// <state_dir>/runtime.yaml (written by brahma on startup) to
// auto-discover worktrees root + claude home + per-pipeline ticks.
// Flags are present only as overrides.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/grasskode/brahmanda/internal/agentui"
)

func main() {
	stateRoot := flag.String("state-dir", defaultStateDir(), "brahma state dir (defaults to $XDG_STATE_HOME/brahma)")
	worktreesRoot := flag.String("worktrees-root", "", "override worktrees root (default: from <state_dir>/runtime.yaml or $WORKTREES_ROOT)")
	since := flag.String("since", "today", "lookback: 'today' (local midnight), 'all' (no cutoff), or a duration like '6h' / '7d'")
	workerTimeout := flag.Duration("worker-timeout", 1*time.Hour, "lock files older than this are flagged as stale")
	watch := flag.Duration("watch", 0, "if > 0, run as a TUI refreshing at this interval (e.g. 5s); 0 = one-shot text mode")
	flag.Parse()

	cutoff, sinceDur, sinceLabel, err := agentui.ResolveSince(*since, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, "chitra: --since:", err)
		os.Exit(2)
	}

	opts := agentui.CollectOptions{
		StateDir:      *stateRoot,
		WorktreesRoot: *worktreesRoot,
		Since:         sinceDur,
		SinceCutoff:   cutoff,
		SinceLabel:    sinceLabel,
		SinceSpec:     *since,
		StaleAfter:    *workerTimeout,
	}
	if *watch <= 0 {
		agentui.Render(os.Stdout, agentui.Collect(context.Background(), opts))
		return
	}
	p := tea.NewProgram(agentui.NewModel(opts, *watch), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "chitra: %v\n", err)
		os.Exit(1)
	}
}

func defaultStateDir() string {
	dir := os.Getenv("XDG_STATE_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "."
		}
		dir = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(dir, "brahma")
}
