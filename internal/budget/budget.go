// Package budget evaluates rolling spend caps against the cost workers
// report on their journal events. The scheduler uses it to decide
// whether a pipeline may launch; the monitor uses it to show where each
// pipeline stands. Both read the same ledger shape so they never
// disagree.
package budget

import (
	"sort"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
	"github.com/grasskode/brahmanda/internal/pipelinespec"
)

// Entry is one reported spend: a worker's agent run that cost USD at At.
type Entry struct {
	At  time.Time
	USD float64
}

// Ledger is an in-memory list of spend entries. Not safe for concurrent
// use; the scheduler guards it with its own mutex.
type Ledger struct {
	entries []Entry
}

// Status is a cap evaluated at one instant. Spent is the sum inside the
// trailing window; Exceeded means launches must hold; ResetAt is the
// earliest instant at which enough spend will have aged out of the
// window for Spent to drop below the cap (zero when not exceeded).
type Status struct {
	Spent    float64
	Exceeded bool
	ResetAt  time.Time
}

// Add records one spend. Zero and negative amounts are ignored.
func (l *Ledger) Add(at time.Time, usd float64) {
	if usd <= 0 {
		return
	}
	l.entries = append(l.entries, Entry{At: at, USD: usd})
}

// AddEvents records the cost carried by every agent-tagged event in
// events, filtered to pool when non-empty.
func (l *Ledger) AddEvents(events []journal.Event, pool string) {
	for _, e := range events {
		if e.Agent == nil || e.Agent.CostUSD <= 0 {
			continue
		}
		if pool != "" && e.Pool != pool {
			continue
		}
		l.Add(e.Timestamp, e.Agent.CostUSD)
	}
}

// Prune drops entries older than before so a long-running ledger stays
// bounded. Entries at exactly before are kept.
func (l *Ledger) Prune(before time.Time) {
	kept := l.entries[:0]
	for _, e := range l.entries {
		if !e.At.Before(before) {
			kept = append(kept, e)
		}
	}
	l.entries = kept
}

// Len reports how many entries the ledger holds.
func (l *Ledger) Len() int { return len(l.entries) }

// Status evaluates cap b at now. A zero cap is never exceeded.
func (l *Ledger) Status(b pipelinespec.Budget, now time.Time) Status {
	if b.IsZero() {
		return Status{}
	}
	since := now.Add(-b.Window)
	var window []Entry
	var spent float64
	for _, e := range l.entries {
		if e.At.Before(since) || e.At.After(now) {
			continue
		}
		window = append(window, e)
		spent += e.USD
	}
	st := Status{Spent: spent}
	if spent < b.Amount {
		return st
	}
	st.Exceeded = true
	// Age entries out oldest-first until what remains fits under the
	// cap; the last one dropped fixes the reset instant.
	sort.Slice(window, func(i, j int) bool { return window[i].At.Before(window[j].At) })
	remaining := spent
	for _, e := range window {
		remaining -= e.USD
		st.ResetAt = e.At.Add(b.Window)
		if remaining < b.Amount {
			break
		}
	}
	return st
}
