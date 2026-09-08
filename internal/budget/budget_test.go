package budget

import (
	"testing"
	"time"

	"github.com/grasskode/brahmanda/internal/journal"
	"github.com/grasskode/brahmanda/internal/pipelinespec"
)

func hourly(amount float64) pipelinespec.Budget {
	return pipelinespec.Budget{Amount: amount, Window: time.Hour}
}

func daily(amount float64) pipelinespec.Budget {
	return pipelinespec.Budget{Amount: amount, Window: 24 * time.Hour}
}

// A daily cap counts only spend since local midnight — not a rolling
// trailing 24h — and resets at the next local midnight.
func TestStatusDailyResetsAtMidnight(t *testing.T) {
	loc := time.Local
	base := time.Now().In(loc)
	y, m, d := base.Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, loc)
	noon := time.Date(y, m, d, 12, 0, 0, 0, loc)

	var l Ledger
	l.Add(midnight.Add(-2*time.Hour), 30) // yesterday 22:00 — before the window
	l.Add(midnight.Add(3*time.Hour), 20)  // today 03:00
	l.Add(noon.Add(-time.Hour), 25)       // today 11:00

	st := l.Status(daily(40), noon)
	if st.Spent != 45 {
		t.Fatalf("daily spent = %v, want 45 (yesterday's 30 excluded)", st.Spent)
	}
	if !st.Exceeded {
		t.Fatalf("45 over a 40USD/d cap must be exceeded: %+v", st)
	}
	if want := midnight.AddDate(0, 0, 1); !st.ResetAt.Equal(want) {
		t.Fatalf("ResetAt = %s, want next midnight %s", st.ResetAt, want)
	}

	// Just after midnight the day's spend has cleared.
	after := l.Status(daily(40), st.ResetAt.Add(time.Second))
	if after.Exceeded || after.Spent != 0 {
		t.Fatalf("new day must start clear: %+v", after)
	}
}

// Spend inside the window counts; spend that has aged out does not.
func TestStatusWindowsSpend(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	var l Ledger
	l.Add(now.Add(-2*time.Hour), 5) // aged out
	l.Add(now.Add(-30*time.Minute), 0.4)
	l.Add(now.Add(-10*time.Minute), 0.5)

	st := l.Status(hourly(1), now)
	if st.Exceeded {
		t.Fatalf("0.9 spent under a 1USD/h cap must not be exceeded: %+v", st)
	}
	if st.Spent != 0.9 {
		t.Fatalf("spent = %v, want 0.9", st.Spent)
	}
}

// Once the cap is reached, ResetAt is the instant the oldest entries that
// need to age out do so — not merely the oldest entry plus the window.
func TestStatusResetAt(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	var l Ledger
	l.Add(now.Add(-50*time.Minute), 0.3)
	l.Add(now.Add(-40*time.Minute), 0.3)
	l.Add(now.Add(-5*time.Minute), 0.5)

	st := l.Status(hourly(1), now)
	if !st.Exceeded {
		t.Fatalf("1.1 spent must exceed 1USD/h: %+v", st)
	}
	// Dropping the -50m entry leaves 0.8 < 1, so the cap lifts when that
	// entry leaves the window: -50m + 1h = +10m.
	if want := now.Add(10 * time.Minute); !st.ResetAt.Equal(want) {
		t.Fatalf("ResetAt = %s, want %s", st.ResetAt, want)
	}

	// Re-evaluating just after the reset instant clears the hold.
	after := l.Status(hourly(1), st.ResetAt.Add(time.Second))
	if after.Exceeded {
		t.Fatalf("still exceeded after reset instant: %+v", after)
	}
}

// Exactly reaching the cap counts as exceeded; a zero cap never does.
func TestStatusBoundaryAndZeroCap(t *testing.T) {
	now := time.Now()
	var l Ledger
	l.Add(now.Add(-time.Minute), 1)
	if !l.Status(hourly(1), now).Exceeded {
		t.Fatal("spend equal to the cap must hold launches")
	}
	if l.Status(pipelinespec.Budget{}, now).Exceeded {
		t.Fatal("zero cap must never be exceeded")
	}
}

// AddEvents takes only agent-tagged events carrying a cost, filtered by
// pool when one is given; Prune drops what has aged out.
func TestAddEventsAndPrune(t *testing.T) {
	now := time.Now()
	events := []journal.Event{
		{Timestamp: now.Add(-3 * time.Hour), Pool: "a", Agent: &journal.AgentSession{CostUSD: 2}},
		{Timestamp: now.Add(-time.Minute), Pool: "a", Agent: &journal.AgentSession{CostUSD: 0.5}},
		{Timestamp: now.Add(-time.Minute), Pool: "b", Agent: &journal.AgentSession{CostUSD: 0.7}},
		{Timestamp: now.Add(-time.Minute), Pool: "a"},                                               // no agent → ignored
		{Timestamp: now.Add(-time.Minute), Pool: "a", Agent: &journal.AgentSession{SessionID: "x"}}, // breadcrumb, no cost
	}
	var a Ledger
	a.AddEvents(events, "a")
	if a.Len() != 2 {
		t.Fatalf("pool a: %d entries, want 2", a.Len())
	}
	var all Ledger
	all.AddEvents(events, "")
	if all.Len() != 3 {
		t.Fatalf("all pools: %d entries, want 3", all.Len())
	}
	all.Prune(now.Add(-time.Hour))
	if all.Len() != 2 {
		t.Fatalf("after prune: %d entries, want 2", all.Len())
	}
}
