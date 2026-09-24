package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// formatQueryDump is what `y` writes to the clipboard. Lock the format so a
// drive-by render change doesn't silently break what users paste into tickets.
func TestFormatQueryDump(t *testing.T) {
	jobStart := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	q := cratedb.ActiveQuery{
		ID:        "abc-123",
		Node:      "data-0",
		Started:   jobStart,
		Stmt:      "SELECT * FROM big_table",
		Username:  "alice",
		UsedBytes: 42 << 20,
		Operations: []cratedb.Operation{
			{Name: "COLLECT", NodeName: "data-1", UsedBytes: 40 << 20, Started: jobStart.Add(100 * time.Millisecond)},
			{Name: "MERGE", NodeName: "data-2", UsedBytes: 2 << 20, Started: jobStart.Add(500 * time.Millisecond)},
		},
	}

	now := jobStart.Add(2*time.Hour + 35*time.Minute)
	dump := formatQueryDump(q, now)

	for _, want := range []string{
		"Job ID:   abc-123",
		"User:     alice",
		"Duration: 2h35m",
		"Memory:   42.0MB (sum of 2 operation(s))",
		"Operations:",
		"- COLLECT on data-1: 40.0MB",
		"- MERGE on data-2: 2.0MB",
		"Statement:\nSELECT * FROM big_table",
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("dump missing %q\nfull dump:\n%s", want, dump)
		}
	}

	// Planning-phase job with no operations still produces a usable dump.
	bare := cratedb.ActiveQuery{
		ID:       "bare",
		Stmt:     "SELECT 1",
		Started:  jobStart,
		Username: "bob",
	}
	bareDump := formatQueryDump(bare, jobStart.Add(500*time.Millisecond))
	if strings.Contains(bareDump, "Operations:") {
		t.Errorf("expected no Operations section for op-less job, got:\n%s", bareDump)
	}
	if !strings.HasSuffix(bareDump, "\n") {
		t.Errorf("dump must end with newline")
	}
}

// snapshotWithQueries is a tiny helper to build a StoreSnapshot for tests.
func snapshotWithQueries(qs ...cratedb.ActiveQuery) store.StoreSnapshot {
	return store.StoreSnapshot{ActiveQueries: qs}
}

// Queries older than stuckThreshold are hidden by default; pressing `h`
// toggles them back in, and the selection re-anchors to the same job ID
// across the toggle so the cursor doesn't jump to an unrelated row.
func TestQueriesHideStuckToggle(t *testing.T) {
	now := time.Now()
	fresh1 := cratedb.ActiveQuery{ID: "fresh-1", Started: now.Add(-2 * time.Second), Stmt: "SELECT 1"}
	fresh2 := cratedb.ActiveQuery{ID: "fresh-2", Started: now.Add(-5 * time.Second), Stmt: "SELECT 2"}
	stuck := cratedb.ActiveQuery{ID: "stuck-1", Started: now.Add(-48 * time.Hour), Stmt: "DECLARE c CURSOR ..."}

	m := NewQueriesModel(120, 40).Refresh(snapshotWithQueries(stuck, fresh1, fresh2))

	visible, hidden := m.visibleQueries()
	if len(visible) != 2 || hidden != 1 {
		t.Fatalf("default filter: want 2 visible / 1 hidden, got %d / %d", len(visible), hidden)
	}
	if visible[0].ID != "fresh-1" || visible[1].ID != "fresh-2" {
		t.Errorf("unexpected visible order: %+v", visible)
	}

	// Anchor selection on fresh-2 (index 1) before toggling.
	m.selected = 1
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'h'}})

	visible, hidden = m.visibleQueries()
	if len(visible) != 3 || hidden != 0 {
		t.Fatalf("after toggle: want 3 visible / 0 hidden, got %d / %d", len(visible), hidden)
	}
	if visible[m.selected].ID != "fresh-2" {
		t.Errorf("selection did not re-anchor to fresh-2; landed on %s", visible[m.selected].ID)
	}

	// Toggle back: should hide stuck again and re-anchor on fresh-2.
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'h'}})
	visible, hidden = m.visibleQueries()
	if len(visible) != 2 || hidden != 1 {
		t.Fatalf("after second toggle: want 2 visible / 1 hidden, got %d / %d", len(visible), hidden)
	}
	if visible[m.selected].ID != "fresh-2" {
		t.Errorf("selection did not re-anchor to fresh-2 after second toggle; landed on %s", visible[m.selected].ID)
	}
}

// `y` on the list (no info modal open) must emit YankResultMsg whose
// clipboard payload matches formatQueryDump for the highlighted row.
func TestQueriesYankFromList(t *testing.T) {
	q := cratedb.ActiveQuery{
		ID:       "row-yank",
		Started:  time.Now().Add(-1 * time.Second),
		Stmt:     "SELECT 99",
		Username: "carol",
	}
	m := NewQueriesModel(120, 40).Refresh(snapshotWithQueries(q))

	_, cmd := m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'y'}})
	if cmd == nil {
		t.Fatal("expected a tea.Cmd from y on list, got nil")
	}
	msg := cmd()
	yr, ok := msg.(YankResultMsg)
	if !ok {
		t.Fatalf("expected YankResultMsg, got %T", msg)
	}
	if yr.Error != "" {
		t.Errorf("unexpected yank error: %s", yr.Error)
	}
}

func keyRune(r rune) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}}
}

// The slowest board keeps its own cursor, anchored by job ID so re-sorting
// between ticks doesn't move it to a different job.
func TestQueriesSlowestToggleAndAnchor(t *testing.T) {
	now := time.Now()
	a := store.ObservedQuery{ActiveQuery: cratedb.ActiveQuery{ID: "a", Started: now.Add(-3 * time.Minute)}, LastSeen: now.Add(-time.Minute), Done: true}
	b := store.ObservedQuery{ActiveQuery: cratedb.ActiveQuery{ID: "b", Started: now.Add(-time.Minute)}}
	live := cratedb.ActiveQuery{ID: "live-1", Started: now.Add(-time.Second)}

	m := NewQueriesModel(120, 40).Refresh(store.StoreSnapshot{
		ActiveQueries:  []cratedb.ActiveQuery{live},
		SlowestQueries: []store.ObservedQuery{a, b},
	})
	m, _ = m.HandleKey(keyRune('S'))
	if !m.showSlowest {
		t.Fatal("S did not switch to slowest view")
	}
	m, _ = m.HandleKey(keyRune('j'))
	if m.slowSelected != 1 || m.selected != 0 {
		t.Fatalf("slowSelected=%d selected=%d, want 1/0", m.slowSelected, m.selected)
	}

	// b overtakes a: cursor follows b to row 0.
	m = m.Refresh(store.StoreSnapshot{SlowestQueries: []store.ObservedQuery{b, a}})
	if m.slowSelected != 0 {
		t.Errorf("slowSelected = %d, want 0 (anchored to b)", m.slowSelected)
	}

	// K is inert on the board.
	m, _ = m.HandleKey(keyRune('K'))
	if m.killTarget != nil {
		t.Error("K must not arm a kill from the slowest view")
	}

	m, _ = m.HandleKey(keyRune('S'))
	if m.showSlowest {
		t.Error("second S did not return to live view")
	}
}

// A finished job's info modal survives refreshes (it's gone from sys.jobs
// but still on the board) and reports the frozen duration.
func TestQueriesSlowestInfoFinished(t *testing.T) {
	t0 := time.Now().Add(-10 * time.Minute)
	done := store.ObservedQuery{
		ActiveQuery: cratedb.ActiveQuery{ID: "done-1", Started: t0, Stmt: "SELECT slow"},
		LastSeen:    t0.Add(4*time.Minute + 12*time.Second),
		Done:        true,
	}
	snap := store.StoreSnapshot{SlowestQueries: []store.ObservedQuery{done}}

	m := NewQueriesModel(120, 40).Refresh(snap)
	m, _ = m.HandleKey(keyRune('S'))
	m, _ = m.HandleKey(keyRune('i'))
	m = m.Refresh(snap)
	if m.infoTarget == nil || m.infoTarget.ID != "done-1" {
		t.Fatal("info modal closed for a finished job still on the board")
	}
	if got := m.infoEnd; !got.Equal(done.LastSeen) {
		t.Errorf("infoEnd = %s, want LastSeen", got)
	}
	if !strings.Contains(m.View(), "4m12s") {
		t.Error("info modal should show the frozen 4m12s duration")
	}

	// Pushed off the board: modal closes.
	m = m.Refresh(store.StoreSnapshot{})
	if m.infoTarget != nil {
		t.Error("info modal should close once the job leaves the board")
	}
}

func TestQueriesSlowestView(t *testing.T) {
	now := time.Now()
	m := NewQueriesModel(120, 40).Refresh(store.StoreSnapshot{
		ObservedSince:  now.Add(-time.Hour),
		SampleInterval: 2 * time.Second,
		ActiveQueries:  []cratedb.ActiveQuery{{ID: "cursor", Started: now.Add(-48 * time.Hour)}},
	})
	m, _ = m.HandleKey(keyRune('S'))
	v := m.View()
	for _, want := range []string{"Slowest Queries", "sampled every 2.0s", "1 stuck excluded", "No queries observed yet"} {
		if !strings.Contains(v, want) {
			t.Errorf("view missing %q\n%s", want, v)
		}
	}
}

// K stays available in read-only mode (the default), behind its confirm.
func TestKillAllowedInReadOnly(t *testing.T) {
	cfg := config.DefaultConfig()
	st := store.New(cfg.TUI.SparklineHistory, cfg.Collectors)
	mgr := collector.NewManager(nil, st, collector.NewQueryTracker(cfg.Collectors, cfg.Connection))
	a := NewApp(st, nil, mgr, context.Background(), cfg.TUI, true)
	a.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	a.Update(keyRune('3'))
	a.queries = a.queries.Refresh(store.StoreSnapshot{ActiveQueries: []cratedb.ActiveQuery{{ID: "a", Started: time.Now()}}})
	a.Update(keyRune('K'))
	if a.queries.killTarget == nil || a.queries.killTarget.ID != "a" {
		t.Fatal("K did not open the kill confirm in read-only mode")
	}
}
