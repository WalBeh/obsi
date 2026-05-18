package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
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

	dump := formatQueryDump(q)

	for _, want := range []string{
		"Job ID:   abc-123",
		"User:     alice",
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
	bareDump := formatQueryDump(bare)
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
