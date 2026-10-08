package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestTablesNodeLeftDelay(t *testing.T) {
	now := time.Now()
	snap := store.StoreSnapshot{
		Tables: []cratedb.TableInfo{
			delayTable("a", time.Minute, time.Minute, 0),
			delayTable("parted", 5*time.Minute, time.Minute, 2),
			delayTable("slow", 10*time.Minute, 10*time.Minute, 0),
		},
		NodeAbsences: []store.NodeAbsence{{Name: "n2", Left: now.Add(-3 * time.Minute), Back: now}},
	}
	m := NewTablesModel(200, 60).Refresh(snap)
	if c := m.delayCell(snap.Tables[1]); !strings.Contains(c, "1m*") {
		t.Errorf("parted cell = %q, want 1m*", c)
	}
	view := m.View()
	if !strings.Contains(view, "DELAY") || !strings.Contains(view, "10m") {
		t.Errorf("view lacks the column:\n%s", view)
	}

	// Read-only refuses.
	m.readOnly = true
	m, _ = m.HandleKey(keyRune('e'))
	if m.delayInput || !strings.Contains(m.noticeText, "not editable") {
		t.Fatalf("read-only: input=%v notice=%q", m.delayInput, m.noticeText)
	}
	m.readOnly = false

	// Selection sits on doc.a (sorted by name): e prefills its value.
	m, _ = m.HandleKey(keyRune('e'))
	if !m.delayInput || m.delayBuf != "1m" {
		t.Fatalf("input=%v buf=%q", m.delayInput, m.delayBuf)
	}
	m.delayBuf = "5x"
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	if !m.noticeIsErr || m.delayConfirm != nil {
		t.Fatalf("bad value went through: %q", m.noticeText)
	}
	m.delayBuf = "5m"
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	if m.delayConfirm == nil || !strings.Contains(m.View(), "Set unassigned.node_left.delayed_timeout = 5m on doc.a?") {
		t.Fatalf("confirm:\n%s", m.View())
	}
	m, cmd := m.HandleKey(keyRune('y'))
	apply, ok := cmd().(ApplyNodeLeftDelayMsg)
	if !ok || apply.Origin != fromTables || len(apply.Tables) != 1 || apply.Tables[0] != [2]string{"doc", "a"} {
		t.Errorf("apply = %+v", apply)
	}
}

// The result goes back to the tab that asked.
func TestNodeLeftDelayResultRouting(t *testing.T) {
	a := newHelpTestApp(TabTables)
	a.tables = NewTablesModel(100, 40)
	a.overview = NewOverviewModel(100, 40, true)
	a.collectors = collector.NewManager(nil, nil, nil)

	a.Update(NodeLeftDelayResultMsg{Origin: fromTables, Note: "5m set on doc.a"})
	if !strings.Contains(a.tables.noticeLine(), "5m set on doc.a") || a.overview.editor.note != "" {
		t.Errorf("tables notice %q, overview note %q", a.tables.noticeLine(), a.overview.editor.note)
	}
	a.Update(NodeLeftDelayResultMsg{Origin: fromOverview, Note: "5m set on 3 tables"})
	if a.overview.editor.note != "5m set on 3 tables" {
		t.Errorf("overview note %q", a.overview.editor.note)
	}
}
