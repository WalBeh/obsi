package tui

import (
	"context"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// Pins app.go's per-tab dispatch (switching, input mode, snapshot hints,
// rendering) ahead of replacing its activeTab switches. No registry: these
// paths must not touch the network.
func newTestApp(t *testing.T) *App {
	t.Helper()
	cfg := config.DefaultConfig()
	st := store.New(cfg.TUI.SparklineHistory, cfg.Collectors)
	mgr := collector.NewManager(nil, st, collector.NewQueryTracker(cfg.Collectors, cfg.Connection))
	a := NewApp(st, nil, mgr, context.Background(), cfg.TUI)
	a.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	return a
}

func TestAppTabSwitching(t *testing.T) {
	a := newTestApp(t)
	for i, want := range []Tab{TabOverview, TabNodes, TabQueries, TabTables, TabShards, TabSQL} {
		a.Update(keyRune(rune('1' + i)))
		if a.activeTab != want {
			t.Errorf("key %d: activeTab=%d, want %d", i+1, a.activeTab, want)
		}
	}
	// From SQL (editing by default) tab still switches, and wraps.
	a.Update(keyType(tea.KeyTab))
	if a.activeTab != TabOverview {
		t.Errorf("tab from SQL: activeTab=%d, want overview", a.activeTab)
	}
	a.Update(keyType(tea.KeyShiftTab))
	if a.activeTab != TabSQL {
		t.Errorf("shift+tab from overview: activeTab=%d, want sql", a.activeTab)
	}
}

// While a tab is taking text, digits are text, not tab switches.
func TestAppInputModeKeepsKeys(t *testing.T) {
	a := newTestApp(t)
	a.Update(keyRune('2'))
	a.Update(keyRune('/'))
	if !a.isTabInputMode() {
		t.Fatal("nodes search should be input mode")
	}
	a.Update(keyRune('3'))
	if a.activeTab != TabNodes || a.nodes.search != "3" {
		t.Errorf("activeTab=%d search=%q, want nodes/3", a.activeTab, a.nodes.search)
	}
	a.Update(keyType(tea.KeyEsc))
	a.Update(keyRune('3'))
	if a.activeTab != TabQueries {
		t.Errorf("after esc, 3 should switch to queries, got %d", a.activeTab)
	}
}

func TestAppInputModePerTab(t *testing.T) {
	a := newTestApp(t)
	for _, tab := range []Tab{TabOverview, TabNodes, TabQueries, TabTables, TabShards} {
		a.setActiveTab(tab)
		if a.isTabInputMode() {
			t.Errorf("tab %d idle should not be input mode", tab)
		}
	}
	a.setActiveTab(TabSQL)
	if !a.isTabInputMode() {
		t.Error("sql editor starts in input mode")
	}
}

func TestAppSnapshotHints(t *testing.T) {
	a := newTestApp(t)
	want := map[Tab]store.SnapshotHint{
		TabOverview: {IncludeCluster: true, IncludeHealth: true, IncludeNodes: true, IncludeTables: true, IncludeJMX: true},
		TabNodes:    {IncludeNodes: true, IncludeJMX: true},
		TabQueries:  {IncludeQueries: true},
		TabTables:   {IncludeTables: true, IncludeHealth: true},
		TabShards:   {IncludeShards: true},
		TabSQL:      {},
	}
	for tab, h := range want {
		a.activeTab = tab
		if got := a.snapshotHint(); got != h {
			t.Errorf("tab %d hint = %+v, want %+v", tab, got, h)
		}
	}
}

func TestAppViewEveryTab(t *testing.T) {
	a := newTestApp(t)
	for tab := TabOverview; tab <= TabSQL; tab++ {
		a.setActiveTab(tab)
		v := a.View()
		if !strings.Contains(v, tabNames[tab]) {
			t.Errorf("tab %d view lacks its tab name", tab)
		}
	}
	a.Update(keyType(tea.KeyF1)) // ? would be typed into the SQL editor
	if !strings.Contains(a.View(), "Keys: SQL") {
		t.Error("help modal not rendered over the SQL tab")
	}
}

func TestAppResizePropagates(t *testing.T) {
	a := newTestApp(t)
	a.Update(tea.WindowSizeMsg{Width: 90, Height: 30})
	body := a.bodyHeight()
	if a.nodes.width != 90 || a.nodes.height != body || a.queries.height != body || a.sql.width != 90 {
		t.Errorf("sizes not propagated: nodes %dx%d queries h=%d sql w=%d, body=%d",
			a.nodes.width, a.nodes.height, a.queries.height, a.sql.width, body)
	}
}

// The manual refresh key triggers these collectors per tab.
func TestAppRefreshCollectors(t *testing.T) {
	a := newTestApp(t)
	want := map[Tab][]string{
		TabOverview: {"health", "cluster"},
		TabNodes:    {"nodes"},
		TabQueries:  {"queries"},
		TabTables:   {"shards"},
		TabShards:   {"shards"},
		TabSQL:      nil,
	}
	for tab, cols := range want {
		a.activeTab = tab
		if got := a.current().Collectors(); strings.Join(got, ",") != strings.Join(cols, ",") {
			t.Errorf("tab %d refresh collectors = %v, want %v", tab, got, cols)
		}
	}
}
