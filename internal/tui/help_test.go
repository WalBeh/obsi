package tui

import (
	"context"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func helpKeys(entries []helpEntry) string {
	var b strings.Builder
	for _, e := range entries {
		b.WriteString(e.keys + " ")
	}
	return b.String()
}

func TestTabHelp(t *testing.T) {
	km := DefaultKeyMap()
	for tab := TabOverview; tab <= TabSQL; tab++ {
		title, entries := tabHelp(tab, km, false)
		if title == "" || len(entries) == 0 {
			t.Errorf("tab %d: empty help", tab)
		}
	}

	_, live := tabHelp(TabQueries, km, false)
	for _, k := range []string{"K", "i", "y", "h", "S"} {
		if !strings.Contains(helpKeys(live), k+" ") {
			t.Errorf("live queries help missing %q", k)
		}
	}
	// K and h are inert on the slowest board; don't advertise them.
	_, slow := tabHelp(TabQueries, km, true)
	if keys := helpKeys(slow); strings.Contains(keys, "K ") || strings.Contains(keys, "h ") {
		t.Errorf("slowest help lists inert keys: %s", keys)
	}

	_, tables := tabHelp(TabTables, km, false)
	if !strings.Contains(helpKeys(tables), "f ") {
		t.Error("tables help missing f")
	}
}

func newHelpTestApp(tab Tab) *App {
	return &App{
		keyMap:    DefaultKeyMap(),
		activeTab: tab,
		queries:   NewQueriesModel(0, 0),
		sql:       NewSQLModel(0, 0, nil, context.Background()),
	}
}

// ? is text while the SQL editor has focus; F1 must still open help, and
// ? must close it again once it's open.
func TestHelpToggleInInputMode(t *testing.T) {
	a := newHelpTestApp(TabSQL)

	a.Update(keyRune('?'))
	if a.showHelp || a.sql.input != "?" {
		t.Fatalf("? in SQL editor: showHelp=%v input=%q, want typed text", a.showHelp, a.sql.input)
	}

	a.Update(tea.KeyMsg{Type: tea.KeyF1})
	if !a.showHelp {
		t.Fatal("F1 did not open help while editing")
	}
	a.Update(keyRune('x'))
	if a.sql.input != "?" {
		t.Errorf("keys leaked to the editor while help was open: %q", a.sql.input)
	}
	a.Update(keyRune('?'))
	if a.showHelp {
		t.Error("? did not close help")
	}
}

func TestHelpSwallowsTabKeys(t *testing.T) {
	a := newHelpTestApp(TabQueries)
	a.Update(keyRune('?'))
	if !a.showHelp {
		t.Fatal("? did not open help")
	}
	a.Update(keyRune('S'))
	if a.queries.showSlowest {
		t.Error("S reached the Queries tab while help was open")
	}
	a.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if a.showHelp {
		t.Error("esc did not close help")
	}
}
