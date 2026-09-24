package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestIsReadOnlyStmt(t *testing.T) {
	for stmt, want := range map[string]bool{
		"SELECT 1":                             true,
		"  select * from sys.nodes":            true,
		"SHOW TABLES":                          true,
		"WITH x AS (SELECT 1) SELECT * FROM x": true,
		"EXPLAIN SELECT 1":                     true,
		"EXPLAIN ANALYZE SELECT 1":             true,
		"explain (costs false) select 1":       true,
		"/* hi */ -- there\nSELECT 1":          true,
		"DELETE FROM t":                        false,
		"KILL ALL":                             false,
		"SET GLOBAL \"stats.enabled\" = false": false,
		"EXPLAIN ANALYZE DELETE FROM t":        false,
		"/* SELECT */ DROP TABLE t":            false,
		"-- SELECT\nINSERT INTO t VALUES (1)":  false,
		"SELECT/**/1":                          true,
		"COPY t TO DIRECTORY '/tmp'":           false,
		"/* unterminated SELECT":               false,
		"":                                     false,
	} {
		if got := isReadOnlyStmt(stmt); got != want {
			t.Errorf("isReadOnlyStmt(%q) = %v, want %v", stmt, got, want)
		}
	}
}

func typeAndRun(m SQLModel, stmt string) (SQLModel, tea.Cmd) {
	m.input = stmt
	return m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
}

func TestSQLReadOnlyRefusesWrites(t *testing.T) {
	m := NewSQLModel(80, 24, nil, nil)
	m.readOnly = true

	m, cmd := typeAndRun(m, "DELETE FROM t")
	if cmd != nil || m.running {
		t.Fatal("DELETE was sent in read-only mode")
	}
	if m.errMsg == "" {
		t.Error("no refusal shown")
	}
	if len(m.history) != 1 {
		t.Error("refused statement should stay in history for editing")
	}

	if _, cmd = typeAndRun(m, "SELECT 1"); cmd == nil {
		t.Error("SELECT refused in read-only mode")
	}

	m.readOnly = false
	if _, cmd = typeAndRun(m, "DELETE FROM t"); cmd == nil {
		t.Error("DELETE refused with read-only off")
	}
}

func TestSettingsEditorReadOnly(t *testing.T) {
	e := newSettingsEditor(true)
	e.readOnly = true
	e, _, consumed := e.handleKey(keyRune('e'))
	if e.active || !consumed {
		t.Fatalf("active=%v consumed=%v, want edit mode refused", e.active, consumed)
	}
	if e.renderEditHint() == "" {
		t.Error("no refusal hint")
	}
}
