package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func throttleModel() ShardsModel {
	m := NewShardsModel(160, 50)
	m.readOnly = true
	m = m.Refresh(store.StoreSnapshot{
		Shards:          []cratedb.ShardInfo{{SchemaName: "doc", TableName: "t", Primary: true, RoutingState: "STARTED", NodeID: "n1"}},
		ClusterSettings: cratedb.ClusterSettings{RecoveryMaxBytesPerSec: "40mb", NodeConcurrentRecoveries: 2, ClusterConcurrentRebalance: 2},
	})
	m, _ = m.HandleKey(keyRune('v'))
	return m
}

// The form works in read-only mode and sets only what changed, TRANSIENT.
func TestThrottleFormApply(t *testing.T) {
	m, _ := throttleModel().HandleKey(keyRune('e'))
	if m.throttle == nil {
		t.Fatal("e in the recovery view did not open the form (read-only)")
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	if m.throttle.confirm {
		t.Fatal("enter without changes must not ask to apply")
	}
	for range "40mb" {
		m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("100mb")})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	if !m.throttle.confirm || !strings.Contains(m.View(), "40mb → 100mb") {
		t.Fatalf("confirm missing:\n%s", m.View())
	}
	m, cmd := m.HandleKey(keyRune('y'))
	if m.throttle != nil || cmd == nil {
		t.Fatal("y did not apply")
	}
	msg := cmd().(ApplyThrottleMsg)
	if len(msg.Stmts) != 1 || msg.Stmts[0].sql != `SET GLOBAL TRANSIENT "indices.recovery.max_bytes_per_sec" = ?` || msg.Stmts[0].arg != "100mb" {
		t.Errorf("stmts = %+v", msg.Stmts)
	}
}

func TestThrottleFormReset(t *testing.T) {
	m, _ := throttleModel().HandleKey(keyRune('e'))
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyCtrlR})
	m, cmd := m.HandleKey(keyRune('y'))
	msg := cmd().(ApplyThrottleMsg)
	if len(msg.Stmts) != 1 || !strings.HasPrefix(msg.Stmts[0].sql, `RESET GLOBAL "indices.recovery.max_bytes_per_sec", `) {
		t.Errorf("stmts = %+v", msg.Stmts)
	}

	// esc closes the form; e outside the recovery view does nothing.
	m, _ = throttleModel().HandleKey(keyRune('e'))
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEsc})
	if m.throttle != nil {
		t.Error("esc did not close")
	}
	m, _ = m.HandleKey(keyRune('v'))
	m, _ = m.HandleKey(keyRune('e'))
	if m.throttle != nil {
		t.Error("e opened the form outside the recovery view")
	}
}
