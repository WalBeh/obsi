package tui

import (
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// throttleField is one recovery throttle setting in the v view's form.
type throttleField struct {
	path, value, orig string
}

// throttleForm edits the recovery throttle from the recovery view. It's
// allowed in read-only mode: it only changes how fast shards are copied.
// Values are set TRANSIENT so an incident-time bump is gone after a full
// cluster restart instead of lingering.
type throttleForm struct {
	fields  []throttleField
	cursor  int
	confirm bool // reviewing changes (or the reset) before applying
	reset   bool // confirm is for RESET GLOBAL
}

// ApplyThrottleMsg asks the App to run the throttle statements in order.
type ApplyThrottleMsg struct {
	Stmts []throttleStmt
}

type throttleStmt struct {
	sql string
	arg string // "" for RESET
}

var throttlePaths = []string{
	"indices.recovery.max_bytes_per_sec",
	"cluster.routing.allocation.node_concurrent_recoveries",
	"cluster.routing.allocation.cluster_concurrent_rebalance",
}

func (m ShardsModel) newThrottleForm() *throttleForm {
	cs := m.snap.ClusterSettings
	vals := []string{cs.RecoveryMaxBytesPerSec, strconv.Itoa(cs.NodeConcurrentRecoveries), strconv.Itoa(cs.ClusterConcurrentRebalance)}
	f := &throttleForm{}
	for i, p := range throttlePaths {
		f.fields = append(f.fields, throttleField{path: p, value: vals[i], orig: vals[i]})
	}
	return f
}

func (f *throttleForm) changes() []throttleField {
	var out []throttleField
	for _, fl := range f.fields {
		if strings.TrimSpace(fl.value) != fl.orig {
			out = append(out, fl)
		}
	}
	return out
}

func (f *throttleForm) stmts() []throttleStmt {
	if f.reset {
		quoted := make([]string, len(throttlePaths))
		for i, p := range throttlePaths {
			quoted[i] = `"` + p + `"`
		}
		return []throttleStmt{{sql: "RESET GLOBAL " + strings.Join(quoted, ", ")}}
	}
	var out []throttleStmt
	for _, c := range f.changes() {
		out = append(out, throttleStmt{sql: fmt.Sprintf(`SET GLOBAL TRANSIENT "%s" = ?`, c.path), arg: strings.TrimSpace(c.value)})
	}
	return out
}

// handleThrottleKey drives the form; it owns every key while open.
func (m ShardsModel) handleThrottleKey(msg tea.KeyMsg) (ShardsModel, tea.Cmd) {
	f := m.throttle
	if f.confirm {
		switch msg.String() {
		case "y", "enter":
			stmts := f.stmts()
			m.throttle = nil
			return m, func() tea.Msg { return ApplyThrottleMsg{Stmts: stmts} }
		case "n", "esc":
			f.confirm, f.reset = false, false
		}
		return m, nil
	}
	fl := &f.fields[f.cursor]
	switch msg.Type {
	case tea.KeyEsc:
		m.throttle = nil
	case tea.KeyUp, tea.KeyShiftTab:
		f.cursor = (f.cursor - 1 + len(f.fields)) % len(f.fields)
	case tea.KeyDown, tea.KeyTab:
		f.cursor = (f.cursor + 1) % len(f.fields)
	case tea.KeyEnter:
		if len(f.changes()) > 0 {
			f.confirm = true
		}
	case tea.KeyCtrlR:
		f.confirm, f.reset = true, true
	case tea.KeyBackspace:
		if r := []rune(fl.value); len(r) > 0 {
			fl.value = string(r[:len(r)-1])
		}
	case tea.KeyRunes:
		fl.value += string(msg.Runes)
	}
	return m, nil
}

func (m ShardsModel) renderThrottleForm() string {
	f := m.throttle
	inner := modalInnerWidth(m.width, 75, 70)
	var lines []string
	if f.confirm {
		if f.reset {
			lines = append(lines, styleModalTitle.Render("Reset the recovery throttle?"), "",
				"RESET GLOBAL on all three: back to the persistent values or CrateDB's defaults.")
		} else {
			lines = append(lines, styleModalTitle.Render("Apply (SET GLOBAL TRANSIENT)?"), "")
			for _, c := range f.changes() {
				lines = append(lines, fmt.Sprintf("%s: %s → %s", c.path, c.orig, styleValue.Render(strings.TrimSpace(c.value))))
			}
		}
		lines = append(lines, "", styleDim.Render("[y]es  [n]o"))
		return placeModal(strings.Join(lines, "\n"), inner, m.width, m.height)
	}
	lines = append(lines, styleModalTitle.Render("Recovery throttle"), "")
	for i, fl := range f.fields {
		marker, val := "  ", fl.value
		if i == f.cursor {
			marker, val = "▸ ", styleEditInput.Render(fl.value+"▏")
		}
		lines = append(lines, fmt.Sprintf("%s%-56s %s", marker, fl.path, val))
	}
	lines = append(lines, "",
		styleDim.Render("Set TRANSIENT: gone after a full cluster restart. Allowed in read-only mode."),
		styleDim.Render("[↑↓] field  [enter] review  [ctrl+r] reset all three  [esc] cancel"))
	return placeModal(strings.Join(lines, "\n"), inner, m.width, m.height)
}
