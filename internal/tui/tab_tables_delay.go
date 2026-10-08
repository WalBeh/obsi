package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// delayCell is the DELAY column: the shortest node-left delay the table or
// a partition has, * when only older partitions are that short, yellow when
// a node was away longer.
func (m TablesModel) delayCell(t cratedb.TableInfo) string {
	if t.NodeLeftDelayMin == 0 {
		return fmt.Sprintf("%7s", "")
	}
	v := shortDelay(t.NodeLeftDelayMin)
	if t.PartitionsBelowDelay > 0 {
		v += "*"
	}
	v = fmt.Sprintf("%7s", v)
	if a, ok := longestAbsence(m.snap.NodeAbsences); ok && a.Duration() > t.NodeLeftDelayMin {
		return styleHealthYellow.Render(v)
	}
	return v
}

func (m TablesModel) delayDetail(t cratedb.TableInfo) string {
	if t.NodeLeftDelayMin == 0 {
		return ""
	}
	line := "    Node-left delay: " + shortDelay(t.NodeLeftDelayMin)
	if t.PartitionsBelowDelay > 0 {
		line += fmt.Sprintf(" (table %s, %d older partitions at %s)",
			shortDelay(t.Settings.NodeLeftDelay), t.PartitionsBelowDelay, shortDelay(t.NodeLeftDelayMin))
	}
	if a, ok := longestAbsence(m.snap.NodeAbsences); ok && a.Duration() > t.NodeLeftDelayMin {
		line += styleHealthYellow.Render(fmt.Sprintf(" ▲ a node was away %s: its replicas get rebuilt", formatDuration(a.Duration().Round(time.Second))))
	}
	return line + styleDim.Render("  e: change")
}

func (m TablesModel) selectedTable() (cratedb.TableInfo, bool) {
	if m.selected < len(m.sorted) {
		return m.snap.Tables[m.sorted[m.selected]], true
	}
	return cratedb.TableInfo{}, false
}

// handleDelayKey covers e, the value input and the confirm. handled is
// false for keys that aren't its business.
func (m TablesModel) handleDelayKey(msg tea.KeyMsg) (TablesModel, tea.Cmd, bool) {
	if m.delayConfirm != nil {
		done, cmd := m.delayConfirm.key(msg)
		if done {
			m.delayConfirm = nil
		}
		return m, cmd, true
	}
	if m.delayInput {
		switch msg.Type {
		case tea.KeyEsc:
			m.delayInput, m.delayBuf = false, ""
		case tea.KeyEnter:
			val := strings.TrimSpace(m.delayBuf)
			if _, ok := parseDelay(val); !ok {
				m = m.notice("", fmt.Sprintf("%q isn't a time value, e.g. 5m or 300s", val))
				return m, nil, true
			}
			m.delayInput, m.delayBuf = false, ""
			if t, ok := m.selectedTable(); ok {
				m.delayConfirm = newDelayConfirm(fromTables, val, []cratedb.TableInfo{t}, 0)
			}
		case tea.KeyBackspace:
			if len(m.delayBuf) > 0 {
				m.delayBuf = m.delayBuf[:len(m.delayBuf)-1]
			}
		case tea.KeyRunes:
			m.delayBuf += string(msg.Runes)
		}
		return m, nil, true
	}
	if !key.Matches(msg, m.keyMap.Edit) {
		return m, nil, false
	}
	t, ok := m.selectedTable()
	switch {
	case !ok:
	case m.readOnly:
		m = m.notice("", readOnlyRefusal("the node-left delay is not editable"))
	default:
		m.delayInput, m.delayBuf = true, shortDelay(t.NodeLeftDelayMin)
	}
	return m, nil, true
}

func (m TablesModel) delayInputLine() string {
	t, _ := m.selectedTable()
	return fmt.Sprintf("  Node-left delay for %s.%s: %s", t.SchemaName, t.TableName, styleEditInput.Render(m.delayBuf+"▏")) +
		styleDim.Render(fmt.Sprintf("  [Enter] review  [Esc] cancel · suggested %s", shortDelay(suggestDelay(m.snap.NodeAbsences))))
}

func (m TablesModel) notice(note, err string) TablesModel {
	m.noticeText, m.noticeIsErr, m.noticeAt = note, err != "", time.Now()
	if err != "" {
		m.noticeText = err
	}
	return m
}

func (m TablesModel) noticeLine() string {
	if m.noticeText == "" || time.Since(m.noticeAt) > 8*time.Second {
		return ""
	}
	if m.noticeIsErr {
		return "  " + styleHealthRed.Render(m.noticeText)
	}
	return "  " + styleHealthGreen.Render(m.noticeText)
}

// YankTableDDLMsg asks the App for SHOW CREATE TABLE of a table, copied to
// the clipboard.
type YankTableDDLMsg struct{ Schema, Name string }

// TablesNoticeMsg reports back to the Tables tab.
type TablesNoticeMsg struct{ Note, Error string }

func yankTableDDL(ctx context.Context, reg *cratedb.Registry, msg YankTableDDLMsg) TablesNoticeMsg {
	resp, err := reg.Query(ctx, fmt.Sprintf("SHOW CREATE TABLE %s.%s", quoteIdent(msg.Schema), quoteIdent(msg.Name))+cratedb.QueryTag)
	if err != nil {
		return TablesNoticeMsg{Error: "SHOW CREATE TABLE failed: " + firstLine(err.Error())}
	}
	if len(resp.Rows) == 0 || len(resp.Rows[0]) == 0 {
		return TablesNoticeMsg{Error: "SHOW CREATE TABLE returned nothing"}
	}
	ddl := strings.TrimRight(cratedb.ToString(resp.Rows[0][0]), "\n") + ";\n"
	if e := writeClipboard(ddl); e != "" {
		return TablesNoticeMsg{Error: "copy failed: " + e}
	}
	return TablesNoticeMsg{Note: fmt.Sprintf("copied CREATE TABLE %s.%s (%d lines)", msg.Schema, msg.Name, strings.Count(ddl, "\n"))}
}
