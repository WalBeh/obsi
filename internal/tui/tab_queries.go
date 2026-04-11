package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// QueriesModel shows active queries.
type QueriesModel struct {
	snap     store.StoreSnapshot
	selected int
	width    int
	height   int
}

func NewQueriesModel(width, height int) QueriesModel {
	return QueriesModel{width: width, height: height}
}

func (m QueriesModel) Refresh(snap store.StoreSnapshot) QueriesModel {
	m.snap = snap
	if m.selected >= len(snap.ActiveQueries) && len(snap.ActiveQueries) > 0 {
		m.selected = len(snap.ActiveQueries) - 1
	}
	return m
}

func (m QueriesModel) SetSize(width, height int) QueriesModel {
	m.width = width
	m.height = height
	return m
}

func (m QueriesModel) HandleKey(msg tea.KeyMsg) (QueriesModel, tea.Cmd) {
	km := DefaultKeyMap()
	switch {
	case key.Matches(msg, km.Up):
		if m.selected > 0 {
			m.selected--
		}
	case key.Matches(msg, km.Down):
		if m.selected < len(m.snap.ActiveQueries)-1 {
			m.selected++
		}
	}
	return m, nil
}

func (m QueriesModel) View() string {
	title := styleTitle.Render("Active Queries")

	if m.snap.Staleness["queries"] {
		return title + "\n" + styleStale.Render("  (stale data)")
	}

	if len(m.snap.ActiveQueries) == 0 {
		return title + "\n  No active queries"
	}

	var lines []string
	lines = append(lines, title)
	lines = append(lines, fmt.Sprintf("  %d active queries", len(m.snap.ActiveQueries)))
	lines = append(lines, "")

	// Header
	header := styleHeader.Render(fmt.Sprintf("  %-3s %-10s %-12s %-10s %s",
		"", "DURATION", "NODE", "USER", "STATEMENT"))
	lines = append(lines, header)

	now := time.Now()
	for i, q := range m.snap.ActiveQueries {
		marker := "  "
		if i == m.selected {
			marker = "▸ "
		}

		duration := now.Sub(q.Started)
		durStr := formatDuration(duration)

		// Truncate statement for list view
		stmt := strings.ReplaceAll(q.Stmt, "\n", " ")
		maxStmtLen := m.width - 42
		if maxStmtLen < 20 {
			maxStmtLen = 20
		}
		if len(stmt) > maxStmtLen {
			stmt = stmt[:maxStmtLen-3] + "..."
		}

		durStyle := styleValue
		if duration > 30*time.Second {
			durStyle = styleHighValue
		} else if duration > 10*time.Second {
			durStyle = styleHealthYellow
		}

		row := fmt.Sprintf("%s%s %-12s %-10s %s",
			marker,
			durStyle.Render(fmt.Sprintf("%-10s", durStr)),
			q.Node, q.Username, stmt)
		lines = append(lines, row)
	}

	// Detail panel for selected query
	if m.selected < len(m.snap.ActiveQueries) {
		lines = append(lines, "")
		q := m.snap.ActiveQueries[m.selected]
		lines = append(lines, styleTitle.Render("  Query Detail"))
		lines = append(lines, fmt.Sprintf("    ID:       %s", q.ID))
		lines = append(lines, fmt.Sprintf("    Node:     %s", q.Node))
		lines = append(lines, fmt.Sprintf("    User:     %s", q.Username))
		lines = append(lines, fmt.Sprintf("    Started:  %s", q.Started.Format("15:04:05")))
		lines = append(lines, fmt.Sprintf("    Duration: %s", formatDuration(now.Sub(q.Started))))
		lines = append(lines, "")
		lines = append(lines, "    Statement:")
		// Show full statement with indentation
		for _, line := range strings.Split(q.Stmt, "\n") {
			lines = append(lines, "      "+line)
		}
	}

	return strings.Join(lines, "\n")
}

func formatDuration(d time.Duration) string {
	switch {
	case d >= time.Hour:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh%dm", h, m)
	case d >= time.Minute:
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", m, s)
	case d >= time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	default:
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
}
