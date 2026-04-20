package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

const killResultDisplayDuration = 3 * time.Second

type KillQueryMsg struct {
	ID string
}

type KillQueryResultMsg struct {
	ID    string
	Error string
}

// QueriesModel shows active queries.
type QueriesModel struct {
	snap         store.StoreSnapshot
	selected     int
	width        int
	height       int
	keyMap       KeyMap
	killTarget   *cratedb.ActiveQuery
	killResult   string
	killIsError  bool
	killResultAt time.Time
}

func NewQueriesModel(width, height int) QueriesModel {
	return QueriesModel{width: width, height: height, keyMap: DefaultKeyMap()}
}

func (m QueriesModel) Refresh(snap store.StoreSnapshot) QueriesModel {
	m.snap = snap
	if m.killResult != "" && time.Since(m.killResultAt) > killResultDisplayDuration {
		m.killResult = ""
	}
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
	km := m.keyMap

	// Modal active — only accept confirmation keys
	if m.killTarget != nil {
		switch msg.String() {
		case "y", "enter":
			target := m.killTarget
			m.killTarget = nil
			return m, func() tea.Msg {
				return KillQueryMsg{ID: target.ID}
			}
		case "n", "esc":
			m.killTarget = nil
		}
		return m, nil
	}

	switch {
	case key.Matches(msg, km.Up):
		if m.selected > 0 {
			m.selected--
		}
	case key.Matches(msg, km.Down):
		if m.selected < len(m.snap.ActiveQueries)-1 {
			m.selected++
		}
	case key.Matches(msg, km.Kill):
		if m.selected < len(m.snap.ActiveQueries) {
			q := m.snap.ActiveQueries[m.selected]
			m.killTarget = &q
		}
	}
	return m, nil
}

func (m QueriesModel) View() string {
	// Short-circuit: render modal over dimmed background without building body
	if m.killTarget != nil {
		return m.renderKillModal()
	}

	title := styleTitle.Render("Active Queries")

	if m.snap.Staleness["queries"] {
		return title + "\n" + styleStale.Render("  (stale data)")
	}

	if len(m.snap.ActiveQueries) == 0 {
		body := title + "\n  No active queries"
		if m.killResult != "" {
			body = "  " + m.killResultStyle().Render(m.killResult) + "\n" + body
		}
		return body
	}

	var lines []string

	if m.killResult != "" {
		lines = append(lines, "  "+m.killResultStyle().Render(m.killResult))
	}

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

		stmt := strings.ReplaceAll(q.Stmt, "\n", " ")
		maxStmtLen := m.width - 42
		if maxStmtLen < 20 {
			maxStmtLen = 20
		}
		stmt = truncateString(stmt, maxStmtLen)

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

func (m QueriesModel) killResultStyle() lipgloss.Style {
	if m.killIsError {
		return styleHealthRed
	}
	return styleHealthGreen
}

// renderKillModal renders the kill confirmation modal centered over a dimmed background.
func (m QueriesModel) renderKillModal() string {
	modalWidth := m.width * 65 / 100
	if modalWidth < 40 {
		modalWidth = 40
	}
	// Inner content width = modal width minus border (2) and padding (4)
	innerWidth := modalWidth - 6
	if innerWidth < 20 {
		innerWidth = 20
	}

	q := m.killTarget
	stmt := truncateString(strings.ReplaceAll(q.Stmt, "\n", " "), innerWidth)

	title := styleModalTitle.Render("Kill this query?")
	id := fmt.Sprintf("ID:   %s", q.ID)
	stmtLine := fmt.Sprintf("Stmt: %s", stmt)
	footer := styleDim.Render("[y]es  [n]o")

	content := lipgloss.JoinVertical(lipgloss.Left, title, "", id, stmtLine, "", footer)
	modal := styleModalBorder.Width(innerWidth).Render(content)

	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, modal,
		lipgloss.WithWhitespaceBackground(colorOverlayBg))
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
