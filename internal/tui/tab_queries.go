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

// stuckThreshold is the age beyond which a sys.jobs row is considered an
// abandoned cursor / runaway query and hidden by default. Long-lived
// `DECLARE ... CURSOR WITH HOLD` statements dominate the list on busy
// clusters and drown out the queries an operator actually needs to see.
const stuckThreshold = 24 * time.Hour

type KillQueryMsg struct {
	ID string
}

type KillQueryResultMsg struct {
	ID    string
	Error string
}

// YankResultMsg is emitted after the clipboard write completes, so the
// status line can confirm to the user that the copy actually happened.
type YankResultMsg struct {
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
	infoTarget   *cratedb.ActiveQuery
	yankResult   string
	yankResultAt time.Time
	showStuck    bool // when true, queries older than stuckThreshold are listed
}

func NewQueriesModel(width, height int) QueriesModel {
	return QueriesModel{width: width, height: height, keyMap: DefaultKeyMap()}
}

func (m QueriesModel) Refresh(snap store.StoreSnapshot) QueriesModel {
	m.snap = snap
	if m.killResult != "" && time.Since(m.killResultAt) > killResultDisplayDuration {
		m.killResult = ""
	}
	if m.yankResult != "" && time.Since(m.yankResultAt) > killResultDisplayDuration {
		m.yankResult = ""
	}
	visible, _ := m.visibleQueries()
	if m.selected >= len(visible) && len(visible) > 0 {
		m.selected = len(visible) - 1
	}
	// Keep the info modal's data fresh so memory/ops update live while open.
	// If the job has finished it's gone from sys.jobs — close the modal.
	if m.infoTarget != nil {
		updated := false
		for i := range snap.ActiveQueries {
			if snap.ActiveQueries[i].ID == m.infoTarget.ID {
				q := snap.ActiveQueries[i]
				m.infoTarget = &q
				updated = true
				break
			}
		}
		if !updated {
			m.infoTarget = nil
		}
	}
	return m
}

func (m QueriesModel) SetSize(width, height int) QueriesModel {
	m.width = width
	m.height = height
	return m
}

// visibleQueries returns the queries shown in the list and the count of
// stuck queries hidden by the current filter. When showStuck is true the
// full snapshot is returned and hidden is zero.
func (m QueriesModel) visibleQueries() (visible []cratedb.ActiveQuery, hidden int) {
	if m.showStuck {
		return m.snap.ActiveQueries, 0
	}
	now := time.Now()
	visible = make([]cratedb.ActiveQuery, 0, len(m.snap.ActiveQueries))
	for _, q := range m.snap.ActiveQueries {
		if now.Sub(q.Started) > stuckThreshold {
			hidden++
			continue
		}
		visible = append(visible, q)
	}
	return visible, hidden
}

func (m QueriesModel) HandleKey(msg tea.KeyMsg) (QueriesModel, tea.Cmd) {
	km := m.keyMap

	// Kill confirmation modal — only accept confirmation keys.
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

	// Info modal — esc closes, y yanks to clipboard.
	if m.infoTarget != nil {
		switch {
		case key.Matches(msg, km.Yank):
			payload := formatQueryDump(*m.infoTarget)
			return m, func() tea.Msg {
				return YankResultMsg{Error: writeClipboard(payload)}
			}
		case msg.String() == "esc" || key.Matches(msg, km.Info):
			m.infoTarget = nil
		}
		return m, nil
	}

	visible, _ := m.visibleQueries()

	switch {
	case key.Matches(msg, km.Up):
		if m.selected > 0 {
			m.selected--
		}
	case key.Matches(msg, km.Down):
		if m.selected < len(visible)-1 {
			m.selected++
		}
	case key.Matches(msg, km.Kill):
		if m.selected < len(visible) {
			q := visible[m.selected]
			m.killTarget = &q
		}
	case key.Matches(msg, km.Info):
		if m.selected < len(visible) {
			q := visible[m.selected]
			m.infoTarget = &q
		}
	case key.Matches(msg, km.Hide):
		// Re-anchor selection to the same job ID across the toggle so the
		// cursor doesn't jump to an unrelated row.
		var anchorID string
		if m.selected < len(visible) {
			anchorID = visible[m.selected].ID
		}
		m.showStuck = !m.showStuck
		newVisible, _ := m.visibleQueries()
		m.selected = 0
		for i, q := range newVisible {
			if q.ID == anchorID {
				m.selected = i
				break
			}
		}
	}
	return m, nil
}

func (m QueriesModel) View() string {
	// Short-circuit: render modal over dimmed background without building body
	if m.killTarget != nil {
		return m.renderKillModal()
	}
	if m.infoTarget != nil {
		return m.renderInfoModal()
	}

	stale := m.snap.Staleness["queries"]
	title := styleTitle.Render("Active Queries")
	if stale {
		title += " " + styleStale.Render("(stale)")
	}

	visible, hidden := m.visibleQueries()

	if len(visible) == 0 {
		body := title + "\n  No active queries"
		if hidden > 0 {
			body += styleDim.Render(fmt.Sprintf(" (%d stuck hidden — press h to show)", hidden))
		}
		if m.killResult != "" {
			body = "  " + m.killResultStyle().Render(m.killResult) + "\n" + body
		}
		return body
	}

	var lines []string

	if m.killResult != "" {
		lines = append(lines, "  "+m.killResultStyle().Render(m.killResult))
	}
	if m.yankResult != "" {
		lines = append(lines, "  "+styleHealthGreen.Render(m.yankResult))
	}

	lines = append(lines, title)
	countLine := fmt.Sprintf("  %d active queries", len(visible))
	if hidden > 0 {
		countLine += styleDim.Render(fmt.Sprintf(" (%d stuck hidden — press h to show)", hidden))
	} else if m.showStuck {
		countLine += styleDim.Render(" (showing stuck — press h to hide)")
	}
	lines = append(lines, countLine)
	lines = append(lines, "")

	// Header
	header := styleHeader.Render(fmt.Sprintf("  %-3s %-10s %-22s %-12s %-10s %s",
		"", "DURATION", "MEMORY", "NODE", "USER", "STATEMENT"))
	lines = append(lines, header)

	now := time.Now()
	for i, q := range visible {
		marker := "  "
		if i == m.selected {
			marker = "▸ "
		}

		duration := now.Sub(q.Started)
		durStr := formatDuration(duration)

		// Memory column: total bytes + dominant operation name. "—" when
		// the job has no operation rows yet (planning phase).
		memCol := "—"
		if len(q.Operations) > 0 {
			memCol = fmt.Sprintf("%s %s", formatBytes(q.UsedBytes), q.Operations[0].Name)
		}
		memCol = truncateString(memCol, 22)

		stmt := strings.ReplaceAll(q.Stmt, "\n", " ")
		maxStmtLen := m.width - 65
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

		row := fmt.Sprintf("%s%s %-22s %-12s %-10s %s",
			marker,
			durStyle.Render(fmt.Sprintf("%-10s", durStr)),
			memCol, q.Node, q.Username, stmt)
		lines = append(lines, row)
	}

	// Detail panel for selected query
	if m.selected < len(visible) {
		lines = append(lines, "")
		q := visible[m.selected]
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

	result := strings.Join(lines, "\n")
	if stale {
		return styleDim.Render(result)
	}
	return result
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

// renderInfoModal shows the operations breakdown plus the full statement for
// the selected query, centered over the dimmed list.
func (m QueriesModel) renderInfoModal() string {
	modalWidth := m.width * 85 / 100
	if modalWidth < 60 {
		modalWidth = 60
	}
	innerWidth := modalWidth - 6
	if innerWidth < 40 {
		innerWidth = 40
	}

	q := m.infoTarget
	now := time.Now()

	header := fmt.Sprintf("Job: %s   User: %s   Duration: %s",
		q.ID, q.Username, formatDuration(now.Sub(q.Started)))

	var sections []string
	sections = append(sections, styleModalTitle.Render("Operation Details"))
	sections = append(sections, header)
	sections = append(sections, "")

	if len(q.Operations) == 0 {
		sections = append(sections, styleDim.Render("No operations recorded yet (job may be in planning phase)."))
	} else {
		sections = append(sections, styleHeader.Render(fmt.Sprintf("%-22s %-18s %-12s %s",
			"NAME", "NODE", "USED", "STARTED")))
		for _, op := range q.Operations {
			offset := op.Started.Sub(q.Started)
			startedStr := "—"
			if !op.Started.IsZero() {
				startedStr = "+" + formatDuration(offset)
			}
			sections = append(sections, fmt.Sprintf("%-22s %-18s %-12s %s",
				truncateString(op.Name, 22),
				truncateString(op.NodeName, 18),
				formatBytes(op.UsedBytes),
				startedStr))
		}
		sections = append(sections, "")
		sections = append(sections, styleDim.Render(fmt.Sprintf("Total: %s across %d operation(s)",
			formatBytes(q.UsedBytes), len(q.Operations))))
	}

	sections = append(sections, "")
	sections = append(sections, styleModalTitle.Render("Statement"))
	for _, line := range wrapText(q.Stmt, innerWidth) {
		sections = append(sections, line)
	}

	sections = append(sections, "")
	sections = append(sections, styleDim.Render("[y] yank to clipboard   [i/esc] close"))

	content := lipgloss.JoinVertical(lipgloss.Left, sections...)
	modal := styleModalBorder.Width(innerWidth).Render(content)

	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, modal,
		lipgloss.WithWhitespaceBackground(colorOverlayBg))
}

// formatQueryDump produces the plaintext blob copied to the clipboard via `y`.
// Pure function so the format is easy to lock down in a test.
func formatQueryDump(q cratedb.ActiveQuery) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Job ID:   %s\n", q.ID)
	fmt.Fprintf(&b, "User:     %s\n", q.Username)
	fmt.Fprintf(&b, "Node:     %s\n", q.Node)
	fmt.Fprintf(&b, "Started:  %s\n", q.Started.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "Memory:   %s (sum of %d operation(s))\n", formatBytes(q.UsedBytes), len(q.Operations))
	b.WriteString("\n")

	if len(q.Operations) > 0 {
		b.WriteString("Operations:\n")
		for _, op := range q.Operations {
			offset := "—"
			if !op.Started.IsZero() {
				offset = "+" + formatDuration(op.Started.Sub(q.Started))
			}
			fmt.Fprintf(&b, "  - %s on %s: %s (started %s)\n",
				op.Name, op.NodeName, formatBytes(op.UsedBytes), offset)
		}
		b.WriteString("\n")
	}

	b.WriteString("Statement:\n")
	b.WriteString(q.Stmt)
	if !strings.HasSuffix(q.Stmt, "\n") {
		b.WriteString("\n")
	}
	return b.String()
}

// wrapText breaks s into lines no wider than width, preserving existing
// newlines. Naive whitespace wrapping — good enough for SQL display.
func wrapText(s string, width int) []string {
	if width < 10 {
		width = 10
	}
	var out []string
	for _, line := range strings.Split(s, "\n") {
		for len(line) > width {
			cut := strings.LastIndex(line[:width], " ")
			if cut <= 0 {
				cut = width
			}
			out = append(out, line[:cut])
			line = strings.TrimLeft(line[cut:], " ")
		}
		out = append(out, line)
	}
	return out
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
