package tui

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

const (
	sqlMaxColWidth   = 40
	sqlHistorySize   = 50
)

// SQLResultMsg carries the result of an async query back to the TUI.
type SQLResultMsg struct {
	Cols     []string
	Rows     [][]string
	RowCount int64
	Duration float64
	Error    string
}

// SQLModel provides an interactive SQL query tab.
type SQLModel struct {
	registry *cratedb.Registry
	ctx      context.Context

	// Input
	input   string
	editing bool // true when cursor is in the input field

	// History
	history    []string
	historyIdx int // -1 = current input, 0..n = history entries

	// Results
	limitApplied bool
	cols         []string
	rows     [][]string
	rowCount int64
	duration float64
	errMsg   string
	running  bool

	// Column widths (computed from results)
	colWidths []int

	// Scroll
	scroll int

	width  int
	height int
}

func NewSQLModel(width, height int, registry *cratedb.Registry, ctx context.Context) SQLModel {
	return SQLModel{
		registry:   registry,
		ctx:        ctx,
		editing:    true,
		historyIdx: -1,
		width:      width,
		height:     height,
	}
}

func (m SQLModel) SetSize(width, height int) SQLModel {
	m.width = width
	m.height = height
	m.clampScroll()
	return m
}

func (m SQLModel) resultHeight() int {
	headerLines := 4 // title + input + blank + column header
	if m.errMsg != "" {
		headerLines += 1
	}
	h := m.height - headerLines
	if h < 3 {
		h = 3
	}
	return h
}

func (m *SQLModel) clampScroll() {
	resultH := m.resultHeight()
	maxScroll := len(m.rows) - resultH
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.scroll > maxScroll {
		m.scroll = maxScroll
	}
	if m.scroll < 0 {
		m.scroll = 0
	}
}

func (m SQLModel) IsEditing() bool {
	return m.editing
}

func (m SQLModel) HandleKey(msg tea.KeyMsg) (SQLModel, tea.Cmd) {
	km := DefaultKeyMap()

	if m.editing {
		switch {
		case msg.Type == tea.KeyEnter:
			stmt := strings.TrimSpace(m.input)
			stmt = strings.TrimRight(stmt, ";")
			if stmt == "" {
				return m, nil
			}
			// Add to history
			if len(m.history) == 0 || m.history[0] != stmt {
				m.history = append([]string{stmt}, m.history...)
				if len(m.history) > sqlHistorySize {
					m.history = m.history[:sqlHistorySize]
				}
			}
			m.historyIdx = -1
			m.running = true
			m.errMsg = ""
			m.rows = nil
			m.cols = nil
			m.scroll = 0
			m.limitApplied = false

			exec := stmt
			if isSelect(stmt) && !hasLimit(stmt) {
				exec = stmt + " LIMIT 100"
				m.limitApplied = true
			}
			return m, m.executeQuery(exec)

		case msg.Type == tea.KeyBackspace:
			if len(m.input) > 0 {
				m.input = m.input[:len(m.input)-1]
			}
			return m, nil

		case msg.Type == tea.KeyUp:
			// History navigation
			if len(m.history) > 0 && m.historyIdx < len(m.history)-1 {
				m.historyIdx++
				m.input = m.history[m.historyIdx]
			}
			return m, nil

		case msg.Type == tea.KeyDown:
			if m.historyIdx > 0 {
				m.historyIdx--
				m.input = m.history[m.historyIdx]
			} else if m.historyIdx == 0 {
				m.historyIdx = -1
				m.input = ""
			}
			return m, nil

		case key.Matches(msg, km.Escape):
			if m.input != "" {
				m.input = ""
				m.historyIdx = -1
			} else if len(m.rows) > 0 || m.errMsg != "" {
				// Switch to result scrolling mode
				m.editing = false
			}
			return m, nil

		case msg.Type == tea.KeySpace:
			m.input += " "
			m.historyIdx = -1
			return m, nil

		case msg.Type == tea.KeyRunes:
			m.input += string(msg.Runes)
			m.historyIdx = -1
			return m, nil
		}
		return m, nil
	}

	// Result scrolling mode
	switch {
	case key.Matches(msg, km.Up):
		if m.scroll > 0 {
			m.scroll--
		}
	case key.Matches(msg, km.Down):
		m.scroll++
		m.clampScroll()
	case key.Matches(msg, km.Search), key.Matches(msg, km.Escape):
		// Back to editing
		m.editing = true
	case msg.Type == tea.KeyEnter:
		m.editing = true
	}
	return m, nil
}

func (m SQLModel) executeQuery(stmt string) tea.Cmd {
	registry := m.registry
	ctx := m.ctx
	return func() tea.Msg {
		resp, err := registry.Query(ctx, stmt)
		if err != nil {
			return SQLResultMsg{Error: err.Error()}
		}

		// Convert all values to strings
		rows := make([][]string, 0, len(resp.Rows))
		for _, row := range resp.Rows {
			srow := make([]string, len(row))
			for i, v := range row {
				srow[i] = formatSQLValue(v)
			}
			rows = append(rows, srow)
		}

		return SQLResultMsg{
			Cols:     resp.Cols,
			Rows:     rows,
			RowCount: resp.RowCount,
			Duration: resp.Duration,
		}
	}
}

func (m SQLModel) HandleResult(msg SQLResultMsg) SQLModel {
	m.running = false
	if msg.Error != "" {
		m.errMsg = msg.Error
		return m
	}
	m.errMsg = ""
	m.cols = msg.Cols
	m.rows = msg.Rows
	m.rowCount = msg.RowCount
	m.duration = msg.Duration
	m.scroll = 0

	// Compute column widths
	m.colWidths = make([]int, len(m.cols))
	for i, c := range m.cols {
		m.colWidths[i] = utf8.RuneCountInString(c)
	}
	for _, row := range m.rows {
		for i, v := range row {
			w := utf8.RuneCountInString(v)
			if w > m.colWidths[i] {
				m.colWidths[i] = w
			}
		}
	}
	// Cap column widths
	for i := range m.colWidths {
		if m.colWidths[i] > sqlMaxColWidth {
			m.colWidths[i] = sqlMaxColWidth
		}
	}

	m.clampScroll()
	return m
}

func (m SQLModel) View() string {
	var lines []string

	title := sectionTitle("SQL")
	lines = append(lines, title)

	// Input line
	prompt := "  SQL> "
	cursor := "▏"
	if !m.editing {
		cursor = ""
	}
	inputLine := fmt.Sprintf("%s%s%s", styleDim.Render(prompt), m.input, cursor)
	if m.running {
		inputLine += styleDim.Render("  (running...)")
	}
	lines = append(lines, inputLine)

	// Hints
	if m.editing {
		lines = append(lines, styleDim.Render("  ↑/↓:history  enter:execute  esc:scroll results"))
	} else {
		lines = append(lines, styleDim.Render("  ↑/↓:scroll  esc:back to input  /:back to input"))
	}

	// Error
	if m.errMsg != "" {
		lines = append(lines, "")
		lines = append(lines, styleHighValue.Render("  Error: "+m.errMsg))
		return strings.Join(lines, "\n")
	}

	// No results yet
	if m.cols == nil {
		return strings.Join(lines, "\n")
	}

	// Summary line
	lines = append(lines, "")
	summary := fmt.Sprintf("  %d rows (%.1fms)", m.rowCount, m.duration)
	if m.limitApplied {
		summary += "  " + styleHealthYellow.Render("(LIMIT 100 applied)")
	}
	lines = append(lines, styleDim.Render(summary))

	// No rows returned (DML or empty result)
	if len(m.rows) == 0 {
		return strings.Join(lines, "\n")
	}

	// Column header
	header := m.renderRow(m.cols)
	lines = append(lines, styleHeader.Render("  "+header))

	// Result rows with scrolling
	resultH := m.resultHeight()
	visibleEnd := m.scroll + resultH
	if visibleEnd > len(m.rows) {
		visibleEnd = len(m.rows)
	}

	if m.scroll > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↑ %d more above", m.scroll)))
	}

	for i := m.scroll; i < visibleEnd; i++ {
		lines = append(lines, "  "+m.renderRow(m.rows[i]))
	}

	remaining := len(m.rows) - visibleEnd
	if remaining > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↓ %d more below", remaining)))
	}

	return strings.Join(lines, "\n")
}

func (m SQLModel) renderRow(values []string) string {
	parts := make([]string, len(values))
	for i, v := range values {
		w := sqlMaxColWidth
		if i < len(m.colWidths) {
			w = m.colWidths[i]
		}
		if utf8.RuneCountInString(v) > w {
			// Truncate with ellipsis
			runes := []rune(v)
			v = string(runes[:w-1]) + "…"
		}
		parts[i] = fmt.Sprintf("%-*s", w, v)
	}
	return strings.Join(parts, "  ")
}

func isSelect(stmt string) bool {
	s := strings.TrimSpace(stmt)
	return len(s) >= 6 && strings.EqualFold(s[:6], "SELECT")
}

func hasLimit(stmt string) bool {
	upper := strings.ToUpper(stmt)
	return strings.Contains(upper, " LIMIT ")
}

func formatSQLValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case []interface{}:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = formatSQLValue(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]interface{}:
		parts := make([]string, 0, len(val))
		for k, item := range val {
			parts = append(parts, k+"="+formatSQLValue(item))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprintf("%v", val)
	}
}

