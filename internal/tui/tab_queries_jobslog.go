package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// slowKeys identifies the rows of the current board mode, for keeping the
// cursor on the same row across refreshes.
func (m QueriesModel) slowKeys(snap store.StoreSnapshot) []string {
	var keys []string
	switch m.logMode {
	case store.JobsLogFailed:
		for _, e := range snap.JobsLog.Failed {
			keys = append(keys, e.ID)
		}
	case store.JobsLogGrouped:
		for _, g := range snap.JobsLog.Groups {
			keys = append(keys, g.Stmt)
		}
	default:
		for _, o := range snap.SlowestQueries {
			keys = append(keys, o.ID)
		}
	}
	return keys
}

// toggleLogMode switches to mode, or back to the plain board if already there.
func (m QueriesModel) toggleLogMode(mode store.JobsLogMode) QueriesModel {
	if m.logMode == mode {
		mode = store.JobsLogSlowest
	}
	m.logMode = mode
	m.slowSelected = 0
	return m
}

func (m QueriesModel) handleJobsLogKey(msg tea.KeyMsg) (QueriesModel, tea.Cmd) {
	km := m.keyMap
	n := len(m.slowKeys(m.snap))
	switch {
	case key.Matches(msg, km.Up):
		if m.slowSelected > 0 {
			m.slowSelected--
		}
	case key.Matches(msg, km.Down):
		if m.slowSelected < n-1 {
			m.slowSelected++
		}
	case key.Matches(msg, km.Yank):
		if m.slowSelected >= n {
			return m, nil
		}
		var payload string
		if m.logMode == store.JobsLogGrouped {
			payload = formatGroupDump(m.snap.JobsLog.Groups[m.slowSelected])
		} else {
			payload = formatJobLogDump(m.snap.JobsLog.Failed[m.slowSelected])
		}
		return m, func() tea.Msg {
			return YankResultMsg{Error: writeClipboard(payload)}
		}
	}
	return m, nil
}

// sourceLine says where the board's numbers come from and how far to trust
// them.
func (m QueriesModel) sourceLine(now time.Time) string {
	jl := m.snap.JobsLog
	sampled := "sampled every " + formatDuration(m.snap.SampleInterval)
	switch {
	case jl.Usable():
		s := "exact from sys.jobs_log"
		if cov := jl.CoveredSince(); !cov.IsZero() && cov.After(m.snap.ObservedSince) {
			s += fmt.Sprintf("; log reaches back %s, older rows (≥) %s", formatDuration(now.Sub(cov)), sampled)
		}
		return s
	case jl.Err != "":
		return sampled + styleDim.Render(" · jobs_log unavailable: "+truncateString(firstLine(jl.Err), 60))
	default:
		return sampled + styleDim.Render(" · waiting for sys.jobs_log")
	}
}

func (m QueriesModel) renderJobsLog() string {
	now := time.Now()
	jl := m.snap.JobsLog
	failed := m.logMode == store.JobsLogFailed

	var lines []string
	if m.yankResult != "" {
		lines = append(lines, "  "+styleHealthGreen.Render(m.yankResult))
	}
	if failed {
		lines = append(lines, styleTitle.Render("Failed Queries")+styleDim.Render("  (f: back  g: grouped  S: live)"))
	} else {
		lines = append(lines, styleTitle.Render("Slowest Statements")+styleDim.Render("  (g: back  f: failed  S: live)"))
	}

	info := fmt.Sprintf("  sys.jobs_log since %s", m.snap.ObservedSince.Format("15:04:05"))
	if failed {
		info += ", newest first"
	} else {
		info += ", grouped by exact statement"
	}
	if cov := jl.CoveredSince(); !cov.IsZero() && cov.After(m.snap.ObservedSince) {
		info += styleDim.Render(fmt.Sprintf(" (log reaches back %s)", formatDuration(now.Sub(cov))))
	}
	lines = append(lines, info)

	if !jl.Usable() {
		reason := "waiting for first poll"
		if jl.Err != "" {
			reason = truncateString(firstLine(jl.Err), max(m.width-30, 30))
		}
		lines = append(lines, "  jobs_log unavailable: "+reason)
		return strings.Join(lines, "\n")
	}
	if len(m.slowKeys(m.snap)) == 0 {
		if failed {
			lines = append(lines, "  No failed queries")
		} else {
			lines = append(lines, "  No queries logged yet")
		}
		return strings.Join(lines, "\n")
	}
	lines = append(lines, "")

	if failed {
		lines = append(lines, m.failedRows()...)
	} else {
		lines = append(lines, m.groupRows()...)
	}
	return strings.Join(lines, "\n")
}

func (m QueriesModel) failedRows() []string {
	rows := m.snap.JobsLog.Failed
	lines := []string{styleHeader.Render(fmt.Sprintf("  %-3s %-9s %-10s %-12s %-10s %-30s %s",
		"#", "ENDED", "DURATION", "NODE", "USER", "ERROR", "STATEMENT"))}
	maxStmtLen := max(m.width-83, 20)
	for i, e := range rows {
		marker := "  "
		if i == m.slowSelected {
			marker = "▸ "
		}
		lines = append(lines, fmt.Sprintf("%s%-3d %-9s %-10s %-12s %-10s %s %s",
			marker, i+1, e.Ended.Format("15:04:05"),
			formatDuration(e.Ended.Sub(e.Started)),
			truncateString(e.Node, 12), truncateString(e.Username, 10),
			styleHealthRed.Render(fmt.Sprintf("%-30s", truncateString(firstLine(e.Error), 30))),
			truncateString(strings.ReplaceAll(e.Stmt, "\n", " "), maxStmtLen)))
	}

	if m.slowSelected < len(rows) {
		e := rows[m.slowSelected]
		lines = append(lines, "", styleTitle.Render("  Query Detail"),
			fmt.Sprintf("    ID:       %s", e.ID),
			fmt.Sprintf("    Node:     %s", e.Node),
			fmt.Sprintf("    User:     %s", e.Username),
			fmt.Sprintf("    Type:     %s", e.Type),
			fmt.Sprintf("    Started:  %s", e.Started.Format("15:04:05")),
			fmt.Sprintf("    Ended:    %s", e.Ended.Format("15:04:05")),
			fmt.Sprintf("    Duration: %s", formatDuration(e.Ended.Sub(e.Started))),
			"", "    Error:")
		for _, line := range wrapText(e.Error, max(m.width-8, 20)) {
			lines = append(lines, "      "+styleHealthRed.Render(line))
		}
		lines = append(lines, "", "    Statement:")
		for _, line := range strings.Split(e.Stmt, "\n") {
			lines = append(lines, "      "+line)
		}
	}
	return lines
}

func (m QueriesModel) groupRows() []string {
	groups := m.snap.JobsLog.Groups
	lines := []string{styleHeader.Render(fmt.Sprintf("  %-3s %-10s %-10s %-7s %-7s %s",
		"#", "MAX", "AVG", "COUNT", "FAILED", "STATEMENT"))}
	maxStmtLen := max(m.width-45, 20)
	for i, g := range groups {
		marker := "  "
		if i == m.slowSelected {
			marker = "▸ "
		}
		failed := fmt.Sprintf("%-7d", g.Failed)
		if g.Failed > 0 {
			failed = styleHealthRed.Render(failed)
		}
		lines = append(lines, fmt.Sprintf("%s%-3d %-10s %-10s %-7d %s %s",
			marker, i+1, formatDuration(g.Max), formatDuration(g.Avg), g.Count, failed,
			truncateString(strings.ReplaceAll(g.Stmt, "\n", " "), maxStmtLen)))
	}

	if m.slowSelected < len(groups) {
		g := groups[m.slowSelected]
		lines = append(lines, "", styleTitle.Render("  Statement Detail"),
			fmt.Sprintf("    Runs:       %d (%d failed)", g.Count, g.Failed),
			fmt.Sprintf("    Max / avg:  %s / %s", formatDuration(g.Max), formatDuration(g.Avg)),
			fmt.Sprintf("    Last ended: %s", g.LastEnded.Format("15:04:05")),
			"", "    Statement:")
		for _, line := range strings.Split(g.Stmt, "\n") {
			lines = append(lines, "      "+line)
		}
	}
	return lines
}

func formatJobLogDump(e cratedb.JobLogEntry) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Job ID:   %s\n", e.ID)
	fmt.Fprintf(&b, "User:     %s\n", e.Username)
	fmt.Fprintf(&b, "Node:     %s\n", e.Node)
	fmt.Fprintf(&b, "Started:  %s\n", e.Started.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "Ended:    %s\n", e.Ended.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "Duration: %s\n", formatDuration(e.Ended.Sub(e.Started)))
	if e.Error != "" {
		fmt.Fprintf(&b, "Error:    %s\n", e.Error)
	}
	b.WriteString("\nStatement:\n")
	b.WriteString(strings.TrimRight(e.Stmt, "\n"))
	b.WriteString("\n")
	return b.String()
}

func formatGroupDump(g cratedb.JobLogGroup) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Runs:       %d (%d failed)\n", g.Count, g.Failed)
	fmt.Fprintf(&b, "Max / avg:  %s / %s\n", formatDuration(g.Max), formatDuration(g.Avg))
	fmt.Fprintf(&b, "Last ended: %s\n", g.LastEnded.UTC().Format(time.RFC3339))
	b.WriteString("\nStatement:\n")
	b.WriteString(strings.TrimRight(g.Stmt, "\n"))
	b.WriteString("\n")
	return b.String()
}
