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

// stmtCell is a one-line, truncated statement for a list column. Config
// changes stand out in their own colour.
func stmtCell(stmt string, width int) string {
	s := truncateString(strings.ReplaceAll(stmt, "\n", " "), width)
	if cratedb.ClassifyChange(stmt) != "" {
		return styleChange.Render(s)
	}
	return s
}

func changeKey(c store.Change) string {
	if c.ID != "" {
		return c.ID
	}
	key := "unseen " + c.Ended.String()
	for _, d := range c.Diffs {
		key += " " + d.Key
	}
	return key
}

// anchorChange keeps the cursor on the same change as new ones arrive on
// top.
func anchorChange(old, cur []store.Change, sel int) int {
	if sel >= len(old) {
		return 0
	}
	want := changeKey(old[sel])
	for i, c := range cur {
		if changeKey(c) == want {
			return i
		}
	}
	return min(sel, max(len(cur)-1, 0))
}

func (m QueriesModel) handleChangesKey(msg tea.KeyMsg) (QueriesModel, tea.Cmd) {
	km := m.keyMap
	changes := m.snap.Changes.Changes
	switch {
	case key.Matches(msg, km.Up):
		if m.chgSelected > 0 {
			m.chgSelected--
		}
	case key.Matches(msg, km.Down):
		if m.chgSelected < len(changes)-1 {
			m.chgSelected++
		}
	case key.Matches(msg, km.Yank):
		if m.chgSelected < len(changes) {
			payload := formatChangeDump(changes[m.chgSelected])
			return m, func() tea.Msg {
				return YankResultMsg{Error: writeClipboard(payload)}
			}
		}
	case msg.String() == "esc":
		m.showChanges = false
	}
	return m, nil
}

// changesDetailLines is roughly what the detail panel takes, so the list
// gets the rest of the screen.
const changesDetailLines = 14

func (m QueriesModel) renderChanges() string {
	cs := m.snap.Changes
	now := time.Now()

	var lines []string
	if m.yankResult != "" {
		lines = append(lines, "  "+styleHealthGreen.Render(m.yankResult))
	}
	lines = append(lines, styleTitle.Render("Config Changes")+styleDim.Render("  (c: back  S: slowest  f: failed)"))

	info := fmt.Sprintf("  from sys.jobs_log since obsi started at %s", m.snap.ObservedSince.Format("15:04:05"))
	if !cs.LogStart.IsZero() && cs.LogStart.Before(m.snap.ObservedSince) {
		info += fmt.Sprintf(", and before that back to %s", cs.LogStart.Format("15:04:05"))
	}
	lines = append(lines, info+styleDim.Render(" · cluster, table settings, users and privileges"))

	if cs.Err != "" {
		lines = append(lines, "  "+styleHealthRed.Render("jobs_log unavailable: "+truncateString(firstLine(cs.Err), max(m.width-30, 30))))
	}
	for i, g := range cs.Gaps {
		if i == 3 {
			lines = append(lines, styleDim.Render(fmt.Sprintf("  (%d more gaps)", len(cs.Gaps)-3)))
			break
		}
		g = cs.Gaps[len(cs.Gaps)-1-i]
		lines = append(lines, "  "+styleHealthYellow.Render(fmt.Sprintf(
			"possible gap on %s %s–%s: the log rotated between polls, changes in it are missing",
			g.Node, g.From.Format("15:04:05"), g.To.Format("15:04:05"))))
	}

	changes := cs.Changes
	if len(changes) == 0 {
		switch {
		case cs.UpdatedAt.IsZero():
			lines = append(lines, "  waiting for the first poll")
		case cs.Err == "":
			lines = append(lines, "  No changes in the log")
		}
		return strings.Join(lines, "\n")
	}
	lines = append(lines, "")
	lines = append(lines, styleHeader.Render(fmt.Sprintf("  %-9s %-9s %-12s %-12s %s",
		"ENDED", "KIND", "USER", "NODE", "CHANGE")))

	room := max(m.height-len(lines)-changesDetailLines, 5)
	start := min(max(m.chgSelected-room/2, 0), max(len(changes)-room, 0))
	end := min(start+room, len(changes))
	if start > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↑ %d newer", start)))
	}
	maxLen := max(m.width-50, 20)
	for i := start; i < end; i++ {
		c := changes[i]
		if i > 0 && !c.Ended.After(m.snap.ObservedSince) && changes[i-1].Ended.After(m.snap.ObservedSince) {
			lines = append(lines, styleDim.Render("  ── obsi started "+m.snap.ObservedSince.Format("15:04:05")+" ──"))
		}
		marker := "  "
		if i == m.chgSelected {
			marker = "▸ "
		}
		user := c.Username
		if c.Obsi {
			user = "obsi"
		}
		lines = append(lines, fmt.Sprintf("%s%-9s %-9s %-12s %-12s %s",
			marker, c.Ended.Format("15:04:05"), c.Kind,
			truncateString(user, 12), truncateString(c.Node, 12), changeCell(c, maxLen)))
	}
	if end < len(changes) {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↓ %d older", len(changes)-end)))
	}

	if m.chgSelected < len(changes) {
		lines = append(lines, m.changeDetail(changes[m.chgSelected], now)...)
	}
	return strings.Join(lines, "\n")
}

func changeCell(c store.Change, width int) string {
	switch {
	case c.Unseen:
		d := c.Diffs[0]
		return styleDim.Render(truncateString(fmt.Sprintf("%s: %s → %s (no statement in the log)", d.Key, settingValue(d.Old), settingValue(d.New)), width))
	case c.Error != "":
		return styleHealthRed.Render("failed ") + truncateString(strings.ReplaceAll(c.Stmt, "\n", " "), width-7)
	}
	suffix := ""
	switch len(c.Diffs) {
	case 0:
	case 1:
		suffix = fmt.Sprintf("  [%s → %s]", settingValue(c.Diffs[0].Old), settingValue(c.Diffs[0].New))
	default:
		suffix = fmt.Sprintf("  [%d settings]", len(c.Diffs))
	}
	stmt := truncateString(strings.ReplaceAll(c.Stmt, "\n", " "), max(width-len([]rune(suffix)), 10))
	return styleChange.Render(stmt) + styleDim.Render(suffix)
}

func settingValue(v string) string {
	if v == "" {
		return "unset"
	}
	return v
}

func (m QueriesModel) changeDetail(c store.Change, now time.Time) []string {
	lines := []string{"", styleTitle.Render("  Change Detail")}
	if !c.Unseen {
		user := c.Username
		if c.Obsi {
			user += " (run by obsi)"
		}
		lines = append(lines,
			fmt.Sprintf("    ID:     %s", c.ID),
			fmt.Sprintf("    Node:   %s", c.Node),
			fmt.Sprintf("    User:   %s", user))
	}
	lines = append(lines, fmt.Sprintf("    Ended:  %s (%s ago)", c.Ended.Format("15:04:05"), formatDuration(now.Sub(c.Ended))))
	if c.Unseen {
		lines = append(lines, "    "+styleDim.Render("Seen in sys.cluster between two polls; the statement isn't in sys.jobs_log (rotated out, or stats were off)."))
	}
	if c.Error != "" {
		lines = append(lines, "    Error:")
		for _, line := range wrapText(c.Error, max(m.width-8, 20)) {
			lines = append(lines, "      "+styleHealthRed.Render(line))
		}
	}
	if len(c.Diffs) > 0 {
		lines = append(lines, "    Settings:")
		for _, d := range c.Diffs {
			lines = append(lines, fmt.Sprintf("      %s: %s → %s", d.Key, settingValue(d.Old), styleValue.Render(settingValue(d.New))))
		}
	}
	if c.Stmt != "" {
		lines = append(lines, "    Statement:")
		for _, line := range strings.Split(c.Stmt, "\n") {
			lines = append(lines, "      "+line)
		}
	}
	return lines
}

func formatChangeDump(c store.Change) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Ended:    %s\n", c.Ended.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "Kind:     %s\n", c.Kind)
	if !c.Unseen {
		fmt.Fprintf(&b, "Job ID:   %s\n", c.ID)
		fmt.Fprintf(&b, "User:     %s\n", c.Username)
		fmt.Fprintf(&b, "Node:     %s\n", c.Node)
	}
	if c.Error != "" {
		fmt.Fprintf(&b, "Error:    %s\n", c.Error)
	}
	for _, d := range c.Diffs {
		fmt.Fprintf(&b, "Setting:  %s: %s -> %s\n", d.Key, settingValue(d.Old), settingValue(d.New))
	}
	if c.Stmt != "" {
		b.WriteString("\nStatement:\n")
		b.WriteString(strings.TrimRight(c.Stmt, "\n"))
		b.WriteString("\n")
	}
	return b.String()
}
