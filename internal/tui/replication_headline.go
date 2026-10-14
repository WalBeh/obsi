package tui

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// styleColumns heads the grouped tables on the Replication and Snapshots
// tabs: bold and dim, no rule under it, so the rows carry the weight.
var styleColumns = lipgloss.NewStyle().Bold(true).Foreground(colorDim)

// styleSpark colours the braille sparklines, so they read as a line of
// their own next to the numbers.
var styleSpark = lipgloss.NewStyle().Foreground(colorPrimary)

// subStateMark is a subscribed table's state as a mark and a word; the
// marks differ in shape, not only in colour.
func subStateMark(state string) string {
	switch state {
	case "r":
		return styleHealthGreen.Render("● in sync")
	case "i":
		return styleHealthYellow.Render("◐ preparing")
	case "d":
		return styleHealthYellow.Render("◐ copying")
	case "e":
		return styleHealthRed.Render("✕ FAILED")
	}
	return state
}

// stateCounts sums table states as marks, worst first: "✕ 1 FAILED ·
// ● 3 in sync".
func stateCounts(counts map[string]int) string {
	var parts []string
	for _, s := range []struct{ key, word string }{{"e", "FAILED"}, {"d", "copying"}, {"i", "preparing"}, {"r", "in sync"}} {
		n := counts[s.key]
		if n == 0 {
			continue
		}
		style := styleHealthGreen
		switch s.key {
		case "e":
			style = styleHealthRed
		case "i", "d":
			style = styleHealthYellow
		}
		parts = append(parts, style.Render(fmt.Sprintf("%s %d %s", markOf(s.key), n, s.word)))
	}
	return strings.Join(parts, " · ")
}

func markOf(state string) string {
	switch state {
	case "r":
		return "●"
	case "e":
		return "✕"
	}
	return "◐"
}

// headline answers "is it fine?" in one line per side, so the rows below
// are detail.
func (m ReplicationModel) headline() []string {
	r := m.repl
	var lines []string
	if len(r.Subscriptions) > 0 {
		counts, tables := map[string]int{}, 0
		var last []string
		for _, s := range r.Subscriptions {
			for _, t := range s.Tables {
				counts[t.State]++
				tables++
			}
		}
		var newest time.Time
		for _, rt := range r.Replicas {
			if rt.LastChange.After(newest) {
				newest = rt.LastChange
			}
		}
		state := stateCounts(counts)
		if counts["e"]+counts["d"]+counts["i"] == 0 && tables > 0 {
			state = styleHealthGreen.Render("● all in sync")
		}
		head := fmt.Sprintf("%s %s %d %s  %s",
			styleValue.Render(fmt.Sprintf("Pulling %d %s", tables, plural(tables, "table", "tables"))),
			styleDim.Render("from"), len(r.Subscriptions), plural(len(r.Subscriptions), "subscription", "subscriptions"), state)
		if !newest.IsZero() {
			last = append(last, styleDim.Render("last change "+formatAgo(m.now.Sub(newest))))
		} else {
			last = append(last, styleDim.Render("no change seen yet"))
		}
		last = append(last, m.settingsSummary(), m.poolSummary())
		lines = append(lines, "  "+head+styleDim.Render("  · ")+strings.Join(slices.DeleteFunc(last, func(s string) bool { return s == "" }), styleDim.Render(" · ")))
	}
	if len(r.Publications) > 0 || len(r.Subscribers) > 0 {
		var behind int64
		unknown, stale := false, 0
		for _, h := range r.Subscribers {
			behind += h.Behind
			unknown = unknown || h.Unknown > 0
			if h.Period > 0 && m.now.After(h.Expires.Add(-h.Period/2)) {
				stale++
			}
		}
		head := styleValue.Render(fmt.Sprintf("Serving %d %s", len(r.Publications), plural(len(r.Publications), "publication", "publications")))
		switch n := len(r.Subscribers); {
		case n == 0:
			head += styleDim.Render("  · no subscriber pulls yet")
		default:
			head += fmt.Sprintf(" %s %d %s  ", styleDim.Render("to"), n, plural(n, "subscriber", "subscribers"))
			switch {
			case behind == 0 && !unknown:
				head += styleHealthGreen.Render("● 0 behind")
			case behind == 0:
				head += styleDim.Render("? behind unknown")
			default:
				head += styleHealthYellow.Render("▲ " + formatRecords(behind) + " behind")
			}
			if stale > 0 {
				head += "  " + styleHealthYellow.Render(fmt.Sprintf("▲ %d stopped pulling", stale))
			}
		}
		if len(r.Subscriptions) == 0 {
			if p := m.poolSummary(); p != "" {
				head += styleDim.Render("  · ") + p
			}
		}
		lines = append(lines, "  "+head)
	}
	return lines
}

// keyLine is the few keys that matter on this side, plus the way to all
// of them, and whose replication the tab shows.
func (m ReplicationModel) keyLine() string {
	var keys []string
	if len(m.repl.Subscriptions) > 0 || len(m.repl.Publications) == 0 {
		keys = append(keys, "s subscribe")
	}
	if len(m.repl.Publications) > 0 || len(m.repl.Subscriptions) == 0 {
		keys = append(keys, "n publish", "e tables", "y ticket")
	}
	keys = append(keys, "d drop", "c settings", "? all keys")
	line := "  " + strings.Join(keys, " · ")
	if m.repl.User != "" {
		owner := "showing what " + m.repl.User + " owns"
		if gap := m.width - lipgloss.Width(line) - len(owner) - 2; gap > 2 {
			line += strings.Repeat(" ", gap) + owner
		} else {
			line += " · " + owner
		}
	}
	return styleDim.Render(line)
}
