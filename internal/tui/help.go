package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/lipgloss"
)

type helpEntry struct {
	keys string
	desc string
}

func bindingHelp(b key.Binding) helpEntry {
	h := b.Help()
	return helpEntry{h.Key, h.Desc}
}

// tabHelp lists the keys a tab handles itself. Labels come from the keymap
// where a binding exists; the rest are keys tabs match inline (f on Tables,
// enter in SQL and the settings editor). slowest selects the Queries board
// variant, where K and h do nothing.
func tabHelp(tab Tab, km KeyMap, slowest bool) (string, []helpEntry) {
	nav := helpEntry{km.Up.Help().Key + " " + km.Down.Help().Key, "navigate"}
	search := []helpEntry{
		bindingHelp(km.Search),
		bindingHelp(km.SortNext),
		{km.Escape.Help().Key, "clear search"},
	}

	switch tab {
	case TabOverview:
		return "Overview", []helpEntry{
			{nav.keys, "scroll"},
			bindingHelp(km.Edit),
			{nav.keys, "editor: move between settings"},
			{"enter", "editor: edit / apply value"},
			{km.Escape.Help().Key, "editor: cancel input / leave"},
		}
	case TabNodes:
		return "Nodes", append([]helpEntry{
			nav,
			{km.DetailDown.Help().Key + " " + km.DetailUp.Help().Key, "scroll detail panel"},
		}, search...)
	case TabQueries:
		if slowest {
			return "Queries (slowest)", []helpEntry{
				nav,
				bindingHelp(km.Info),
				bindingHelp(km.Yank),
				bindingHelp(km.Failed),
				bindingHelp(km.Grouped),
				bindingHelp(km.Slowest),
			}
		}
		return "Queries (live)", []helpEntry{
			nav,
			bindingHelp(km.Kill),
			bindingHelp(km.Info),
			bindingHelp(km.Yank),
			bindingHelp(km.Hide),
			bindingHelp(km.Slowest),
			bindingHelp(km.Failed),
			bindingHelp(km.Grouped),
		}
	case TabTables:
		return "Tables", append([]helpEntry{nav, {"f", "unhealthy tables only"}}, search...)
	case TabShards:
		return "Shards", append([]helpEntry{nav}, search...)
	case TabSQL:
		return "SQL", []helpEntry{
			{"enter", "run query"},
			{"↑ ↓", "history (editor) / scroll (results)"},
			{km.Escape.Help().Key, "clear input, then results (global keys work there)"},
			{"enter /", "results: back to editor"},
		}
	}
	return "", nil
}

func globalHelp(km KeyMap) []helpEntry {
	return []helpEntry{
		{"1-6", "switch tab"},
		{km.NextTab.Help().Key + " " + km.PrevTab.Help().Key, "next / prev tab"},
		bindingHelp(km.Refresh),
		bindingHelp(km.Throttle),
		bindingHelp(km.Reconnect),
		bindingHelp(km.QueryLog),
		{"? F1", "toggle this help (F1 also while typing)"},
		bindingHelp(km.Quit),
	}
}

// renderHelp draws the key list for the active tab as a centered modal.
func renderHelp(tab Tab, km KeyMap, slowest bool, width, height int) string {
	title, local := tabHelp(tab, km, slowest)
	global := globalHelp(km)

	keyWidth := 0
	for _, e := range append(local, global...) {
		keyWidth = max(keyWidth, lipgloss.Width(e.keys))
	}
	rows := func(entries []helpEntry) []string {
		out := make([]string, 0, len(entries))
		for _, e := range entries {
			pad := strings.Repeat(" ", keyWidth-lipgloss.Width(e.keys))
			out = append(out, fmt.Sprintf("%s%s   %s", styleValue.Render(e.keys), pad, e.desc))
		}
		return out
	}

	sections := []string{styleModalTitle.Render("Keys: " + title)}
	sections = append(sections, rows(local)...)
	sections = append(sections, "", styleModalTitle.Render("Global"))
	sections = append(sections, rows(global)...)
	sections = append(sections, "", styleDim.Render("[?/esc] close"))

	content := lipgloss.JoinVertical(lipgloss.Left, sections...)
	modal := styleModalBorder.Render(content)
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, modal,
		lipgloss.WithWhitespaceBackground(colorOverlayBg))
}
