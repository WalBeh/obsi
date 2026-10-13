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
// enter in SQL and the settings editor). view selects the Queries variant;
// K and h only work on the live list.
func tabHelp(tab Tab, km KeyMap, view queriesView) (string, []helpEntry) {
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
		switch view {
		case queriesChanges:
			return "Queries (config changes)", []helpEntry{
				nav,
				{km.Yank.Help().Key, "yank change to clipboard"},
				{km.Changes.Help().Key, "back"},
				bindingHelp(km.Slowest),
				bindingHelp(km.Failed),
			}
		case queriesSlowest:
			return "Queries (slowest)", []helpEntry{
				nav,
				bindingHelp(km.Info),
				bindingHelp(km.Yank),
				bindingHelp(km.Explain),
				bindingHelp(km.Failed),
				bindingHelp(km.Grouped),
				bindingHelp(km.Slowest),
				bindingHelp(km.Changes),
			}
		}
		return "Queries (live)", []helpEntry{
			nav,
			bindingHelp(km.Kill),
			bindingHelp(km.Info),
			bindingHelp(km.Yank),
			bindingHelp(km.Explain),
			bindingHelp(km.Hide),
			bindingHelp(km.Slowest),
			bindingHelp(km.Failed),
			bindingHelp(km.Grouped),
			bindingHelp(km.Changes),
		}
	case TabTables:
		return "Tables", append([]helpEntry{nav, {"f", "unhealthy tables only"},
			{km.Edit.Help().Key, "node-left delay of the selected table"},
			{km.Yank.Help().Key, "copy SHOW CREATE TABLE of the selected table"},
			{"enter", "partitions of the selected table (s sort, y copy, esc back)"}}, search...)
	case TabShards:
		return "Shards", append([]helpEntry{nav}, append(search,
			bindingHelp(km.Recovery),
			helpEntry{km.Edit.Help().Key, "recovery view: edit the recovery throttle (also in read-only)"},
			helpEntry{km.Yank.Help().Key, "copy the suggested fix"},
			bindingHelp(km.RunFix))...)
	case TabReplication:
		return "Replication", []helpEntry{
			{"↑ ↓", "select a subscription or publication (scroll without any)"},
			{"s", "new subscription: paste a subscribe ticket or fill in host, mode (pg_tunnel or sniff), user, password, publications"},
			{"pgup pgdn", "scroll"},
			{"n", "new publication: name, tables, review with who can read them and lease periods"},
			{km.Edit.Help().Key, "change the tables of the selected publication"},
			{"d", "drop the selected publication, or the selected subscription after typing its name (its tables become writable)"},
			{"l", "retention lease period of a table of the selected publication"},
			{"c", "replication settings: ops_batch_size, reads_poll_duration, chunk_size, max_concurrent_file_chunks, node_initial_primaries_recoveries"},
			{km.Yank.Help().Key, "copy a subscribe ticket: CREATE SUBSCRIPTION to fill in, tables, publisher nodes and ports (also in read-only)"},
		}
	case TabSnapshots:
		return "Snapshots", []helpEntry{
			{"↑ ↓", "select a repository or snapshot; it opens with its details (scroll past either end)"},
			{"enter", "on a repository: list only its snapshots"},
			{"/", "filter snapshots by repository.name (esc clears)"},
			{"pgup pgdn", "scroll"},
			{km.Refresh.Help().Key, "list snapshots now (reads every repository)"},
		}
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

// tabNotes explains what a tab shows where the tab itself has no room.
func tabNotes(tab Tab) []string {
	switch tab {
	case TabSnapshots:
		return []string{
			"Listing snapshots (sys.snapshots) reads every repository: its index and two files per snapshot, whatever the query asks for. obsi lists them every 15 minutes by default ([collectors.snapshots] interval), every minute while one runs, when a repository changes, and on R. Repositories and running restores are cluster state, read every minute.",
			"A repository CrateDB can't read lists no snapshots and gives no error.",
		}
	case TabReplication:
	default:
		return nil
	}
	return []string{
		"Subscriber side: how far behind the publisher a table is shows only on the publisher; here it's changes/s and when obsi last saw one arrive.",
		"Publisher side: subscribers are the cluster names from the retention leases they hold on published primaries; BEHIND is the operations those leases haven't reached. A lease that expires lets the publisher drop operations the subscriber hasn't pulled yet.",
		"Settings (c) are cluster settings read where the subscription runs, the subscriber.",
	}
}

func globalHelp(km KeyMap) []helpEntry {
	return []helpEntry{
		{"1-8", "switch tab"},
		{km.NextTab.Help().Key + " " + km.PrevTab.Help().Key, "next / prev tab"},
		bindingHelp(km.Refresh),
		bindingHelp(km.Throttle),
		bindingHelp(km.Reconnect),
		bindingHelp(km.QueryLog),
		bindingHelp(km.Alerts),
		{"? F1", "toggle this help (F1 also while typing)"},
		bindingHelp(km.Quit),
	}
}

// renderHelp draws the key list for the active tab as a centered modal.
func renderHelp(tab Tab, km KeyMap, view queriesView, width, height int) string {
	title, local := tabHelp(tab, km, view)
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
	if notes := tabNotes(tab); len(notes) > 0 {
		sections = append(sections, "", styleModalTitle.Render("About this tab"))
		for _, n := range notes {
			for _, l := range wrapText(n, 76) {
				sections = append(sections, styleDim.Render(l))
			}
		}
	}
	sections = append(sections, "", styleModalTitle.Render("Global"))
	sections = append(sections, rows(global)...)
	sections = append(sections, "", styleDim.Render("[?/esc] close"))

	content := lipgloss.JoinVertical(lipgloss.Left, sections...)
	return placeModal(content, 0, width, height)
}
