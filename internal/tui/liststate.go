package tui

import (
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// listState is the selection, scroll, search and sort plumbing shared by the
// Nodes, Tables and Shards lists. Tabs embed it and keep their own filtering,
// sort order and list height. Sort field 0 is the name-like one: it sorts
// ascending, every other field descending.
type listState[F ~int] struct {
	sorted    []int // indices into the tab's rows after filter+sort
	selected  int
	scroll    int
	sortField F
	sortDesc  bool
	searching bool
	search    string
}

// listKey reports what handleKey did with a key.
type listKey struct {
	handled bool // the key belonged to the list (always true while searching)
	rebuild bool // search or sort changed: refilter and resort
	moved   bool // selection moved up or down
}

// handleKey applies the keys every list shares. While searching it swallows
// all keys. Keys it doesn't handle are left to the tab.
func (l *listState[F]) handleKey(msg tea.KeyMsg, km KeyMap, sortFields F) listKey {
	if l.searching {
		switch {
		case key.Matches(msg, km.Escape):
			l.searching = false
			l.search = ""
			l.resetCursor()
			return listKey{handled: true, rebuild: true}
		case msg.Type == tea.KeyEnter:
			l.searching = false
		case msg.Type == tea.KeyBackspace:
			if len(l.search) > 0 {
				l.search = l.search[:len(l.search)-1]
				l.resetCursor()
				return listKey{handled: true, rebuild: true}
			}
		case msg.Type == tea.KeyRunes:
			l.search += string(msg.Runes)
			l.resetCursor()
			return listKey{handled: true, rebuild: true}
		}
		return listKey{handled: true}
	}

	switch {
	case key.Matches(msg, km.Up):
		if l.selected > 0 {
			l.selected--
			return listKey{handled: true, moved: true}
		}
	case key.Matches(msg, km.Down):
		if l.selected < len(l.sorted)-1 {
			l.selected++
			return listKey{handled: true, moved: true}
		}
	case key.Matches(msg, km.Search):
		// Starts empty but keeps the old filter applied until the first
		// keystroke; that's how it has always behaved.
		l.searching = true
		l.search = ""
		l.resetCursor()
	case key.Matches(msg, km.SortNext):
		old := l.sortField
		l.sortField = (l.sortField + 1) % sortFields
		if l.sortField == old {
			l.sortDesc = !l.sortDesc
		} else {
			l.sortDesc = l.sortField != 0
		}
		l.resetCursor()
		return listKey{handled: true, rebuild: true}
	case key.Matches(msg, km.Escape):
		if l.search != "" {
			l.search = ""
			l.resetCursor()
			return listKey{handled: true, rebuild: true}
		}
	default:
		return listKey{}
	}
	return listKey{handled: true}
}

func (l *listState[F]) resetCursor() {
	l.selected = 0
	l.scroll = 0
}

// matches reports whether s passes the search: case-insensitive substring.
func (l *listState[F]) matches(s string) bool {
	return l.search == "" || strings.Contains(strings.ToLower(s), strings.ToLower(l.search))
}

// sortRows orders sorted by less, flipped when sortDesc.
func (l *listState[F]) sortRows(less func(ia, ib int) bool) {
	desc := l.sortDesc
	sort.Slice(l.sorted, func(a, b int) bool {
		lt := less(l.sorted[a], l.sorted[b])
		if desc {
			return !lt
		}
		return lt
	})
}

// clampSelection pulls the selection onto the last row after the list shrank.
func (l *listState[F]) clampSelection() {
	if l.selected >= len(l.sorted) && len(l.sorted) > 0 {
		l.selected = len(l.sorted) - 1
	}
}

// clampScrollTo keeps the selection inside a window of listH rows.
func (l *listState[F]) clampScrollTo(listH int) {
	if l.selected < l.scroll {
		l.scroll = l.selected
	}
	if l.selected >= l.scroll+listH {
		l.scroll = l.selected - listH + 1
	}
	maxScroll := max(len(l.sorted)-listH, 0)
	if l.scroll > maxScroll {
		l.scroll = maxScroll
	}
	if l.scroll < 0 {
		l.scroll = 0
	}
}

func (l *listState[F]) sortLabel(names []string) string {
	if l.sortDesc {
		return "sort: " + names[l.sortField] + " ↓"
	}
	return "sort: " + names[l.sortField] + " ↑"
}
