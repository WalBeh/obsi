package tui

import "github.com/charmbracelet/bubbles/key"

// KeyMap defines all key bindings.
type KeyMap struct {
	Tab1    key.Binding
	Tab2    key.Binding
	Tab3    key.Binding
	Tab4    key.Binding
	Tab5    key.Binding
	Tab6    key.Binding
	NextTab key.Binding
	PrevTab key.Binding
	Up      key.Binding
	Down    key.Binding
	Search    key.Binding
	SortNext  key.Binding
	Throttle  key.Binding
	Refresh   key.Binding
	Escape    key.Binding
	Reconnect key.Binding
	QueryLog  key.Binding
	Kill      key.Binding
	Quit      key.Binding
	Help      key.Binding
}

// DefaultKeyMap returns the default key bindings.
func DefaultKeyMap() KeyMap {
	return KeyMap{
		Tab1: key.NewBinding(
			key.WithKeys("1"),
			key.WithHelp("1", "overview"),
		),
		Tab2: key.NewBinding(
			key.WithKeys("2"),
			key.WithHelp("2", "nodes"),
		),
		Tab3: key.NewBinding(
			key.WithKeys("3"),
			key.WithHelp("3", "queries"),
		),
		Tab4: key.NewBinding(
			key.WithKeys("4"),
			key.WithHelp("4", "tables"),
		),
		Tab5: key.NewBinding(
			key.WithKeys("5"),
			key.WithHelp("5", "shards"),
		),
		Tab6: key.NewBinding(
			key.WithKeys("6"),
			key.WithHelp("6", "sql"),
		),
		NextTab: key.NewBinding(
			key.WithKeys("tab"),
			key.WithHelp("tab", "next tab"),
		),
		PrevTab: key.NewBinding(
			key.WithKeys("shift+tab"),
			key.WithHelp("shift+tab", "prev tab"),
		),
		Up: key.NewBinding(
			key.WithKeys("up", "k"),
			key.WithHelp("↑/k", "up"),
		),
		Down: key.NewBinding(
			key.WithKeys("down", "j"),
			key.WithHelp("↓/j", "down"),
		),
		Search: key.NewBinding(
			key.WithKeys("/"),
			key.WithHelp("/", "search"),
		),
		SortNext: key.NewBinding(
			key.WithKeys("s"),
			key.WithHelp("s", "cycle sort"),
		),
		Throttle: key.NewBinding(
		key.WithKeys("t"),
		key.WithHelp("t", "cycle throttle"),
	),
	Refresh: key.NewBinding(
			key.WithKeys("R", "ctrl+r", "f5"),
			key.WithHelp("ctrl+r", "refresh"),
		),
		Escape: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "clear search"),
		),
		Reconnect: key.NewBinding(
			key.WithKeys("r"),
			key.WithHelp("r", "reconnect"),
		),
		QueryLog: key.NewBinding(
			key.WithKeys("L"),
			key.WithHelp("L", "query log"),
		),
		Kill: key.NewBinding(
			key.WithKeys("K"),
			key.WithHelp("K", "kill query"),
		),
		Quit: key.NewBinding(
			key.WithKeys("q", "ctrl+c"),
			key.WithHelp("q", "quit"),
		),
		Help: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "help"),
		),
	}
}
