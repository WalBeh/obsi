package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// tabModel is what App needs from a tab.
type tabModel interface {
	Refresh(store.StoreSnapshot)
	SetSize(width, height int)
	HandleKey(tea.KeyMsg) tea.Cmd
	View() string
	InputMode() bool
	Hint() store.SnapshotHint
	Collectors() []string
}

// tab adapts a value-style tab model (methods return the updated model, the
// bubbletea idiom the tabs and their tests use) to tabModel by writing
// results back through m.
type tab[M any] struct {
	m          *M
	refresh    func(M, store.StoreSnapshot) M // nil: tab shows no store data
	setSize    func(M, int, int) M
	handleKey  func(M, tea.KeyMsg) (M, tea.Cmd)
	view       func(M) string
	inputMode  func(M) bool       // taking text: keys go to the tab first
	hint       store.SnapshotHint // what the tab reads from the store
	collectors []string           // run by the manual refresh key
}

func (t tab[M]) Refresh(snap store.StoreSnapshot) {
	if t.refresh != nil {
		*t.m = t.refresh(*t.m, snap)
	}
}

func (t tab[M]) SetSize(width, height int) { *t.m = t.setSize(*t.m, width, height) }

func (t tab[M]) HandleKey(msg tea.KeyMsg) tea.Cmd {
	var cmd tea.Cmd
	*t.m, cmd = t.handleKey(*t.m, msg)
	return cmd
}

func (t tab[M]) View() string             { return t.view(*t.m) }
func (t tab[M]) InputMode() bool          { return t.inputMode(*t.m) }
func (t tab[M]) Hint() store.SnapshotHint { return t.hint }
func (t tab[M]) Collectors() []string     { return t.collectors }

// tabs lists every tab in Tab order. Adding a tab means adding it here and
// to tabNames.
func (a *App) tabs() []tabModel {
	return []tabModel{
		TabOverview: tab[OverviewModel]{
			m: &a.overview, refresh: OverviewModel.Refresh, setSize: OverviewModel.SetSize,
			handleKey: OverviewModel.HandleKey, view: OverviewModel.View,
			inputMode:  func(m OverviewModel) bool { return m.editor.isInputMode() },
			hint:       store.SnapshotHint{IncludeCluster: true, IncludeHealth: true, IncludeNodes: true, IncludeTables: true, IncludeJMX: true},
			collectors: []string{"health", "cluster", "snapshots"},
		},
		TabNodes: tab[NodesModel]{
			m: &a.nodes, refresh: NodesModel.Refresh, setSize: NodesModel.SetSize,
			handleKey: NodesModel.HandleKey, view: NodesModel.View,
			inputMode:  func(m NodesModel) bool { return m.searching },
			hint:       store.SnapshotHint{IncludeNodes: true, IncludeJMX: true},
			collectors: []string{"nodes"},
		},
		TabQueries: tab[QueriesModel]{
			m: &a.queries, refresh: QueriesModel.Refresh, setSize: QueriesModel.SetSize,
			handleKey: QueriesModel.HandleKey, view: QueriesModel.View,
			inputMode:  func(m QueriesModel) bool { return m.killTarget != nil || m.infoTarget != nil },
			hint:       store.SnapshotHint{IncludeQueries: true},
			collectors: []string{"queries"},
		},
		TabTables: tab[TablesModel]{
			m: &a.tables, refresh: TablesModel.Refresh, setSize: TablesModel.SetSize,
			handleKey: TablesModel.HandleKey, view: TablesModel.View,
			inputMode:  func(m TablesModel) bool { return m.searching },
			hint:       store.SnapshotHint{IncludeTables: true, IncludeHealth: true},
			collectors: []string{"shards"},
		},
		TabShards: tab[ShardsModel]{
			m: &a.shards, refresh: ShardsModel.Refresh, setSize: ShardsModel.SetSize,
			handleKey: ShardsModel.HandleKey, view: ShardsModel.View,
			inputMode:  func(m ShardsModel) bool { return m.searching },
			hint:       store.SnapshotHint{IncludeShards: true},
			collectors: []string{"shards"},
		},
		TabSQL: tab[SQLModel]{
			m: &a.sql, setSize: SQLModel.SetSize,
			handleKey: SQLModel.HandleKey, view: SQLModel.View,
			inputMode: SQLModel.IsEditing,
		},
	}
}

func (a *App) current() tabModel { return a.tabs()[a.activeTab] }
