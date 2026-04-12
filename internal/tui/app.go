package tui

import (
	"context"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// Tab represents the active tab.
type Tab int

const (
	TabOverview Tab = iota
	TabNodes
	TabQueries
	TabTables
	TabShards
	TabSQL
)

var tabNames = []string{"Overview", "Nodes", "Queries", "Tables", "Shards", "SQL"}

// StoreTickMsg triggers a TUI refresh from the store.
type StoreTickMsg struct{}

// App is the root bubbletea model.
type App struct {
	activeTab   Tab
	overview    OverviewModel
	nodes       NodesModel
	queries     QueriesModel
	tables      TablesModel
	shards      ShardsModel
	sql         SQLModel
	statusBar   StatusBarModel
	store       *store.Store
	registry    *cratedb.Registry
	collectors  *collector.Manager
	ctx         context.Context
	keyMap      KeyMap
	refreshRate time.Duration
	width       int
	height      int
	ready       bool
}

// NewApp creates the root TUI model.
func NewApp(st *store.Store, reg *cratedb.Registry, mgr *collector.Manager, ctx context.Context, refreshRate time.Duration) *App {
	return &App{
		store:       st,
		registry:    reg,
		collectors:  mgr,
		ctx:         ctx,
		keyMap:      DefaultKeyMap(),
		refreshRate: refreshRate,
		sql:         NewSQLModel(0, 0, reg, ctx),
	}
}

func (a *App) Init() tea.Cmd {
	return a.doStoreTick()
}

func (a *App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		a.width = msg.Width
		a.height = msg.Height
		a.ready = true
		bodyHeight := a.height - 3 // tab bar + status bar
		a.overview = a.overview.SetSize(a.width, bodyHeight)
		a.nodes = a.nodes.SetSize(a.width, bodyHeight)
		a.queries = a.queries.SetSize(a.width, bodyHeight)
		a.tables = a.tables.SetSize(a.width, bodyHeight)
		a.shards = a.shards.SetSize(a.width, bodyHeight)
		a.sql = a.sql.SetSize(a.width, bodyHeight)
		a.statusBar = a.statusBar.SetWidth(a.width)
		return a, nil

	case tea.KeyMsg:
		// When a tab is in input mode (search), delegate all keys to it first
		if a.isTabInputMode() {
			// Only ctrl+c can exit during input mode
			if msg.Type == tea.KeyCtrlC {
				return a, tea.Quit
			}
			// Allow tab navigation from SQL tab via tab/shift+tab
			if a.activeTab == TabSQL &&
				(key.Matches(msg, a.keyMap.NextTab) || key.Matches(msg, a.keyMap.PrevTab)) {
				// Don't return — fall through to tab switching below
			} else {
				return a, a.delegateKey(msg)
			}
		}

		switch {
		case key.Matches(msg, a.keyMap.Quit):
			return a, tea.Quit
		case key.Matches(msg, a.keyMap.Tab1):
			a.setActiveTab(TabOverview)
		case key.Matches(msg, a.keyMap.Tab2):
			a.setActiveTab(TabNodes)
		case key.Matches(msg, a.keyMap.Tab3):
			a.setActiveTab(TabQueries)
		case key.Matches(msg, a.keyMap.Tab4):
			a.setActiveTab(TabTables)
		case key.Matches(msg, a.keyMap.Tab5):
			a.setActiveTab(TabShards)
		case key.Matches(msg, a.keyMap.Tab6):
			a.setActiveTab(TabSQL)
		case key.Matches(msg, a.keyMap.NextTab):
			a.setActiveTab((a.activeTab + 1) % Tab(len(tabNames)))
		case key.Matches(msg, a.keyMap.PrevTab):
			a.setActiveTab((a.activeTab - 1 + Tab(len(tabNames))) % Tab(len(tabNames)))
		case key.Matches(msg, a.keyMap.Throttle):
			a.collectors.CycleThrottle()
			return a, nil
		case key.Matches(msg, a.keyMap.Refresh):
			// Manual refresh for current tab's data
			switch a.activeTab {
			case TabTables:
				a.collectors.TriggerCollector(a.ctx, "shards")
			case TabShards:
				a.collectors.TriggerCollector(a.ctx, "shards")
			case TabOverview:
				a.collectors.TriggerCollector(a.ctx, "health")
				a.collectors.TriggerCollector(a.ctx, "cluster")
			case TabNodes:
				a.collectors.TriggerCollector(a.ctx, "nodes")
			case TabQueries:
				a.collectors.TriggerCollector(a.ctx, "queries")
			}
			return a, nil
		case key.Matches(msg, a.keyMap.Reconnect):
			a.registry.Reconnect(a.ctx)
			return a, nil
		default:
			return a, a.delegateKey(msg)
		}
		return a, nil

	case SQLResultMsg:
		a.sql = a.sql.HandleResult(msg)
		return a, nil

	case StoreTickMsg:
		throttle := a.collectors.Throttle()
		hint := a.snapshotHint()
		snap := a.store.Snapshot(collector.ThrottleMultiplier(throttle), hint)
		switch a.activeTab {
		case TabOverview:
			a.overview = a.overview.Refresh(snap)
		case TabNodes:
			a.nodes = a.nodes.Refresh(snap)
		case TabQueries:
			a.queries = a.queries.Refresh(snap)
		case TabTables:
			a.tables = a.tables.Refresh(snap)
		case TabShards:
			a.shards = a.shards.Refresh(snap)
		}
		a.statusBar = a.statusBar.Refresh(
			a.registry.Status(),
			throttle,
			a.collectors.SuggestThrottle(),
		)
		return a, a.doStoreTick()
	}

	return a, nil
}

func (a *App) View() string {
	if !a.ready {
		return "Initializing..."
	}

	tabBar := a.renderTabBar()

	var body string
	switch a.activeTab {
	case TabOverview:
		body = a.overview.View()
	case TabNodes:
		body = a.nodes.View()
	case TabQueries:
		body = a.queries.View()
	case TabTables:
		body = a.tables.View()
	case TabShards:
		body = a.shards.View()
	case TabSQL:
		body = a.sql.View()
	}

	status := a.statusBar.View()

	// Calculate available body height and truncate if needed
	bodyHeight := a.height - 3 // tab bar (1) + spacing (1) + status bar (1)
	if bodyHeight < 1 {
		bodyHeight = 1
	}
	body = lipgloss.NewStyle().MaxHeight(bodyHeight).Render(body)

	return lipgloss.JoinVertical(lipgloss.Left, tabBar, body, status)
}

func (a *App) renderTabBar() string {
	var tabs []string
	for i, name := range tabNames {
		if Tab(i) == a.activeTab {
			tabs = append(tabs, styleTabActive.Render(name))
		} else {
			tabs = append(tabs, styleTabInactive.Render(name))
		}
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, tabs...)
}

func (a *App) delegateKey(msg tea.KeyMsg) tea.Cmd {
	switch a.activeTab {
	case TabOverview:
		var cmd tea.Cmd
		a.overview, cmd = a.overview.HandleKey(msg)
		return cmd
	case TabNodes:
		var cmd tea.Cmd
		a.nodes, cmd = a.nodes.HandleKey(msg)
		return cmd
	case TabQueries:
		var cmd tea.Cmd
		a.queries, cmd = a.queries.HandleKey(msg)
		return cmd
	case TabTables:
		var cmd tea.Cmd
		a.tables, cmd = a.tables.HandleKey(msg)
		return cmd
	case TabShards:
		var cmd tea.Cmd
		a.shards, cmd = a.shards.HandleKey(msg)
		return cmd
	case TabSQL:
		var cmd tea.Cmd
		a.sql, cmd = a.sql.HandleKey(msg)
		return cmd
	}
	return nil
}

func (a *App) isTabInputMode() bool {
	switch a.activeTab {
	case TabNodes:
		return a.nodes.searching
	case TabTables:
		return a.tables.searching
	case TabShards:
		return a.shards.searching
	case TabSQL:
		return a.sql.IsEditing()
	}
	return false
}

func (a *App) setActiveTab(tab Tab) {
	a.activeTab = tab
	a.collectors.SetFastPath("shards", tab == TabShards)
	// Refresh the newly active tab immediately so it has current data
	throttle := a.collectors.Throttle()
	hint := a.snapshotHint()
	snap := a.store.Snapshot(collector.ThrottleMultiplier(throttle), hint)
	switch tab {
	case TabOverview:
		a.overview = a.overview.Refresh(snap)
	case TabNodes:
		a.nodes = a.nodes.Refresh(snap)
	case TabQueries:
		a.queries = a.queries.Refresh(snap)
	case TabTables:
		a.tables = a.tables.Refresh(snap)
	case TabShards:
		a.shards = a.shards.Refresh(snap)
	}
}

func (a *App) snapshotHint() store.SnapshotHint {
	switch a.activeTab {
	case TabOverview:
		return store.SnapshotHint{IncludeCluster: true, IncludeHealth: true, IncludeNodes: true, IncludeTables: true}
	case TabNodes:
		return store.SnapshotHint{IncludeNodes: true}
	case TabQueries:
		return store.SnapshotHint{IncludeQueries: true}
	case TabTables:
		return store.SnapshotHint{IncludeTables: true, IncludeHealth: true}
	case TabShards:
		return store.SnapshotHint{IncludeShards: true}
	default:
		return store.SnapshotHint{}
	}
}

func (a *App) doStoreTick() tea.Cmd {
	return tea.Tick(a.refreshRate, func(time.Time) tea.Msg {
		return StoreTickMsg{}
	})
}
