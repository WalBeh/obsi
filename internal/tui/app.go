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
)

var tabNames = []string{"Overview", "Nodes", "Queries", "Tables"}

// StoreTickMsg triggers a TUI refresh from the store.
type StoreTickMsg struct{}

// App is the root bubbletea model.
type App struct {
	activeTab   Tab
	overview    OverviewModel
	nodes       NodesModel
	queries     QueriesModel
	tables      TablesModel
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
		a.statusBar = a.statusBar.SetWidth(a.width)
		return a, nil

	case tea.KeyMsg:
		// When a tab is in input mode (search), delegate all keys to it first
		if a.isTabInputMode() {
			// Only ctrl+c can exit during input mode
			if msg.Type == tea.KeyCtrlC {
				return a, tea.Quit
			}
			return a, a.delegateKey(msg)
		}

		switch {
		case key.Matches(msg, a.keyMap.Quit):
			return a, tea.Quit
		case key.Matches(msg, a.keyMap.Tab1):
			a.activeTab = TabOverview
		case key.Matches(msg, a.keyMap.Tab2):
			a.activeTab = TabNodes
		case key.Matches(msg, a.keyMap.Tab3):
			a.activeTab = TabQueries
		case key.Matches(msg, a.keyMap.Tab4):
			a.activeTab = TabTables
		case key.Matches(msg, a.keyMap.NextTab):
			a.activeTab = (a.activeTab + 1) % Tab(len(tabNames))
		case key.Matches(msg, a.keyMap.PrevTab):
			a.activeTab = (a.activeTab - 1 + Tab(len(tabNames))) % Tab(len(tabNames))
		case key.Matches(msg, a.keyMap.Throttle):
			a.collectors.CycleThrottle()
			return a, nil
		case key.Matches(msg, a.keyMap.Refresh):
			// Manual refresh for current tab's data
			switch a.activeTab {
			case TabTables:
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

	case StoreTickMsg:
		snap := a.store.Snapshot()
		a.overview = a.overview.Refresh(snap)
		a.nodes = a.nodes.Refresh(snap)
		a.queries = a.queries.Refresh(snap)
		a.tables = a.tables.Refresh(snap)
		a.statusBar = a.statusBar.Refresh(
			a.registry.Status(),
			a.collectors.Throttle(),
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
	}
	return nil
}

func (a *App) isTabInputMode() bool {
	switch a.activeTab {
	case TabNodes:
		return a.nodes.searching
	case TabTables:
		return a.tables.searching
	}
	return false
}

func (a *App) doStoreTick() tea.Cmd {
	return tea.Tick(a.refreshRate, func(time.Time) tea.Msg {
		return StoreTickMsg{}
	})
}
