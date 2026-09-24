package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
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
	activeTab    Tab
	overview     OverviewModel
	nodes        NodesModel
	queries      QueriesModel
	tables       TablesModel
	shards       ShardsModel
	sql          SQLModel
	statusBar    StatusBarModel
	queryLog     QueryLogOverlay
	showQueryLog bool
	showHelp     bool
	showAlerts   bool
	alertScroll  int
	alerts       store.AlertsState
	alertBell    bool
	store        *store.Store
	registry     *cratedb.Registry
	collectors   *collector.Manager
	ctx          context.Context
	keyMap       KeyMap
	refreshRate  time.Duration
	width        int
	height       int
	ready        bool
}

// NewApp creates the root TUI model.
func NewApp(st *store.Store, reg *cratedb.Registry, mgr *collector.Manager, ctx context.Context, tuiCfg config.TUIConfig, readOnly bool) *App {
	persistent := tuiCfg.SetGlobalMode != "transient"
	a := &App{
		store:       st,
		registry:    reg,
		collectors:  mgr,
		ctx:         ctx,
		keyMap:      DefaultKeyMap(),
		refreshRate: tuiCfg.RefreshRate.Duration,
		alertBell:   tuiCfg.AlertBell,
		overview:    NewOverviewModel(0, 0, persistent),
		nodes:       NewNodesModel(0, 0),
		queries:     NewQueriesModel(0, 0),
		tables:      NewTablesModel(0, 0),
		shards:      NewShardsModel(0, 0),
		sql:         NewSQLModel(0, 0, reg, ctx),
	}
	a.sql.readOnly = readOnly
	a.overview.editor.readOnly = readOnly
	a.statusBar.readOnly = readOnly
	return a
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
		a.queryLog.SetSize(a.width, a.height*40/100)
		a.resizeTabs()
		return a, nil

	case tea.KeyMsg:
		// Help modal swallows everything but its close keys and ctrl+c.
		if a.showHelp {
			switch {
			case msg.Type == tea.KeyCtrlC:
				return a, tea.Quit
			case key.Matches(msg, a.keyMap.Help), key.Matches(msg, a.keyMap.Escape), key.Matches(msg, a.keyMap.Quit):
				a.showHelp = false
			}
			return a, nil
		}
		if a.showAlerts {
			switch {
			case msg.Type == tea.KeyCtrlC:
				return a, tea.Quit
			case key.Matches(msg, a.keyMap.Alerts), key.Matches(msg, a.keyMap.Escape), key.Matches(msg, a.keyMap.Quit):
				a.showAlerts = false
			case key.Matches(msg, a.keyMap.Up):
				a.alertScroll = max(a.alertScroll-1, 0)
			case key.Matches(msg, a.keyMap.Down):
				a.alertScroll = min(a.alertScroll+1, max(len(a.alerts.History)-1, 0))
			}
			return a, nil
		}
		// F1 opens help even while typing, where ? is text.
		if msg.Type == tea.KeyF1 {
			a.showHelp = true
			return a, nil
		}

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
			for _, name := range a.current().Collectors() {
				a.collectors.TriggerCollector(a.ctx, name)
			}
			return a, nil
		case key.Matches(msg, a.keyMap.QueryLog):
			a.showQueryLog = !a.showQueryLog
			if a.showQueryLog {
				a.queryLog.SetSize(a.width, a.height*40/100)
				a.queryLog.Refresh(a.collectors.QueryTracker(), a.collectors.Throttle())
			}
			a.resizeTabs()
			return a, nil
		case key.Matches(msg, a.keyMap.Reconnect):
			a.registry.Reconnect(a.ctx)
			return a, nil
		case key.Matches(msg, a.keyMap.Help):
			a.showHelp = true
			return a, nil
		case key.Matches(msg, a.keyMap.Alerts):
			a.showAlerts, a.alertScroll = true, 0
			return a, nil
		default:
			return a, a.delegateKey(msg)
		}
		return a, nil

	case SQLResultMsg:
		a.sql = a.sql.HandleResult(msg)
		return a, nil

	case KillQueryMsg:
		reg := a.registry
		ctx := a.ctx
		id := msg.ID
		return a, func() tea.Msg {
			_, err := reg.Query(ctx, "KILL ?", id)
			if err != nil {
				return KillQueryResultMsg{ID: id, Error: err.Error()}
			}
			return KillQueryResultMsg{ID: id}
		}

	case SetSettingMsg:
		reg := a.registry
		ctx := a.ctx
		persistence := "PERSISTENT"
		if !msg.Persistent {
			persistence = "TRANSIENT"
		}
		stmt := fmt.Sprintf(`SET GLOBAL %s "%s" = ?`, persistence, msg.SettingPath)
		slotIdx := msg.SlotIndex
		return a, func() tea.Msg {
			_, err := reg.Query(ctx, stmt, msg.Value)
			if err != nil {
				return SetSettingResultMsg{SlotIndex: slotIdx, Error: err.Error()}
			}
			return SetSettingResultMsg{SlotIndex: slotIdx}
		}

	case SetSettingResultMsg:
		a.overview.editor.handleResult(msg)
		if msg.Error == "" {
			a.collectors.TriggerCollector(a.ctx, "cluster")
		}
		return a, nil

	case KillQueryResultMsg:
		if msg.Error != "" {
			a.queries.killResult = fmt.Sprintf("Kill failed: %s", msg.Error)
			a.queries.killIsError = true
		} else {
			a.queries.killResult = fmt.Sprintf("Killed query %s", msg.ID)
			a.queries.killIsError = false
			a.collectors.TriggerCollector(a.ctx, "queries")
		}
		a.queries.killResultAt = time.Now()
		return a, nil

	case YankResultMsg:
		if msg.Error != "" {
			a.queries.yankResult = fmt.Sprintf("Yank failed: %s", msg.Error)
		} else {
			a.queries.yankResult = "Copied to clipboard"
		}
		a.queries.yankResultAt = time.Now()
		return a, nil

	case StoreTickMsg:
		throttle := a.collectors.Throttle()
		status := a.registry.Status()
		a.store.ObserveConnection(status.Connected)
		hint := a.snapshotHint()
		snap := a.store.Snapshot(collector.ThrottleMultiplier(throttle), hint)
		a.current().Refresh(snap)
		var ring tea.Cmd
		if a.alertBell && snap.Alerts.Raised > a.alerts.Raised {
			ring = bell
		}
		a.alerts = snap.Alerts
		a.statusBar.alerts = snap.Alerts
		shardStat := a.collectors.QueryTracker().GetStat(collector.QueryShards)
		a.statusBar = a.statusBar.Refresh(
			status,
			throttle,
			a.collectors.SuggestThrottle(),
			a.store.ClusterHealth(),
			a.store.ShardCount(),
			shardStat.LastDur,
		)
		if a.showQueryLog {
			a.queryLog.Refresh(a.collectors.QueryTracker(), throttle)
		}
		// jobs_log is only polled while the slowest board is on screen.
		a.collectors.SetJobsLogView(a.ctx, a.activeTab == TabQueries && a.queries.showSlowest, a.queries.logMode)
		return a, tea.Batch(a.doStoreTick(), ring)
	}

	return a, nil
}

func (a *App) View() string {
	if !a.ready {
		return "Initializing..."
	}

	tabBar := a.renderTabBar()

	body := a.current().View()
	if a.showHelp {
		body = renderHelp(a.activeTab, a.keyMap, a.queries.showSlowest, a.width, a.bodyHeight())
	}
	if a.showAlerts {
		body = renderAlerts(a.alerts, a.alertScroll, a.width, a.bodyHeight())
	}

	status := a.statusBar.View()

	// Calculate available body height
	bodyHeight := a.bodyHeight()
	if bodyHeight < 1 {
		bodyHeight = 1
	}
	body = lipgloss.NewStyle().MaxHeight(bodyHeight).Render(body)

	if a.showQueryLog {
		overlay := a.queryLog.View()
		return lipgloss.JoinVertical(lipgloss.Left, tabBar, body, overlay, status)
	}

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
	bar := lipgloss.JoinHorizontal(lipgloss.Top, tabs...)
	room := a.width - lipgloss.Width(bar) - 2
	if banner := alertBanner(a.alerts, room); banner != "" {
		bar += strings.Repeat(" ", room-lipgloss.Width(banner)+1) + banner
	}
	return bar
}

func (a *App) delegateKey(msg tea.KeyMsg) tea.Cmd {
	return a.current().HandleKey(msg)
}

func (a *App) isTabInputMode() bool {
	return a.current().InputMode()
}

func (a *App) setActiveTab(tab Tab) {
	a.activeTab = tab
	a.collectors.SetFastPath("shards", tab == TabShards)
	// Refresh the newly active tab immediately so it has current data
	throttle := a.collectors.Throttle()
	hint := a.snapshotHint()
	snap := a.store.Snapshot(collector.ThrottleMultiplier(throttle), hint)
	a.current().Refresh(snap)
}

func (a *App) snapshotHint() store.SnapshotHint {
	return a.current().Hint()
}

// bodyHeight returns the available height for the tab body, accounting for
// tab bar, status bar, and optionally the query log overlay.
func (a *App) bodyHeight() int {
	h := a.height - 3 // tab bar (1) + spacing (1) + status bar (1)
	if a.showQueryLog {
		h -= a.queryLog.Height()
	}
	if h < 1 {
		h = 1
	}
	return h
}

// resizeTabs recalculates tab body sizes (e.g. after toggling the overlay).
func (a *App) resizeTabs() {
	bodyHeight := a.bodyHeight()
	for _, t := range a.tabs() {
		t.SetSize(a.width, bodyHeight)
	}
	a.statusBar = a.statusBar.SetWidth(a.width)
}

func (a *App) doStoreTick() tea.Cmd {
	return tea.Tick(a.refreshRate, func(time.Time) tea.Msg {
		return StoreTickMsg{}
	})
}
