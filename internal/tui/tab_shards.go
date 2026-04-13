package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// ShardSortField defines sort columns for the problem shards list.
type ShardSortField int

const (
	ShardSortByTable    ShardSortField = iota
	ShardSortByState
	ShardSortByRecovery
	ShardSortBySize
	shardSortFieldCount
)

var shardSortFieldNames = [shardSortFieldCount]string{"table", "state", "recovery", "size"}

// ShardsModel is the Shards tab (Tab 5) showing shard health and allocation info.
type ShardsModel struct {
	snap      store.StoreSnapshot
	sorted    []int // indices into problemShards after sort+filter
	selected  int
	scroll    int
	sortField ShardSortField
	sortDesc  bool
	searching bool
	search    string
	width     int
	height    int

	keyMap    KeyMap

	// Derived data recomputed on each Refresh
	countStarted      int
	countInitializing int
	countUnassigned   int
	countRelocating   int
	problemShards     []cratedb.ShardInfo
}

func NewShardsModel(width, height int) ShardsModel {
	return ShardsModel{width: width, height: height, keyMap: DefaultKeyMap()}
}

func (m ShardsModel) Refresh(snap store.StoreSnapshot) ShardsModel {
	m.snap = snap

	// Count shards by routing state
	m.countStarted = 0
	m.countInitializing = 0
	m.countUnassigned = 0
	m.countRelocating = 0
	m.problemShards = m.problemShards[:0]

	for _, s := range snap.Shards {
		switch s.RoutingState {
		case "STARTED":
			m.countStarted++
		case "INITIALIZING":
			m.countInitializing++
		case "UNASSIGNED":
			m.countUnassigned++
		case "RELOCATING":
			m.countRelocating++
		}

		if s.RoutingState != "STARTED" {
			m.problemShards = append(m.problemShards, s)
		}
	}

	m.rebuildSorted()
	if m.selected >= len(m.sorted) && len(m.sorted) > 0 {
		m.selected = len(m.sorted) - 1
	}
	if len(m.sorted) == 0 {
		m.selected = 0
	}
	m.clampScroll()
	return m
}

func (m ShardsModel) SetSize(width, height int) ShardsModel {
	m.width = width
	m.height = height
	m.clampScroll()
	return m
}

func (m ShardsModel) listHeight() int {
	// title(1) + summary(1) + blank(1) + column header(1)
	headerLines := 4
	if m.searching {
		headerLines++
	}

	// Reserve 40% for detail panel, at least 6 lines
	detailReserve := m.height * 40 / 100
	if detailReserve < 6 {
		detailReserve = 6
	}
	listH := m.height - headerLines - detailReserve
	if listH < 3 {
		listH = 3
	}
	return listH
}

func (m *ShardsModel) clampScroll() {
	listH := m.listHeight()
	if m.selected < m.scroll {
		m.scroll = m.selected
	}
	if m.selected >= m.scroll+listH {
		m.scroll = m.selected - listH + 1
	}
	maxScroll := len(m.sorted) - listH
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.scroll > maxScroll {
		m.scroll = maxScroll
	}
	if m.scroll < 0 {
		m.scroll = 0
	}
}

func (m *ShardsModel) rebuildSorted() {
	m.sorted = m.sorted[:0]
	for i, s := range m.problemShards {
		if m.search != "" {
			name := strings.ToLower(s.SchemaName + "." + s.TableName)
			if !strings.Contains(name, strings.ToLower(m.search)) {
				continue
			}
		}
		m.sorted = append(m.sorted, i)
	}

	ps := m.problemShards
	sf := m.sortField
	desc := m.sortDesc
	sort.Slice(m.sorted, func(a, b int) bool {
		ia, ib := m.sorted[a], m.sorted[b]
		var less bool
		switch sf {
		case ShardSortByTable:
			na := ps[ia].SchemaName + "." + ps[ia].TableName
			nb := ps[ib].SchemaName + "." + ps[ib].TableName
			if na == nb {
				less = ps[ia].ID < ps[ib].ID
			} else {
				less = na < nb
			}
		case ShardSortByState:
			if ps[ia].RoutingState == ps[ib].RoutingState {
				less = ps[ia].SchemaName+"."+ps[ia].TableName < ps[ib].SchemaName+"."+ps[ib].TableName
			} else {
				less = ps[ia].RoutingState < ps[ib].RoutingState
			}
		case ShardSortByRecovery:
			less = ps[ia].RecoveryPercent < ps[ib].RecoveryPercent
		case ShardSortBySize:
			less = ps[ia].Size < ps[ib].Size
		default:
			less = ia < ib
		}
		if desc {
			return !less
		}
		return less
	})
}

func (m ShardsModel) HandleKey(msg tea.KeyMsg) (ShardsModel, tea.Cmd) {
	km := m.keyMap

	if m.searching {
		switch {
		case key.Matches(msg, km.Escape):
			m.searching = false
			m.search = ""
			m.selected = 0
			m.scroll = 0
			m.rebuildSorted()
			return m, nil
		case msg.Type == tea.KeyEnter:
			m.searching = false
			return m, nil
		case msg.Type == tea.KeyBackspace:
			if len(m.search) > 0 {
				m.search = m.search[:len(m.search)-1]
				m.selected = 0
				m.scroll = 0
				m.rebuildSorted()
			}
			return m, nil
		case msg.Type == tea.KeyRunes:
			m.search += string(msg.Runes)
			m.selected = 0
			m.scroll = 0
			m.rebuildSorted()
			return m, nil
		}
		return m, nil
	}

	switch {
	case key.Matches(msg, km.Up):
		if m.selected > 0 {
			m.selected--
			m.clampScroll()
		}
	case key.Matches(msg, km.Down):
		if m.selected < len(m.sorted)-1 {
			m.selected++
			m.clampScroll()
		}
	case key.Matches(msg, km.Search):
		m.searching = true
		m.search = ""
		m.selected = 0
		m.scroll = 0
	case key.Matches(msg, km.SortNext):
		oldField := m.sortField
		m.sortField = (m.sortField + 1) % shardSortFieldCount
		if m.sortField == oldField {
			m.sortDesc = !m.sortDesc
		}
		if m.sortField != oldField {
			m.sortDesc = m.sortField != ShardSortByTable
		}
		m.selected = 0
		m.scroll = 0
		m.rebuildSorted()
	case key.Matches(msg, km.Escape):
		if m.search != "" {
			m.search = ""
			m.selected = 0
			m.scroll = 0
			m.rebuildSorted()
		}
	}
	return m, nil
}

func (m ShardsModel) View() string {
	title := styleTitle.Render("Shard Health")

	if m.snap.Staleness["shards"] {
		return title + "\n" + styleStale.Render("  (stale data)")
	}

	totalShards := len(m.snap.Shards)
	if totalShards == 0 {
		return title + "\n  No shards found"
	}

	var lines []string
	lines = append(lines, title)

	// Summary line with counts by state
	summary := m.renderSummary()
	lines = append(lines, summary)
	lines = append(lines, "")

	// Happy path: all healthy
	if len(m.problemShards) == 0 {
		lines = append(lines, styleHealthGreen.Render(
			fmt.Sprintf("  All %d shards healthy", totalShards)))
		return strings.Join(lines, "\n")
	}

	// Search input
	if m.searching {
		lines = append(lines, fmt.Sprintf("  Search: %s▏", m.search))
	}

	// Column header
	sortIndicator := fmt.Sprintf("sort: %s", shardSortFieldNames[m.sortField])
	if m.sortDesc {
		sortIndicator += " ↓"
	} else {
		sortIndicator += " ↑"
	}

	filterInfo := ""
	if m.search != "" {
		filterInfo = fmt.Sprintf(" | filter: %q (%d match)", m.search, len(m.sorted))
	}

	lines = append(lines, fmt.Sprintf("  Shards (%d) | %s%s | %s",
		len(m.problemShards), sortIndicator, filterInfo,
		styleDim.Render("s:sort  /:search  R:refresh")))

	header := styleHeader.Render(fmt.Sprintf("  %-3s %-28s %5s %3s %-14s %21s %10s %s",
		"", "TABLE", "SHARD", "P/R", "STATE", "RECOVERY", "SIZE", "NODE"))
	lines = append(lines, header)

	if len(m.sorted) == 0 {
		lines = append(lines, "  No matching shards")
	} else {
		// Virtual scrolling
		listH := m.listHeight()
		visibleEnd := m.scroll + listH
		if visibleEnd > len(m.sorted) {
			visibleEnd = len(m.sorted)
		}

		if m.scroll > 0 {
			lines = append(lines, styleDim.Render(fmt.Sprintf("  ↑ %d more above", m.scroll)))
		}

		for viewIdx := m.scroll; viewIdx < visibleEnd; viewIdx++ {
			shardIdx := m.sorted[viewIdx]
			s := m.problemShards[shardIdx]
			marker := "  "
			if viewIdx == m.selected {
				marker = "▸ "
			}

			tableName := fmt.Sprintf("%s.%s", s.SchemaName, s.TableName)
			if len(tableName) > 28 {
				tableName = tableName[:25] + "..."
			}

			pr := "R"
			if s.Primary {
				pr = "P"
			}

			stateStyle := styleDim
			switch s.RoutingState {
			case "UNASSIGNED":
				stateStyle = styleHealthRed
			case "INITIALIZING":
				stateStyle = styleHealthYellow
			case "RELOCATING":
				stateStyle = styleHealthYellow
			}

			node := s.NodeName
			if node == "" {
				node = "-"
			}

			// Show inline recovery progress for recovering shards
			recovery := ""
			if s.RecoveryStage != "" && s.RecoveryStage != "DONE" {
				pct := s.RecoveryPercent
				if pct > 100 {
					pct = 100
				}
				bar := metricBar(pct, 12)
				recovery = fmt.Sprintf("%s %5.1f%%", bar, pct)
			}

			row := fmt.Sprintf("%s%-28s %5d  %s  %s %21s %10s %s",
				marker, tableName, s.ID, pr,
				stateStyle.Render(fmt.Sprintf("%-14s", s.RoutingState)),
				recovery, formatBytes(s.Size), node)
			lines = append(lines, row)
		}

		remaining := len(m.sorted) - visibleEnd
		if remaining > 0 {
			lines = append(lines, styleDim.Render(fmt.Sprintf("  ↓ %d more below", remaining)))
		}
	}

	// Detail panel for selected problem shard
	if len(m.sorted) > 0 && m.selected < len(m.sorted) {
		lines = append(lines, "")
		lines = append(lines, m.renderDetail(m.problemShards[m.sorted[m.selected]]))
	}

	return strings.Join(lines, "\n")
}

func (m ShardsModel) renderSummary() string {
	lastRefresh := ""
	if t, ok := m.snap.LastUpdated["shards"]; ok && !t.IsZero() {
		ago := time.Since(t).Truncate(time.Second)
		lastRefresh = fmt.Sprintf(" | updated %s ago", ago)
	}

	parts := []string{
		fmt.Sprintf("%d STARTED", m.countStarted),
	}
	if m.countInitializing > 0 {
		parts = append(parts, styleHealthYellow.Render(fmt.Sprintf("%d INITIALIZING", m.countInitializing)))
	}
	if m.countUnassigned > 0 {
		parts = append(parts, styleHealthRed.Render(fmt.Sprintf("%d UNASSIGNED", m.countUnassigned)))
	}
	if m.countRelocating > 0 {
		parts = append(parts, styleHealthYellow.Render(fmt.Sprintf("%d RELOCATING", m.countRelocating)))
	}

	return fmt.Sprintf("  %s%s", strings.Join(parts, " | "), styleDim.Render(lastRefresh))
}

func (m ShardsModel) renderDetail(s cratedb.ShardInfo) string {
	pr := "replica"
	if s.Primary {
		pr = "primary"
	}

	var lines []string
	lines = append(lines, styleTitle.Render(fmt.Sprintf("  Allocation: %s.%s shard %d (%s)",
		s.SchemaName, s.TableName, s.ID, pr)))

	if s.NodeName != "" {
		lines = append(lines, fmt.Sprintf("    Node: %s", s.NodeName))
	}
	lines = append(lines, fmt.Sprintf("    State: %s | Size: %s | Docs: %s",
		s.RoutingState, formatBytes(s.Size), formatRecords(s.NumDocs)))

	if s.RecoveryStage != "" {
		pct := s.RecoveryPercent
		if pct > 100 {
			pct = 100
		}
		bar := metricBar(pct, 20)
		lines = append(lines, fmt.Sprintf("    Recovery: %s %5.1f%%  stage: %s", bar, pct, s.RecoveryStage))
	}

	if s.Relocating && s.RelocatingNode != "" {
		lines = append(lines, fmt.Sprintf("    Relocating to: %s", s.RelocatingNode))
	}

	// Find allocation reasons for this specific shard
	shardAllocs := m.findAllocations(s)
	if len(shardAllocs) > 0 {
		lines = append(lines, "")
		lines = append(lines, "    Allocation reasons:")

		// Deduplicate: group by explanation, count occurrences
		groups := deduplicateExplanations(shardAllocs)
		for _, g := range groups {
			prefix := styleHealthRed.Render("    x")
			lines = append(lines, fmt.Sprintf("%s %s (%d node%s)",
				prefix, g.explanation, g.count, pluralS(g.count)))
		}
	} else if len(m.snap.Allocations) == 0 && len(m.problemShards) > 0 {
		lines = append(lines, "")
		lines = append(lines, styleDim.Render("    (allocation detail unavailable)"))
	}

	return strings.Join(lines, "\n")
}

func (m ShardsModel) findAllocations(s cratedb.ShardInfo) []cratedb.AllocationInfo {
	var result []cratedb.AllocationInfo
	for _, a := range m.snap.Allocations {
		if a.TableSchema == s.SchemaName &&
			a.TableName == s.TableName &&
			a.ShardID == s.ID &&
			a.Primary == s.Primary {
			result = append(result, a)
		}
	}
	return result
}

type explanationGroup struct {
	explanation string
	count       int
}

func deduplicateExplanations(allocs []cratedb.AllocationInfo) []explanationGroup {
	counts := make(map[string]int)
	order := make([]string, 0)

	for _, a := range allocs {
		exp := a.Explanation
		if exp == "" {
			continue
		}
		if counts[exp] == 0 {
			order = append(order, exp)
		}
		counts[exp]++
	}

	groups := make([]explanationGroup, 0, len(order))
	for _, exp := range order {
		groups = append(groups, explanationGroup{
			explanation: exp,
			count:       counts[exp],
		})
	}
	return groups
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
