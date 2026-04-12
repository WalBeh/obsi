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

// SortField defines what column to sort by.
type SortField int

const (
	SortByName SortField = iota
	SortBySize
	SortByShards
	SortByRecords
	sortFieldCount // sentinel for cycling
)

var sortFieldNames = [sortFieldCount]string{"name", "size", "shards", "records"}

// TablesModel shows tables with shard distribution per node.
type TablesModel struct {
	snap        store.StoreSnapshot
	sorted      []int // indices into snap.Tables after sort+filter
	selected    int
	scroll      int // first visible row index
	sortField   SortField
	sortDesc    bool
	searching   bool
	search      string
	width       int
	height      int
	tableHealth map[string]string // "schema.table" -> worst health ("RED" > "YELLOW" > "GREEN")
}

func NewTablesModel(width, height int) TablesModel {
	return TablesModel{width: width, height: height, sortDesc: false}
}

func (m TablesModel) Refresh(snap store.StoreSnapshot) TablesModel {
	m.snap = snap
	m.buildHealthMap()
	m.rebuildSorted()
	if m.selected >= len(m.sorted) && len(m.sorted) > 0 {
		m.selected = len(m.sorted) - 1
	}
	m.clampScroll()
	return m
}

func (m TablesModel) SetSize(width, height int) TablesModel {
	m.width = width
	m.height = height
	m.clampScroll()
	return m
}

// listHeight returns how many table rows fit in the list area.
// We reserve ~40% of height for the detail panel, and subtract header lines.
func (m TablesModel) listHeight() int {
	// Header takes: title(1) + summary(1) + blank(1) + [search(2)] + column header(1) = 4-6 lines
	headerLines := 4
	if m.searching {
		headerLines += 2
	}
	// Reserve 40% for detail, at least 8 lines
	detailReserve := m.height * 40 / 100
	if detailReserve < 8 {
		detailReserve = 8
	}
	listH := m.height - headerLines - detailReserve
	if listH < 3 {
		listH = 3
	}
	return listH
}

func (m *TablesModel) clampScroll() {
	listH := m.listHeight()
	// Ensure selected is visible
	if m.selected < m.scroll {
		m.scroll = m.selected
	}
	if m.selected >= m.scroll+listH {
		m.scroll = m.selected - listH + 1
	}
	// Clamp scroll to valid range
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

func (m *TablesModel) buildHealthMap() {
	m.tableHealth = make(map[string]string)
	for _, h := range m.snap.TableHealth {
		key := h.TableSchema + "." + h.TableName
		prev := m.tableHealth[key]
		// Keep worst health: RED > YELLOW > GREEN
		if prev == "" || prev == "GREEN" || (prev == "YELLOW" && h.Health == "RED") {
			m.tableHealth[key] = h.Health
		}
	}
}

func (m *TablesModel) rebuildSorted() {
	m.sorted = m.sorted[:0]
	for i, t := range m.snap.Tables {
		if m.search != "" {
			name := strings.ToLower(t.SchemaName + "." + t.TableName)
			if !strings.Contains(name, strings.ToLower(m.search)) {
				continue
			}
		}
		m.sorted = append(m.sorted, i)
	}

	tables := m.snap.Tables
	sf := m.sortField
	desc := m.sortDesc
	sort.Slice(m.sorted, func(a, b int) bool {
		ia, ib := m.sorted[a], m.sorted[b]
		var less bool
		switch sf {
		case SortByName:
			na := tables[ia].SchemaName + "." + tables[ia].TableName
			nb := tables[ib].SchemaName + "." + tables[ib].TableName
			less = na < nb
		case SortBySize:
			less = tables[ia].TotalSize < tables[ib].TotalSize
		case SortByShards:
			less = tables[ia].TotalShards < tables[ib].TotalShards
		case SortByRecords:
			less = tables[ia].TotalRecords < tables[ib].TotalRecords
		default:
			less = ia < ib
		}
		if desc {
			return !less
		}
		return less
	})
}

func (m TablesModel) HandleKey(msg tea.KeyMsg) (TablesModel, tea.Cmd) {
	km := DefaultKeyMap()

	// Search mode: capture typed characters
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
		m.sortField = (m.sortField + 1) % sortFieldCount
		if m.sortField == oldField {
			m.sortDesc = !m.sortDesc
		}
		if m.sortField != oldField {
			m.sortDesc = m.sortField != SortByName
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

func (m TablesModel) View() string {
	title := styleTitle.Render("Tables & Shards")

	if m.snap.Staleness["shards"] {
		return title + "\n" + styleStale.Render("  (stale data)")
	}

	if len(m.snap.Tables) == 0 {
		return title + "\n  No tables found"
	}

	var lines []string
	lines = append(lines, title)

	// Summary + sort/search info
	sortIndicator := fmt.Sprintf("sort: %s", sortFieldNames[m.sortField])
	if m.sortDesc {
		sortIndicator += " ↓"
	} else {
		sortIndicator += " ↑"
	}

	filterInfo := ""
	if m.search != "" {
		var matchSize, matchDisk int64
		var matchShards int
		for _, idx := range m.sorted {
			matchSize += m.snap.Tables[idx].TotalSize
			matchDisk += m.snap.Tables[idx].TotalDiskSize
			matchShards += m.snap.Tables[idx].TotalShards
		}
		filterInfo = fmt.Sprintf(" │ filter: %q (%d match, size: %s, disk: %s, shards: %d)",
			m.search, len(m.sorted), formatBytes(matchSize), formatBytes(matchDisk), matchShards)
	} else if m.searching {
		filterInfo = " │ filter: _"
	}

	lastRefresh := ""
	if t, ok := m.snap.LastUpdated["shards"]; ok && !t.IsZero() {
		ago := time.Since(t).Truncate(time.Second)
		lastRefresh = fmt.Sprintf(" │ updated %s ago", ago)
	}

	lines = append(lines, fmt.Sprintf("  %d tables, %d total shards │ %s%s%s │ %s",
		len(m.snap.Tables), len(m.snap.Shards),
		sortIndicator, filterInfo,
		styleDim.Render(lastRefresh),
		styleDim.Render("s:sort  /:search  R:refresh")))
	lines = append(lines, "")

	// Search input line
	if m.searching {
		lines = append(lines, fmt.Sprintf("  Search: %s▏", m.search))
		lines = append(lines, "")
	}

	// Header with sort indicator
	nameHdr := m.sortHeader("TABLE", SortByName, 30)
	shardsHdr := m.sortHeader("SHARDS", SortByShards, 8)
	replicaHdr := "REPLICA"
	recordsHdr := m.sortHeader("RECORDS", SortByRecords, 10)
	sizeHdr := m.sortHeader("SIZE", SortBySize, 12)

	header := styleHeader.Render(fmt.Sprintf("  %-3s %-30s %7s %7s %10s %10s %10s",
		"", nameHdr, shardsHdr, replicaHdr, recordsHdr, sizeHdr, "DISK"))
	lines = append(lines, header)

	if len(m.sorted) == 0 {
		lines = append(lines, "  No matching tables")
		return strings.Join(lines, "\n")
	}

	// Virtual scrolling: only render visible rows
	listH := m.listHeight()
	visibleEnd := m.scroll + listH
	if visibleEnd > len(m.sorted) {
		visibleEnd = len(m.sorted)
	}

	// Scroll indicator top
	if m.scroll > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↑ %d more above", m.scroll)))
	}

	for viewIdx := m.scroll; viewIdx < visibleEnd; viewIdx++ {
		tableIdx := m.sorted[viewIdx]
		t := m.snap.Tables[tableIdx]
		marker := "  "
		if viewIdx == m.selected {
			marker = "▸ "
		}

		tableName := fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
		if len(tableName) > 30 {
			tableName = tableName[:27] + "..."
		}

		// Color table name by health status
		health := m.tableHealth[t.SchemaName+"."+t.TableName]
		switch health {
		case "RED":
			tableName = styleHealthRed.Render(fmt.Sprintf("%-30s", tableName))
		case "YELLOW":
			tableName = styleHealthYellow.Render(fmt.Sprintf("%-30s", tableName))
		default:
			tableName = fmt.Sprintf("%-30s", tableName)
		}

		row := fmt.Sprintf("%s%s %7d %7d %10s %10s %10s",
			marker, tableName,
			t.PrimaryShards, t.ReplicaShards,
			formatRecords(t.TotalRecords), formatBytes(t.TotalSize), formatBytes(t.TotalDiskSize))
		lines = append(lines, row)
	}

	// Scroll indicator bottom
	remaining := len(m.sorted) - visibleEnd
	if remaining > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↓ %d more below", remaining)))
	}

	// Detail panel for selected table (always visible below the list)
	if m.selected < len(m.sorted) {
		t := m.snap.Tables[m.sorted[m.selected]]
		lines = append(lines, "")
		lines = append(lines, m.renderDetail(t))

		// Show health issues for this table
		healthLines := m.renderTableHealth(t)
		if healthLines != "" {
			lines = append(lines, healthLines)
		}
	}

	return strings.Join(lines, "\n")
}

func (m TablesModel) sortHeader(label string, field SortField, width int) string {
	if m.sortField == field {
		arrow := "↑"
		if m.sortDesc {
			arrow = "↓"
		}
		return fmt.Sprintf("%s %s", label, arrow)
	}
	return label
}

func (m TablesModel) renderDetail(t cratedb.TableInfo) string {
	var lines []string
	lines = append(lines, styleTitle.Render(fmt.Sprintf("  Table: %s.%s", t.SchemaName, t.TableName)))

	// Table settings — only show operator-relevant values, highlight non-defaults
	ts := t.Settings
	if ts.NumberOfShards > 0 {
		lines = append(lines, fmt.Sprintf("    Shards: %d × %s replicas",
			ts.NumberOfShards, ts.NumberOfReplicas))

		var highlights []string

		if ts.ClusteredBy != "" && ts.ClusteredBy != "_id" {
			highlights = append(highlights, fmt.Sprintf("clustered by: %s", ts.ClusteredBy))
		}
		if len(ts.PartitionedBy) > 0 {
			highlights = append(highlights, fmt.Sprintf("partitioned by: %s", strings.Join(ts.PartitionedBy, ", ")))
		}
		if ts.ColumnPolicy == "dynamic" {
			highlights = append(highlights, styleHealthYellow.Render("policy: dynamic"))
		}
		if ts.RefreshInterval != 1000 {
			refreshStr := fmt.Sprintf("%dms", ts.RefreshInterval)
			if ts.RefreshInterval >= 1000 {
				refreshStr = fmt.Sprintf("%.0fs", float64(ts.RefreshInterval)/1000)
			}
			highlights = append(highlights, fmt.Sprintf("refresh: %s", refreshStr))
		}
		if ts.Codec != "" && ts.Codec != "default" {
			highlights = append(highlights, fmt.Sprintf("codec: %s", ts.Codec))
		}

		if len(highlights) > 0 {
			lines = append(lines, "    "+strings.Join(highlights, " │ "))
		}
	}

	// Shard size stats
	lines = append(lines, fmt.Sprintf("    Shard size   min: %-10s  avg: %-10s  max: %s",
		formatBytes(t.MinShardSize), formatBytes(t.AvgShardSize), formatBytes(t.MaxShardSize)))

	// Skew indicator
	if t.AvgShardSize > 0 && t.MaxShardSize > t.AvgShardSize*3 {
		lines = append(lines, styleHealthYellow.Render("    ⚠ shard size skew detected (max > 3x avg)"))
	}
	lines = append(lines, "")

	// Shard distribution per node
	lines = append(lines, "    Shard distribution:")

	if len(t.ShardsPerNode) == 0 {
		lines = append(lines, "      (no shards assigned)")
		return strings.Join(lines, "\n")
	}

	type nodeCount struct {
		name  string
		count int
	}
	var nodes []nodeCount
	for name, count := range t.ShardsPerNode {
		nodes = append(nodes, nodeCount{name, count})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].name < nodes[j].name
	})

	maxCount := 0
	for _, nc := range nodes {
		if nc.count > maxCount {
			maxCount = nc.count
		}
	}

	barWidth := 30
	for _, nc := range nodes {
		barLen := 0
		if maxCount > 0 {
			barLen = nc.count * barWidth / maxCount
		}
		if barLen < 1 && nc.count > 0 {
			barLen = 1
		}
		bar := repeat('█', barLen) + repeat('░', barWidth-barLen)
		lines = append(lines, fmt.Sprintf("      %-18s %s %d", nc.name, styleDim.Render(bar), nc.count))
	}

	return strings.Join(lines, "\n")
}

func (m TablesModel) renderTableHealth(t cratedb.TableInfo) string {
	var issues []string
	for _, h := range m.snap.TableHealth {
		if h.TableSchema != t.SchemaName || h.TableName != t.TableName {
			continue
		}
		if h.Health == "GREEN" {
			continue
		}
		style := healthStyle(h.Health)
		detail := fmt.Sprintf("    %s missing: %d, underreplicated: %d",
			style.Render(h.Health), h.MissingShards, h.UnderReplicated)
		if h.Partition != "" {
			detail += fmt.Sprintf(" (partition: %s)", h.Partition)
		}
		issues = append(issues, detail)
	}
	if len(issues) == 0 {
		return ""
	}
	return "    Health:\n" + strings.Join(issues, "\n")
}

func formatRecords(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}
