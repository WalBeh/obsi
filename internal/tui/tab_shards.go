package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// ShardSortField defines sort columns for the problem shards list.
type ShardSortField int

const (
	ShardSortByTable ShardSortField = iota
	ShardSortByState
	ShardSortByRecovery
	ShardSortBySize
	shardSortFieldCount
)

var shardSortFieldNames = [shardSortFieldCount]string{"table", "state", "recovery", "size"}

// ShardsModel is the Shards tab (Tab 5) showing shard health and allocation info.
type ShardsModel struct {
	listState[ShardSortField] // sorted indexes problemShards
	snap                      store.StoreSnapshot
	width                     int
	height                    int

	keyMap KeyMap

	// Derived data recomputed on each Refresh
	countStarted  int
	buckets       [bucketCount]int
	problemShards []cratedb.ShardInfo
	nodeNames     map[string]string // node id -> name, from the shards themselves
	fixes         [][]shardFix      // per problemShards entry
	diagnoses     []shardDiagnosis

	readOnly    bool
	fixTarget   *shardFix // x confirm modal
	noticeText  string
	noticeIsErr bool
	noticeAt    time.Time
}

func NewShardsModel(width, height int) ShardsModel {
	return ShardsModel{width: width, height: height, keyMap: DefaultKeyMap()}
}

func (m ShardsModel) Refresh(snap store.StoreSnapshot) ShardsModel {
	m.snap = snap

	m.countStarted = 0
	m.buckets = [bucketCount]int{}
	m.problemShards = m.problemShards[:0]
	m.nodeNames = make(map[string]string)

	for _, s := range snap.Shards {
		if s.NodeID != "" {
			m.nodeNames[s.NodeID] = s.NodeName
		}
		if s.RoutingState == "STARTED" {
			m.countStarted++
		}
	}
	for _, s := range problemShards(snap.Shards, snap.Allocations) {
		if s.NodeName == "" && s.NodeID != "" {
			s.NodeName = m.nodeName(s.NodeID)
		}
		m.buckets[classifyShard(s)]++
		m.problemShards = append(m.problemShards, s)
	}
	replicas := make(map[string]string, len(snap.Tables))
	filters := make(map[string]map[string]string, len(snap.Tables))
	for _, t := range snap.Tables {
		replicas[t.SchemaName+"."+t.TableName] = t.Settings.NumberOfReplicas
		filters[t.SchemaName+"."+t.TableName] = t.Settings.AllocationFilters
	}
	var gone []string
	for _, n := range snap.Nodes {
		if n.Gone {
			gone = append(gone, n.Name)
		}
	}
	sort.Strings(gone)
	m.fixes = m.fixes[:0]
	for _, s := range m.problemShards {
		a, _ := m.findAllocation(s)
		m.fixes = append(m.fixes, diagnoseShard(s, a, snap.ClusterSettings, replicas[s.SchemaName+"."+s.TableName], gone))
	}
	all := append([][]shardFix(nil), m.fixes...)
	stuckNodes := map[string][]string{}
	for _, a := range snap.StuckShards {
		t, n := a.TableSchema+"."+a.TableName, m.nodeName(a.NodeID)
		if !contains(stuckNodes[t], n) {
			stuckNodes[t] = append(stuckNodes[t], n)
			sort.Strings(stuckNodes[t])
		}
	}
	for _, a := range snap.StuckShards {
		t := a.TableSchema + "." + a.TableName
		all = append(all, []shardFix{stuckFix(a, filters[t], strings.Join(stuckNodes[t], ", "))})
	}
	m.diagnoses = diagnose(all)
	if m.noticeText != "" && time.Since(m.noticeAt) > 5*time.Second {
		m.noticeText = ""
	}

	m.rebuildSorted()
	m.clampSelection()
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
	// title(1) + verdict(1) + counts(1) + diagnoses + blank(1) + column header(1)
	headerLines := 5 + len(m.renderDiagnoses())
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
	m.clampScrollTo(m.listHeight())
}

func (m *ShardsModel) rebuildSorted() {
	m.sorted = m.sorted[:0]
	for i, s := range m.problemShards {
		if m.matches(s.SchemaName + "." + s.TableName) {
			m.sorted = append(m.sorted, i)
		}
	}

	ps := m.problemShards
	sf := m.sortField
	m.sortRows(func(ia, ib int) bool {
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
			ba, bb := classifyShard(ps[ia]), classifyShard(ps[ib])
			if ba == bb {
				less = ps[ia].SchemaName+"."+ps[ia].TableName < ps[ib].SchemaName+"."+ps[ib].TableName
			} else {
				less = ba < bb
			}
		case ShardSortByRecovery:
			less = ps[ia].RecoveryPercent < ps[ib].RecoveryPercent
		case ShardSortBySize:
			less = ps[ia].Size < ps[ib].Size
		default:
			less = ia < ib
		}
		return less
	})
}

func (m ShardsModel) HandleKey(msg tea.KeyMsg) (ShardsModel, tea.Cmd) {
	if !m.searching {
		if m, cmd, ok := m.handleFixKey(msg); ok {
			return m, cmd
		}
	}
	if r := m.handleKey(msg, m.keyMap, shardSortFieldCount); r.handled {
		if r.rebuild {
			m.rebuildSorted()
		}
		if r.moved {
			m.clampScroll()
		}
	}
	return m, nil
}

func (m ShardsModel) View() string {
	stale := m.snap.Staleness["shards"]
	title := styleTitle.Render("Shard Health")
	if stale {
		title += " " + styleStale.Render("(stale)")
	}

	totalShards := len(m.snap.Shards)
	if totalShards == 0 {
		return title + "\n  No shards found"
	}

	var lines []string
	lines = append(lines, title)

	if m.fixTarget != nil {
		return m.renderFixModal()
	}

	// Summary line with counts by state
	summary := m.renderSummary()
	lines = append(lines, summary)
	lines = append(lines, m.renderDiagnoses()...)
	lines = append(lines, "")

	// Happy path: all healthy
	if len(m.problemShards) == 0 {
		result := strings.Join(lines, "\n")
		if stale {
			return styleDim.Render(result)
		}
		return result
	}

	// Search input
	if m.searching {
		lines = append(lines, fmt.Sprintf("  Search: %s▏", m.search))
	}

	// Column header
	sortIndicator := m.sortLabel(shardSortFieldNames[:])

	filterInfo := ""
	if m.search != "" {
		filterInfo = fmt.Sprintf(" | filter: %q (%d match)", m.search, len(m.sorted))
	}

	lines = append(lines, fmt.Sprintf("  Shards (%d) | %s%s | %s",
		len(m.problemShards), sortIndicator, filterInfo,
		styleDim.Render("s:sort  /:search  y:copy fix  x:run fix")))

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
			tableName = truncateString(tableName, 28)

			pr := "R"
			if s.Primary {
				pr = "P"
			}

			b := classifyShard(s)

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
				bucketStyle(b).Render(fmt.Sprintf("%-14s", bucketState(b))),
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

	result := strings.Join(lines, "\n")
	if stale {
		return styleDim.Render(result)
	}
	return result
}

// renderSummary is the verdict line plus the per-bucket counts.
func (m ShardsModel) renderSummary() string {
	lastRefresh := ""
	if t, ok := m.snap.LastUpdated["shards"]; ok && !t.IsZero() {
		ago := time.Since(t).Truncate(time.Second)
		lastRefresh = fmt.Sprintf(" | updated %s ago", ago)
	}

	verdict, style := shardVerdict(m.buckets)
	if verdict == "" {
		return fmt.Sprintf("  %s%s", styleHealthGreen.Render(fmt.Sprintf("All %d shards started", m.countStarted)), styleDim.Render(lastRefresh))
	}
	counts := fmt.Sprintf("%d started", m.countStarted)
	if c := bucketCounts(m.buckets); c != "" {
		counts += " · " + c
	}
	return fmt.Sprintf("  %s%s\n  %s", style.Render(verdict), styleDim.Render(lastRefresh), counts)
}

func (m ShardsModel) fixesFor(s cratedb.ShardInfo) []shardFix {
	for i, p := range m.problemShards {
		if p == s {
			return m.fixes[i]
		}
	}
	return nil
}

func (m ShardsModel) nodeName(id string) string {
	if n, ok := m.nodeNames[id]; ok && n != "" {
		return n
	}
	return id
}

func (m ShardsModel) renderDetail(s cratedb.ShardInfo) string {
	pr := "replica"
	if s.Primary {
		pr = "primary"
	}

	var lines []string
	lines = append(lines, styleTitle.Render(fmt.Sprintf("  Allocation: %s.%s shard %d (%s)",
		s.SchemaName, s.TableName, s.ID, pr)))

	if s.PartitionIdent != "" {
		lines = append(lines, fmt.Sprintf("    Partition: %s", s.PartitionIdent))
	}
	if s.NodeName != "" {
		lines = append(lines, fmt.Sprintf("    Node: %s", s.NodeName))
	}
	b := classifyShard(s)
	lines = append(lines, "    What: "+bucketStyle(b).Render(bucketMeaning(s, b, m.nodeName)))
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

	if a, ok := m.findAllocation(s); ok {
		lines = append(lines, "")
		if a.Explanation != "" {
			lines = append(lines, "    Why: "+a.Explanation)
		}
		for _, g := range groupDecisions(a.Decisions) {
			lines = append(lines, fmt.Sprintf("%s %s: %s",
				styleHealthRed.Render("    x"), strings.Join(g.nodes, ", "), g.explanation))
		}
		for _, f := range m.fixesFor(s) {
			lines = append(lines, "    → "+f.cause)
			if f.stmt != "" {
				lines = append(lines, "      fix: "+styleValue.Render(f.stmt))
			}
		}
	} else if len(m.snap.Allocations) == 0 && len(m.problemShards) > 0 {
		lines = append(lines, "")
		lines = append(lines, styleDim.Render("    (allocation detail unavailable)"))
	}

	return strings.Join(lines, "\n")
}

// findAllocation returns the sys.allocations row for s. Copies of one shard
// share the reasons, so the first match is enough.
func (m ShardsModel) findAllocation(s cratedb.ShardInfo) (cratedb.AllocationInfo, bool) {
	for _, a := range m.snap.Allocations {
		if a.TableSchema == s.SchemaName &&
			a.TableName == s.TableName &&
			a.PartitionIdent == s.PartitionIdent &&
			a.ShardID == s.ID &&
			a.Primary == s.Primary {
			return a, true
		}
	}
	return cratedb.AllocationInfo{}, false
}

type decisionGroup struct {
	explanation string
	nodes       []string
}

// groupDecisions folds nodes that refuse for the same reason into one line.
// Explanations end in a node-specific shard routing dump ("... [[.partitioned
// .t.04166][0], node[...], ...]]"), which is cut so equal reasons match.
func groupDecisions(decisions []cratedb.AllocationDecision) []decisionGroup {
	var groups []decisionGroup
	idx := map[string]int{}
	for _, d := range decisions {
		for _, e := range d.Explanations {
			if i := strings.Index(e, " [["); i > 0 {
				e = e[:i]
			}
			i, ok := idx[e]
			if !ok {
				i = len(groups)
				idx[e] = i
				groups = append(groups, decisionGroup{explanation: e})
			}
			groups[i].nodes = append(groups[i].nodes, d.NodeName)
		}
	}
	return groups
}
