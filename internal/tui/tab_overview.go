package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// OverviewModel shows cluster health checks and a node summary.
type OverviewModel struct {
	snap   store.StoreSnapshot
	scroll int
	lines  []string // cached rendered lines
	width  int
	height int
	keyMap KeyMap
	editor settingsEditor
}

func NewOverviewModel(width, height int, persistent bool) OverviewModel {
	return OverviewModel{width: width, height: height, keyMap: DefaultKeyMap(), editor: newSettingsEditor(persistent)}
}

func (m OverviewModel) Refresh(snap store.StoreSnapshot) OverviewModel {
	m.snap = snap
	m.editor.syncFromSettings(snap.ClusterSettings)
	// Re-render all sections into lines
	var sections []string
	sections = append(sections, m.renderClusterSettings())
	sections = append(sections, m.renderChecks())
	sections = append(sections, m.renderNodeSummary())
	sections = append(sections, m.renderTableHealth())
	sections = append(sections, m.renderSummit())
	content := strings.Join(sections, "\n\n")
	m.lines = strings.Split(content, "\n")
	m.clampScroll()
	return m
}

func (m OverviewModel) SetSize(width, height int) OverviewModel {
	m.width = width
	m.height = height
	m.clampScroll()
	return m
}

func (m OverviewModel) HandleKey(msg tea.KeyMsg) (OverviewModel, tea.Cmd) {
	// Delegate to settings editor first
	editor, cmd, consumed := m.editor.handleKey(msg)
	m.editor = editor
	if consumed {
		return m, cmd
	}

	km := m.keyMap
	switch {
	case key.Matches(msg, km.Up):
		if m.scroll > 0 {
			m.scroll--
		}
	case key.Matches(msg, km.Down):
		m.scroll++
		m.clampScroll()
	}
	return m, nil
}

func (m *OverviewModel) clampScroll() {
	maxScroll := len(m.lines) - m.height
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

func (m OverviewModel) View() string {
	if len(m.lines) == 0 {
		return "Loading..."
	}

	// Virtual scroll: show only what fits
	end := m.scroll + m.height
	if end > len(m.lines) {
		end = len(m.lines)
	}

	visible := m.lines[m.scroll:end]

	var result []string
	if m.scroll > 0 {
		result = append(result, styleDim.Render(fmt.Sprintf("  ↑ scroll up (%d lines above)", m.scroll)))
		if len(visible) > 0 {
			visible = visible[0 : len(visible)-1] // make room for indicator
		}
	}
	result = append(result, visible...)
	remaining := len(m.lines) - end
	if remaining > 0 {
		result = append(result, styleDim.Render(fmt.Sprintf("  ↓ scroll down (%d lines below)", remaining)))
	}

	return strings.Join(result, "\n")
}

func (m OverviewModel) renderChecks() string {
	stale := m.snap.Staleness["health"]
	title := sectionTitle("Cluster Health Checks")
	if stale {
		title += " " + styleStale.Render("(stale)")
	}

	if len(m.snap.ClusterChecks) == 0 {
		return title + "\n  No checks available"
	}

	var failed []string
	passedCount := 0
	for _, c := range m.snap.ClusterChecks {
		if c.Passed {
			passedCount++
			continue
		}
		sev := "?"
		style := styleDim
		switch c.Severity {
		case 1:
			sev = "INFO"
			style = styleHealthGreen
		case 2:
			sev = "WARN"
			style = styleHealthYellow
		case 3:
			sev = "CRIT"
			style = styleHealthRed
		}
		failed = append(failed, fmt.Sprintf("  %s %s", style.Render(fmt.Sprintf("[%s]", sev)), c.Description))
	}

	var lines []string
	lines = append(lines, title)

	if len(failed) == 0 {
		lines = append(lines, styleHealthGreen.Render(fmt.Sprintf("  All %d checks passed", passedCount)))
	} else {
		lines = append(lines, fmt.Sprintf("  %s passed, %s failed",
			styleHealthGreen.Render(fmt.Sprintf("%d", passedCount)),
			styleHealthRed.Render(fmt.Sprintf("%d", len(failed)))))
		lines = append(lines, failed...)
	}

	result := strings.Join(lines, "\n")
	if stale {
		return styleDim.Render(result)
	}
	return result
}

func (m OverviewModel) renderNodeSummary() string {
	stale := m.snap.Staleness["nodes"]
	title := sectionTitle("Node Summary")
	if stale {
		title += " " + styleStale.Render("(stale)")
	}

	if len(m.snap.Nodes) == 0 {
		return title + "\n  No nodes available"
	}

	// Detect if any node has zone info
	hasZones := false
	for _, n := range m.snap.Nodes {
		if n.Zone != "" {
			hasZones = true
			break
		}
	}

	// Header
	var headerFmt string
	if hasZones {
		headerFmt = fmt.Sprintf("  %-20s %-12s %6s %6s %6s %5s %10s %8s",
			"NAME", "ZONE", "CPU%", "HEAP%", "SAT%", "CPUs", "HEAP", "LOAD1")
	} else {
		headerFmt = fmt.Sprintf("  %-20s %6s %6s %6s %5s %10s %8s",
			"NAME", "CPU%", "HEAP%", "SAT%", "CPUs", "HEAP", "LOAD1")
	}
	header := styleHeader.Render(headerFmt)

	var rows []string
	rows = append(rows, title)
	rows = append(rows, header)

	for _, n := range m.snap.Nodes {
		// Gone node — show as disappeared
		if n.Gone {
			ago := time.Since(n.LastSeen).Truncate(time.Second)
			goneRow := fmt.Sprintf("  %s %s",
				styleHealthRed.Render(fmt.Sprintf("%-20s", n.Name+" ✗")),
				styleStale.Render(fmt.Sprintf("gone — last seen %s ago", ago)))
			rows = append(rows, goneRow)
			continue
		}

		heapPct := float64(0)
		if n.HeapMax > 0 {
			heapPct = float64(n.HeapUsed) / float64(n.HeapMax) * 100
		}
		loadSat := float64(0)
		if n.NumCPUs > 0 {
			loadSat = n.Load[0] / float64(n.NumCPUs) * 100
		}

		cpuStr := formatCPU(n.CPUPercent)

		cpuStyle := styleValue
		if n.CPUPercent >= 0 {
			if n.CPUPercent > 80 {
				cpuStyle = styleHighValue
			} else if n.CPUPercent > 60 {
				cpuStyle = styleHealthYellow
			}
		} else {
			cpuStyle = styleDim
		}

		heapStyle := styleValue
		if heapPct > 80 {
			heapStyle = styleHighValue
		} else if heapPct > 60 {
			heapStyle = styleHealthYellow
		}

		satStyle := styleValue
		if loadSat > 100 {
			satStyle = styleHighValue
		} else if loadSat > 70 {
			satStyle = styleHealthYellow
		}

		name := padNodeName(n.Name, n.IsMaster, 20)

		// Sparklines
		cpuSpark := ""
		if nh, ok := m.snap.NodeHistory[n.ID]; ok && len(nh.CPU) > 1 {
			cpuSpark = " " + styleDim.Render(sparkline(nh.CPU, 15))
		}

		var row string
		if hasZones {
			row = fmt.Sprintf("  %s %-12s %s %s %s %5d %10s %8.2f%s",
				name, n.Zone,
				cpuStyle.Render(fmt.Sprintf("%6s", cpuStr)),
				heapStyle.Render(fmt.Sprintf("%6.1f", heapPct)),
				satStyle.Render(fmt.Sprintf("%6.0f", loadSat)),
				n.NumCPUs,
				formatBytes(n.HeapUsed),
				n.Load[0],
				cpuSpark,
			)
		} else {
			row = fmt.Sprintf("  %s %s %s %s %5d %10s %8.2f%s",
				name,
				cpuStyle.Render(fmt.Sprintf("%6s", cpuStr)),
				heapStyle.Render(fmt.Sprintf("%6.1f", heapPct)),
				satStyle.Render(fmt.Sprintf("%6.0f", loadSat)),
				n.NumCPUs,
				formatBytes(n.HeapUsed),
				n.Load[0],
				cpuSpark,
			)
		}
		rows = append(rows, row)
	}

	result := strings.Join(rows, "\n")
	if stale {
		return styleDim.Render(result)
	}
	return result
}

func (m OverviewModel) renderTableHealth() string {
	stale := m.snap.Staleness["health"]
	title := sectionTitle("Table Health")
	if stale {
		title += " " + styleStale.Render("(stale)")
	}

	// Count by health status
	counts := map[string]int{}
	for _, h := range m.snap.TableHealth {
		counts[h.Health]++
	}

	green := counts["GREEN"]
	yellow := counts["YELLOW"]
	red := counts["RED"]

	summary := fmt.Sprintf("  %s %s %s",
		styleHealthGreen.Render(fmt.Sprintf("%d GREEN", green)),
		styleHealthYellow.Render(fmt.Sprintf("%d YELLOW", yellow)),
		styleHealthRed.Render(fmt.Sprintf("%d RED", red)))

	lines := []string{title, summary}

	// Show non-green tables
	for _, h := range m.snap.TableHealth {
		if h.Health == "GREEN" {
			continue
		}
		style := healthStyle(h.Health)
		detail := fmt.Sprintf("  %s %s.%s (missing: %d, underreplicated: %d)",
			style.Render(h.Health),
			h.TableSchema, h.TableName,
			h.MissingShards, h.UnderReplicated)
		lines = append(lines, detail)
	}

	result := strings.Join(lines, "\n")
	if stale {
		return styleDim.Render(result)
	}
	return result
}

func (m OverviewModel) renderClusterSettings() string {
	cs := m.snap.ClusterSettings
	if cs.MaxShardsPerNode == 0 && cs.AllocationEnable == "" && cs.RebalanceEnable == "" {
		return sectionTitle("Cluster Settings") + "\n" + styleStale.Render("  (loading...)")
	}

	title := sectionTitle("Cluster Settings")

	// Allocation status — warn if not "all"
	allocStyle := styleHealthGreen
	if cs.AllocationEnable != "all" {
		allocStyle = styleHealthYellowBold
	}
	rebalanceStyle := styleHealthGreen
	if cs.RebalanceEnable != "all" {
		rebalanceStyle = styleHealthYellowBold
	}

	var lines []string
	lines = append(lines, title)

	// Data size and shard counts
	var primarySize, totalSize int64
	var primaryShards, replicaShards int
	for _, t := range m.snap.Tables {
		primarySize += t.TotalSize
		totalSize += t.TotalDiskSize
		primaryShards += t.PrimaryShards
		replicaShards += t.ReplicaShards
	}
	totalShards := primaryShards + replicaShards
	tableCount := len(m.snap.Tables)
	viewCount := m.snap.ViewCount

	if totalShards > 0 {
		pLabel := fmt.Sprintf("%dp", primaryShards)
		rLabel := fmt.Sprintf("%dr", replicaShards)
		if replicaShards == primaryShards {
			pLabel = styleHealthGreen.Render(pLabel)
			rLabel = styleHealthGreen.Render(rLabel)
		} else if replicaShards < primaryShards {
			rLabel = styleHealthYellowBold.Render(rLabel)
		}
		dataLine := fmt.Sprintf("  Data: %s primary / %s total │ %d shards (%s / %s) │ %d tables",
			formatBytes(primarySize), formatBytes(totalSize),
			totalShards, pLabel, rLabel, tableCount)
		if viewCount > 0 {
			dataLine += fmt.Sprintf(", %d views", viewCount)
		}
		lines = append(lines, dataLine)
	} else if tableCount > 0 || viewCount > 0 {
		dataLine := fmt.Sprintf("  Data: %d tables", tableCount)
		if viewCount > 0 {
			dataLine += fmt.Sprintf(", %d views", viewCount)
		}
		lines = append(lines, dataLine)
	}

	// Node/zone topology and version collection
	nodeCount := 0
	zones := make(map[string]bool)
	versions := make(map[string]bool)
	for _, n := range m.snap.Nodes {
		if n.Gone {
			continue
		}
		nodeCount++
		if n.Zone != "" {
			zones[n.Zone] = true
		}
		if n.Version != "" {
			versions[n.Version] = true
		}
	}
	if len(zones) > 0 {
		zoneList := make([]string, 0, len(zones))
		for z := range zones {
			zoneList = append(zoneList, z)
		}
		sort.Strings(zoneList)
		lines = append(lines, fmt.Sprintf("  Topology: %d nodes / %d zones (%s)",
			nodeCount, len(zones), strings.Join(zoneList, ", ")))
	} else {
		lines = append(lines, fmt.Sprintf("  Topology: %d nodes", nodeCount))
	}
	if len(versions) > 0 {
		vList := make([]string, 0, len(versions))
		for v := range versions {
			vList = append(vList, v)
		}
		sort.Strings(vList)
		versionStr := strings.Join(vList, " ")
		if len(versions) > 1 {
			versionStr = styleHealthYellowBold.Render(versionStr)
		}
		lines = append(lines, fmt.Sprintf("  CrateDB: %s", versionStr))
	}

	lines = append(lines, "")

	// Editable settings — use editor rendering when in edit mode
	wmLow := m.editor.renderValue(slotWMLow, cs.DiskWatermarkLow)
	wmHigh := m.editor.renderValue(slotWMHigh, cs.DiskWatermarkHigh)
	wmFlood := m.editor.renderValue(slotWMFlood, cs.DiskWatermarkFlood)
	lines = append(lines, fmt.Sprintf("  Watermarks: %s │ %s │ %s", wmLow, wmHigh, wmFlood))

	maxClusterShards := cs.MaxShardsPerNode * nodeCount
	shardStyle := styleHealthGreen
	if maxClusterShards > 0 && totalShards >= maxClusterShards*80/100 {
		shardStyle = styleHealthYellowBold
	}

	// Per-node shard counts for hotspot detection
	nodeShardsMap := make(map[string]int)
	for _, s := range m.snap.Shards {
		nodeShardsMap[s.NodeName]++
	}
	var hotspotNode string
	var hotspotCount int
	var hotspotOver int
	threshold := cs.MaxShardsPerNode * 80 / 100
	if threshold > 0 {
		for name, count := range nodeShardsMap {
			if count >= threshold {
				if count > hotspotCount {
					if hotspotNode != "" {
						hotspotOver++
					}
					hotspotNode = name
					hotspotCount = count
				} else {
					hotspotOver++
				}
			}
		}
	}
	hotspotSuffix := ""
	if hotspotNode != "" {
		hotspotSuffix = fmt.Sprintf(" ⚠ %s: %d", hotspotNode, hotspotCount)
		if hotspotOver > 0 {
			hotspotSuffix += fmt.Sprintf(" +%d >80%%", hotspotOver)
		}
		hotspotSuffix = " " + styleHealthYellowBold.Render(hotspotSuffix)
	}

	allocVal := m.editor.renderValue(slotAlloc, allocStyle.Render(cs.AllocationEnable))
	rebalanceVal := m.editor.renderValue(slotRebalance, rebalanceStyle.Render(cs.RebalanceEnable))
	maxShardsVal := m.editor.renderValue(slotMaxShards, fmt.Sprintf("%d", cs.MaxShardsPerNode))
	lines = append(lines, fmt.Sprintf("  Allocation: %s │ Rebalance: %s │ Shards: %s/%d (%s/node)%s",
		allocVal, rebalanceVal,
		shardStyle.Render(fmt.Sprintf("%d", totalShards)), maxClusterShards, maxShardsVal, hotspotSuffix))
	// Picker dropdown for allocation/rebalance
	for _, idx := range []int{slotAlloc, slotRebalance} {
		if picker := m.editor.renderPicker(idx, "              "); picker != "" {
			lines = append(lines, picker)
		}
	}

	recoveryBytes := m.editor.renderValue(slotRecoveryBytes, cs.RecoveryMaxBytesPerSec)
	recoveryNode := m.editor.renderValue(slotRecoveryNode, fmt.Sprintf("%d", cs.NodeConcurrentRecoveries))
	recoveryCluster := m.editor.renderValue(slotRecoveryClust, fmt.Sprintf("%d", cs.ClusterConcurrentRebalance))
	lines = append(lines, fmt.Sprintf("  Recovery: %s/s │ %s/node │ %s/cluster",
		recoveryBytes, recoveryNode, recoveryCluster))

	// Edit mode hint and error
	if hint := m.editor.renderEditHint(); hint != "" {
		lines = append(lines, hint)
	}
	if errMsg := m.editor.renderError(); errMsg != "" {
		lines = append(lines, errMsg)
	}

	return strings.Join(lines, "\n")
}

func (m OverviewModel) renderSummit() string {
	s := m.snap.Summit
	if s.Mountain == "" {
		return ""
	}
	ascent := ""
	if s.FirstAscent > 0 {
		ascent = fmt.Sprintf(", first ascent %d", s.FirstAscent)
	}
	return styleDim.Render(fmt.Sprintf("  /\\/\\  %s (%dm) — %s, %s%s",
		s.Mountain, s.Height, s.Region, s.Country, ascent))
}

func formatCPU(pct int16) string {
	if pct < 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d", pct)
}

func formatRate(bytesPerSec float64) string {
	switch {
	case bytesPerSec >= 1<<30:
		return fmt.Sprintf("%.1fGB", bytesPerSec/(1<<30))
	case bytesPerSec >= 1<<20:
		return fmt.Sprintf("%.1fMB", bytesPerSec/(1<<20))
	case bytesPerSec >= 1<<10:
		return fmt.Sprintf("%.1fKB", bytesPerSec/(1<<10))
	case bytesPerSec > 0:
		return fmt.Sprintf("%.0fB", bytesPerSec)
	default:
		return "0"
	}
}

func formatIOPS(iops float64) string {
	switch {
	case iops >= 1000:
		return fmt.Sprintf("%.1fK", iops/1000)
	case iops > 0:
		return fmt.Sprintf("%.0f", iops)
	default:
		return "0"
	}
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
