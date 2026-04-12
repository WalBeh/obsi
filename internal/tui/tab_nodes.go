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

// NodeSortField defines what column to sort nodes by.
type NodeSortField int

const (
	NodeSortByName NodeSortField = iota
	NodeSortByCPU
	NodeSortByHeap
	NodeSortBySat
	NodeSortByLoad
	NodeSortByIO
	nodeSortFieldCount
)

var nodeSortFieldNames = [nodeSortFieldCount]string{"name", "cpu", "heap", "sat", "load", "io"}

// NodesModel shows detailed per-node metrics.
type NodesModel struct {
	snap      store.StoreSnapshot
	sorted    []int // indices into snap.Nodes after sort+filter
	selected  int
	scroll    int
	sortField NodeSortField
	sortDesc  bool
	searching bool
	search    string
	width     int
	height    int
}

func NewNodesModel(width, height int) NodesModel {
	return NodesModel{width: width, height: height}
}

func (m NodesModel) Refresh(snap store.StoreSnapshot) NodesModel {
	m.snap = snap
	m.rebuildSorted()
	if m.selected >= len(m.sorted) && len(m.sorted) > 0 {
		m.selected = len(m.sorted) - 1
	}
	m.clampScroll()
	return m
}

func (m NodesModel) SetSize(width, height int) NodesModel {
	m.width = width
	m.height = height
	m.clampScroll()
	return m
}

func (m NodesModel) listHeight() int {
	headerLines := 3 // title + summary + column header
	if m.searching {
		headerLines += 2
	}
	detailReserve := m.height * 45 / 100
	if detailReserve < 10 {
		detailReserve = 10
	}
	listH := m.height - headerLines - detailReserve
	if listH < 3 {
		listH = 3
	}
	return listH
}

func (m *NodesModel) clampScroll() {
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

func (m *NodesModel) rebuildSorted() {
	m.sorted = m.sorted[:0]
	for i, n := range m.snap.Nodes {
		if m.search != "" {
			searchable := strings.ToLower(n.Name + " " + n.Zone + " " + n.NodeRole)
			if !strings.Contains(searchable, strings.ToLower(m.search)) {
				continue
			}
		}
		m.sorted = append(m.sorted, i)
	}

	nodes := m.snap.Nodes
	sf := m.sortField
	desc := m.sortDesc
	sort.Slice(m.sorted, func(a, b int) bool {
		ia, ib := m.sorted[a], m.sorted[b]
		na, nb := nodes[ia], nodes[ib]
		var less bool
		switch sf {
		case NodeSortByName:
			less = na.Name < nb.Name
		case NodeSortByCPU:
			less = na.CPUPercent < nb.CPUPercent
		case NodeSortByHeap:
			ha, hb := float64(0), float64(0)
			if na.HeapMax > 0 {
				ha = float64(na.HeapUsed) / float64(na.HeapMax)
			}
			if nb.HeapMax > 0 {
				hb = float64(nb.HeapUsed) / float64(nb.HeapMax)
			}
			less = ha < hb
		case NodeSortBySat:
			sa, sb := float64(0), float64(0)
			if na.NumCPUs > 0 {
				sa = na.Load[0] / float64(na.NumCPUs)
			}
			if nb.NumCPUs > 0 {
				sb = nb.Load[0] / float64(nb.NumCPUs)
			}
			less = sa < sb
		case NodeSortByLoad:
			less = na.Load[0] < nb.Load[0]
		case NodeSortByIO:
			less = (na.ReadThroughput + na.WriteThroughput) < (nb.ReadThroughput + nb.WriteThroughput)
		default:
			less = ia < ib
		}
		if desc {
			return !less
		}
		return less
	})
}

func (m NodesModel) HandleKey(msg tea.KeyMsg) (NodesModel, tea.Cmd) {
	km := DefaultKeyMap()

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
		m.sortField = (m.sortField + 1) % nodeSortFieldCount
		if m.sortField == oldField {
			m.sortDesc = !m.sortDesc
		}
		if m.sortField != oldField {
			m.sortDesc = m.sortField != NodeSortByName
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

func (m NodesModel) View() string {
	title := sectionTitle("Nodes")

	if m.snap.Staleness["nodes"] {
		return title + "\n" + styleStale.Render("  (stale data)")
	}

	if len(m.snap.Nodes) == 0 {
		return title + "\n  No nodes available"
	}

	var lines []string
	lines = append(lines, title)

	// Summary + sort/search
	sortIndicator := fmt.Sprintf("sort: %s", nodeSortFieldNames[m.sortField])
	if m.sortDesc {
		sortIndicator += " ↓"
	} else {
		sortIndicator += " ↑"
	}

	filterInfo := ""
	if m.search != "" {
		filterInfo = fmt.Sprintf(" │ filter: %q (%d match)", m.search, len(m.sorted))
	} else if m.searching {
		filterInfo = " │ filter: _"
	}

	lastRefresh := ""
	if t, ok := m.snap.LastUpdated["nodes"]; ok && !t.IsZero() {
		ago := time.Since(t).Truncate(time.Second)
		lastRefresh = fmt.Sprintf(" │ updated %s ago", ago)
	}

	lines = append(lines, fmt.Sprintf("  %d nodes │ %s%s%s │ %s",
		len(m.snap.Nodes),
		sortIndicator, filterInfo,
		styleDim.Render(lastRefresh),
		styleDim.Render("s:sort  /:search  ctrl+r:refresh")))

	if m.searching {
		lines = append(lines, fmt.Sprintf("  Search: %s▏", m.search))
	}

	// Column header
	header := styleHeader.Render(fmt.Sprintf("  %-3s %-20s %6s %6s %6s %6s %5s %10s %15s",
		"", m.nodeSortHeader("NAME", NodeSortByName),
		m.nodeSortHeader("CPU%", NodeSortByCPU),
		m.nodeSortHeader("HEAP%", NodeSortByHeap),
		m.nodeSortHeader("SAT%", NodeSortBySat),
		m.nodeSortHeader("LOAD1", NodeSortByLoad),
		"TPOOL",
		m.nodeSortHeader("IOPS r/w", NodeSortByIO),
		"DISK r/w"))
	lines = append(lines, header)

	if len(m.sorted) == 0 {
		lines = append(lines, "  No matching nodes")
		return strings.Join(lines, "\n")
	}

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
		nodeIdx := m.sorted[viewIdx]
		n := m.snap.Nodes[nodeIdx]

		marker := "  "
		if viewIdx == m.selected {
			marker = "▸ "
		}

		// Gone node
		if n.Gone {
			ago := time.Since(n.LastSeen).Truncate(time.Second)
			lines = append(lines, fmt.Sprintf("%s%s %s",
				marker,
				styleHealthRed.Render(fmt.Sprintf("%-20s", n.Name+" ✗")),
				styleStale.Render(fmt.Sprintf("gone — last seen %s ago", ago))))
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

		indicator := styleHealthGreen.Render("●")
		nodeName := padNodeName(n.Name, n.IsMaster, 20)

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

		cpuStr := formatCPU(n.CPUPercent)
		iopsStr := fmt.Sprintf("%.0f/%.0f", n.ReadIOPS, n.WriteIOPS)

		tpoolFlag := threadPoolFlag(n)

		diskStr := fmt.Sprintf("%s/%s", formatRate(n.ReadThroughput), formatRate(n.WriteThroughput))

		row := fmt.Sprintf("%s%s %s %s %s %s %6.2f %5s %10s %15s",
			marker, indicator, nodeName,
			cpuStyle.Render(fmt.Sprintf("%6s", cpuStr)),
			heapStyle.Render(fmt.Sprintf("%6.1f", heapPct)),
			satStyle.Render(fmt.Sprintf("%6.0f", loadSat)),
			n.Load[0],
			tpoolFlag,
			iopsStr, diskStr,
		)
		lines = append(lines, row)
	}

	remaining := len(m.sorted) - visibleEnd
	if remaining > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↓ %d more below", remaining)))
	}

	// Detail panel (always visible)
	if m.selected < len(m.sorted) {
		lines = append(lines, "")
		lines = append(lines, m.renderDetail(m.snap.Nodes[m.sorted[m.selected]]))
	}

	return strings.Join(lines, "\n")
}

// threadPoolFlag returns a colored pressure indicator for the node list.
// Dim "·" when healthy, yellow "▲" when queue > 0, red "▲" when new rejections detected.
func threadPoolFlag(n store.NodeSnapshot) string {
	if n.ThreadPoolNewRejections > 0 {
		return styleHighValue.Render("▲ REJ")
	}
	var totalQueue int64
	for _, p := range n.ThreadPools {
		if p.Name == "write" || p.Name == "search" || p.Name == "generic" {
			totalQueue += p.Queue
		}
	}
	if totalQueue > 0 {
		return styleHealthYellow.Render(fmt.Sprintf("▲ Q%d", totalQueue))
	}
	return styleDim.Render("  ·  ")
}

// getThreadPool returns the stats for a named pool, or nil if not found.
func getThreadPool(pools []cratedb.ThreadPoolStats, name string) *cratedb.ThreadPoolStats {
	for i := range pools {
		if pools[i].Name == name {
			return &pools[i]
		}
	}
	return nil
}

func (m NodesModel) nodeSortHeader(label string, field NodeSortField) string {
	if m.sortField == field {
		arrow := "↑"
		if m.sortDesc {
			arrow = "↓"
		}
		return fmt.Sprintf("%s %s", label, arrow)
	}
	return label
}

func (m NodesModel) renderDetail(n store.NodeSnapshot) string {
	if n.Gone {
		var lines []string
		lines = append(lines, styleTitle.Render(fmt.Sprintf("  Node: %s", n.Name)))
		ago := time.Since(n.LastSeen).Truncate(time.Second)
		lines = append(lines, styleHealthRed.Render(fmt.Sprintf("    Node left the cluster %s ago", ago)))
		if n.Zone != "" {
			lines = append(lines, styleDim.Render(fmt.Sprintf("    Was in zone: %s", n.Zone)))
		}
		return strings.Join(lines, "\n")
	}

	heapPct := float64(0)
	if n.HeapMax > 0 {
		heapPct = float64(n.HeapUsed) / float64(n.HeapMax) * 100
	}
	memPct := float64(0)
	if n.MemTotal > 0 {
		memPct = float64(n.MemUsed) / float64(n.MemTotal) * 100
	}
	fsPct := float64(0)
	if n.FSTotal > 0 {
		fsPct = float64(n.FSUsed) / float64(n.FSTotal) * 100
	}
	loadSat := float64(0)
	if n.NumCPUs > 0 {
		loadSat = n.Load[0] / float64(n.NumCPUs) * 100
	}

	barWidth := 25

	var lines []string
	nameLabel := n.Name
	if n.IsMaster {
		nameLabel = n.Name + " " + stylePrimary.Render("★ master")
	}
	lines = append(lines, styleTitle.Render(fmt.Sprintf("  Node Detail: %s", nameLabel)))

	shortHost := shortenHostname(n.Hostname, 40)
	var infoLine string
	if n.Zone != "" {
		infoLine = fmt.Sprintf("    %s │ %s │ %s │ %d CPUs │ JVM %s", n.Version, shortHost, n.Zone, n.NumCPUs, n.JVMVersion)
	} else {
		infoLine = fmt.Sprintf("    %s │ %s │ %d CPUs │ JVM %s", n.Version, shortHost, n.NumCPUs, n.JVMVersion)
	}
	lines = append(lines, styleDim.Render(infoLine))
	lines = append(lines, "")

	// Process CPU bar + sparkline
	cpuLabel := fmt.Sprintf("%3s%%", formatCPU(n.CPUPercent))
	cpuSpark := ""
	if hist, ok := m.snap.NodeCPUHistory[n.ID]; ok && len(hist) > 1 {
		cpuSpark = " " + sparkline(hist, 30)
	}
	lines = append(lines, fmt.Sprintf("    Process CPU  %s %s %s", metricBar(float64(n.CPUPercent), barWidth), cpuLabel, styleDim.Render(cpuSpark)))

	// Load saturation bar + sparkline
	satLabel := fmt.Sprintf("%.0f%%", loadSat)
	satDetail := fmt.Sprintf("(%.2f / %d CPUs)", n.Load[0], n.NumCPUs)
	satSpark := ""
	if hist, ok := m.snap.NodeLoadSatHistory[n.ID]; ok && len(hist) > 1 {
		satSpark = " " + sparkline(hist, 30)
	}
	lines = append(lines, fmt.Sprintf("    Load / CPUs  %s %4s %s %s", metricBar(loadSat, barWidth), satLabel, styleDim.Render(satDetail), styleDim.Render(satSpark)))

	// Load average
	loadSpark := ""
	if hist, ok := m.snap.NodeLoadHistory[n.ID]; ok && len(hist) > 1 {
		loadSpark = " " + sparkline(hist, 30)
	}
	lines = append(lines, fmt.Sprintf("    Load avg     1m: %.2f   5m: %.2f   15m: %.2f %s",
		n.Load[0], n.Load[1], n.Load[2], styleDim.Render(loadSpark)))

	// Heap bar + sparkline
	heapLabel := fmt.Sprintf("%.1f%%", heapPct)
	heapDetail := fmt.Sprintf("(%s / %s)", formatBytes(n.HeapUsed), formatBytes(n.HeapMax))
	heapSpark := ""
	if hist, ok := m.snap.NodeHeapHistory[n.ID]; ok && len(hist) > 1 {
		heapSpark = " " + sparkline(hist, 30)
	}
	lines = append(lines, fmt.Sprintf("    Heap         %s %5s %s %s", metricBar(heapPct, barWidth), heapLabel, styleDim.Render(heapDetail), styleDim.Render(heapSpark)))

	// Memory bar
	memLabel := fmt.Sprintf("%.1f%%", memPct)
	memDetail := fmt.Sprintf("(%s / %s)", formatBytes(n.MemUsed), formatBytes(n.MemTotal))
	lines = append(lines, fmt.Sprintf("    Memory       %s %5s %s", metricBar(memPct, barWidth), memLabel, styleDim.Render(memDetail)))

	// Disk bar with watermark markers
	cs := m.snap.ClusterSettings
	fsLabel := fmt.Sprintf("%.1f%%", fsPct)
	fsDetail := fmt.Sprintf("(%s / %s, avail %s)", formatBytes(n.FSUsed), formatBytes(n.FSTotal), formatBytes(n.FSAvail))
	if cs.DiskWatermarkLow != "" {
		lines = append(lines, fmt.Sprintf("    Disk         %s %5s %s", diskWatermarkBar(fsPct, cs, barWidth), fsLabel, styleDim.Render(fsDetail)))
	} else {
		lines = append(lines, fmt.Sprintf("    Disk         %s %5s %s", metricBar(fsPct, barWidth), fsLabel, styleDim.Render(fsDetail)))
	}

	// Disk IO: throughput + IOPS with sparklines
	lines = append(lines, "")

	// Check if IOPS counters are available (some platforms don't expose them)
	hasReadIOPS, hasWriteIOPS := false, false
	if hist, ok := m.snap.NodeReadIOPSHistory[n.ID]; ok {
		for _, v := range hist {
			if v > 0 {
				hasReadIOPS = true
				break
			}
		}
	}
	if hist, ok := m.snap.NodeWriteIOPSHistory[n.ID]; ok {
		for _, v := range hist {
			if v > 0 {
				hasWriteIOPS = true
				break
			}
		}
	}
	hasIOPS := hasReadIOPS || hasWriteIOPS

	readTPSpark, readTPStats := "", ""
	if hist, ok := m.snap.NodeReadTPHistory[n.ID]; ok && len(hist) > 1 {
		readTPSpark = sparkline(hist, 20)
		mx, av, p := historyStats(hist)
		readTPStats = fmt.Sprintf("%s/%s/%s", formatRate(av), formatRate(p), formatRate(mx))
	}
	writeTPSpark, writeTPStats := "", ""
	if hist, ok := m.snap.NodeWriteTPHistory[n.ID]; ok && len(hist) > 1 {
		writeTPSpark = sparkline(hist, 20)
		mx, av, p := historyStats(hist)
		writeTPStats = fmt.Sprintf("%s/%s/%s", formatRate(av), formatRate(p), formatRate(mx))
	}

	if hasIOPS {
		lines = append(lines, fmt.Sprintf("    Disk read    %9s/s %s %s",
			formatRate(n.ReadThroughput),
			styleDim.Render(readTPSpark), styleDim.Render(readTPStats)))
		lines = append(lines, fmt.Sprintf("    Disk write   %9s/s %s %s",
			formatRate(n.WriteThroughput),
			styleDim.Render(writeTPSpark), styleDim.Render(writeTPStats)))

		readIOPSSpark, readIOPSStats := "", ""
		if hist, ok := m.snap.NodeReadIOPSHistory[n.ID]; ok && len(hist) > 1 {
			readIOPSSpark = sparkline(hist, 20)
			mx, av, p := historyStats(hist)
			readIOPSStats = fmt.Sprintf("%s/%s/%s", formatIOPS(av), formatIOPS(p), formatIOPS(mx))
		}
		writeIOPSSpark, writeIOPSStats := "", ""
		if hist, ok := m.snap.NodeWriteIOPSHistory[n.ID]; ok && len(hist) > 1 {
			writeIOPSSpark = sparkline(hist, 20)
			mx, av, p := historyStats(hist)
			writeIOPSStats = fmt.Sprintf("%s/%s/%s", formatIOPS(av), formatIOPS(p), formatIOPS(mx))
		}
		lines = append(lines, fmt.Sprintf("    IOPS read    %9s %s %s",
			formatIOPS(n.ReadIOPS),
			styleDim.Render(readIOPSSpark), styleDim.Render(readIOPSStats)))
		lines = append(lines, fmt.Sprintf("    IOPS write   %9s %s %s",
			formatIOPS(n.WriteIOPS),
			styleDim.Render(writeIOPSSpark), styleDim.Render(writeIOPSStats)))
	} else {
		lines = append(lines, fmt.Sprintf("    Disk read    %9s/s %s %s",
			formatRate(n.ReadThroughput),
			styleDim.Render(readTPSpark), styleDim.Render(readTPStats)))
		lines = append(lines, fmt.Sprintf("    Disk write   %9s/s %s %s",
			formatRate(n.WriteThroughput),
			styleDim.Render(writeTPSpark), styleDim.Render(writeTPStats)))
	}

	// Thread pools
	if len(n.ThreadPools) > 0 {
		lines = append(lines, "")
		rejLabel := "rejected"
		if n.ThreadPoolNewRejections > 0 {
			rejLabel = styleHighValue.Render("rejected*")
		}
		lines = append(lines, styleDim.Render(fmt.Sprintf("    Thread Pools       active  queue  %s  completed", rejLabel)))
		for _, name := range []string{"write", "search", "generic"} {
			if p := getThreadPool(n.ThreadPools, name); p != nil {
				queueStyle := styleDim
				if p.Queue > 0 {
					queueStyle = styleHealthYellow
				}
				rejStr := fmt.Sprintf("%8d", p.Rejected)
				if p.Rejected > 0 {
					rejStr = styleDim.Render(rejStr) // counter, not alarming by itself
				} else {
					rejStr = styleDim.Render(rejStr)
				}
				lines = append(lines, fmt.Sprintf("    %-18s %5d  %s  %s  %9d",
					name,
					p.Active,
					queueStyle.Render(fmt.Sprintf("%5d", p.Queue)),
					rejStr,
					p.Completed))
			}
		}
		if n.ThreadPoolNewRejections > 0 {
			lines = append(lines, styleHighValue.Render(fmt.Sprintf("    * %d new rejections since last poll", n.ThreadPoolNewRejections)))
		}
	}

	// Latency (if direct reachable)
	if n.LastLatency > 0 {
		lines = append(lines, fmt.Sprintf("    Latency      %s (direct)", n.LastLatency.Truncate(1e6)))
	}

	return strings.Join(lines, "\n")
}
