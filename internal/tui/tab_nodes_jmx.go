package tui

import (
	"fmt"
	"sort"
	"strings"

	"github.com/waltergrande/cratedb-observer/internal/jmx"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// renderContainerMem inserts a single line showing the cgroup memory the
// pod is actually using vs the JVM heap, with the difference labelled
// "native". An RSS that grows independently of heap is the classic
// off-heap leak signal.
func (m NodesModel) renderContainerMem(n store.NodeSnapshot) string {
	snap := m.snap.JMX[n.Hostname]
	if snap == nil || snap.ContainerMemBytes == 0 {
		return ""
	}
	native := snap.ContainerMemBytes - n.HeapUsed
	if native < 0 {
		native = 0
	}
	return fmt.Sprintf("    Container    %s %s",
		formatBytes(snap.ContainerMemBytes),
		styleDim.Render(fmt.Sprintf("(heap %s + %s native)", formatBytes(n.HeapUsed), formatBytes(native))),
	)
}

// renderJMXNetAndDisk inserts the network rx/tx rate lines and the
// per-device disk rate breakdown. Returns nil when no JMX data is
// available, so non-Cloud setups see no change.
func (m NodesModel) renderJMXNetAndDisk(n store.NodeSnapshot) []string {
	rates := m.snap.JMXRates[n.Hostname]
	if rates == nil {
		return nil
	}
	hist := m.snap.JMXHistory[n.Hostname]

	var lines []string

	netRxSpark := ""
	if len(hist.NetRxRate) > 1 {
		netRxSpark = " " + sparkline(hist.NetRxRate, 30)
	}
	netTxSpark := ""
	if len(hist.NetTxRate) > 1 {
		netTxSpark = " " + sparkline(hist.NetTxRate, 30)
	}
	lines = append(lines, fmt.Sprintf("    Net rx       %9s/s %s",
		formatRate(rates.NetRxBytesPerSec), styleDim.Render(netRxSpark)))
	lines = append(lines, fmt.Sprintf("    Net tx       %9s/s %s",
		formatRate(rates.NetTxBytesPerSec), styleDim.Render(netTxSpark)))

	// Per-device disk: skip devices with no recent activity to avoid
	// listing every /dev/sda..sdf on CrateDB Cloud where only data
	// volumes are interesting.
	type devRate struct {
		name string
		r, w float64
	}
	all := map[string]*devRate{}
	for dev, r := range rates.DiskReadPerSec {
		if all[dev] == nil {
			all[dev] = &devRate{name: dev}
		}
		all[dev].r = r
	}
	for dev, w := range rates.DiskWritePerSec {
		if all[dev] == nil {
			all[dev] = &devRate{name: dev}
		}
		all[dev].w = w
	}
	var active []*devRate
	for _, d := range all {
		if d.r > 0 || d.w > 0 {
			active = append(active, d)
		}
	}
	if len(active) > 0 {
		sort.Slice(active, func(i, j int) bool { return active[i].name < active[j].name })
		lines = append(lines, styleDim.Render("    Per-device   r/s         w/s"))
		for _, d := range active {
			lines = append(lines, fmt.Sprintf("    %-12s %9s/s %9s/s",
				strings.TrimPrefix(d.name, "/dev/"),
				formatRate(d.r), formatRate(d.w),
			))
		}
	}

	return lines
}

// renderJMX produces the GC, memory pool, and buffer pool sections of the
// node detail panel. Returns nil when no JMX data is available for this
// node (collector disabled, scrape not yet successful, or pod label
// missing from the scrape).
func (m NodesModel) renderJMX(n store.NodeSnapshot) []string {
	snap := m.snap.JMX[n.Hostname]
	if snap == nil {
		return nil
	}

	var lines []string

	hist := m.snap.JMXHistory[n.Hostname]
	if gcLines := renderGC(snap, hist); len(gcLines) > 0 {
		lines = append(lines, "")
		lines = append(lines, styleDim.Render("    GC"))
		lines = append(lines, gcLines...)
	}

	if poolLines := renderMemoryPools(snap, n.HeapMax); len(poolLines) > 0 {
		lines = append(lines, "")
		lines = append(lines, styleDim.Render("    Memory Pools"))
		lines = append(lines, poolLines...)
	}

	if bufLines := renderBufferPools(snap); len(bufLines) > 0 {
		lines = append(lines, "")
		lines = append(lines, styleDim.Render("    Buffer Pools"))
		lines = append(lines, bufLines...)
	}

	return lines
}

// gcOrder fixes the row order so Young (frequent) is always above Concurrent
// (medium) above Old (rare). Any unknown collector falls to the end.
var gcOrder = []string{"G1 Young Generation", "G1 Concurrent GC", "G1 Old Generation"}

func renderGC(snap *jmx.JMXSnapshot, hist store.JMXHistorySnapshot) []string {
	if len(snap.GC) == 0 {
		return nil
	}
	names := orderedKeys(snap.GC, gcOrder)
	out := make([]string, 0, len(names))
	for _, name := range names {
		g := snap.GC[name]
		lifetimeAvg := 0.0
		if g.Count > 0 {
			lifetimeAvg = g.TotalSeconds * 1000 / float64(g.Count)
		}

		// Recent stats: the headline mean is the weighted average pause
		// computed in the store (Σdsec / Σdcount), which matches the
		// rate(sum)/rate(count) metric used on our Grafana dashboards.
		// The max is the worst per-interval average we saw in the window.
		var recent string
		var spark string
		if ring, ok := hist.GCPauseMs[name]; ok && len(ring) > 0 {
			spark = " " + sparkline(ring, 20)
		}
		if w, ok := hist.GCRecent[name]; ok && w.Collections > 0 {
			rateStr := ""
			if w.RatePerSec > 0 {
				rateStr = fmt.Sprintf(" at %s", formatGCRate(w.RatePerSec))
			}
			recent = styleDim.Render(fmt.Sprintf("│ recent %5.1fms (max %5.1fms)%s", w.MeanPauseMs, w.MaxPauseMs, rateStr))
		} else if spark != "" {
			recent = styleDim.Render("│ recent       —          ")
		}

		if g.Count == 0 {
			out = append(out, fmt.Sprintf("    %-15s %s",
				shortGCName(name),
				styleDim.Render("never"),
			))
			continue
		}
		out = append(out, fmt.Sprintf("    %-15s %6d colls │ %5.1fs total │ avg %5.1fms %s%s",
			shortGCName(name),
			g.Count,
			g.TotalSeconds,
			lifetimeAvg,
			recent,
			styleDim.Render(spark),
		))
	}
	return out
}

// poolOrder sorts pools with the JVM-pressure-relevant ones first (heap),
// then non-heap pools that change infrequently.
var poolOrder = []string{
	"G1 Eden Space",
	"G1 Old Gen",
	"G1 Survivor Space",
	"Metaspace",
	"Compressed Class Space",
	"CodeHeap 'profiled nmethods'",
	"CodeHeap 'non-profiled nmethods'",
	"CodeHeap 'non-nmethods'",
}

// heapPoolPrefix identifies pools that participate in heap_max budgeting.
// Showing a percentage bar only makes sense for these — non-heap pools
// (Metaspace, CodeHeap, Compressed Class) have no shared denominator.
const heapPoolPrefix = "G1 "

func renderMemoryPools(snap *jmx.JMXSnapshot, heapMax int64) []string {
	if len(snap.Pools) == 0 {
		return nil
	}
	names := orderedKeys(snap.Pools, poolOrder)
	out := make([]string, 0, len(names))
	for _, name := range names {
		used := snap.Pools[name]
		label := shortPoolName(name)
		if strings.HasPrefix(name, heapPoolPrefix) && heapMax > 0 {
			pct := float64(used) / float64(heapMax) * 100
			out = append(out, fmt.Sprintf("    %-20s %s %5.1f%% %s",
				label,
				metricBar(pct, 16),
				pct,
				styleDim.Render(fmt.Sprintf("(%s / %s)", formatBytes(used), formatBytes(heapMax))),
			))
		} else {
			out = append(out, fmt.Sprintf("    %-20s %s",
				label,
				styleDim.Render(formatBytes(used)),
			))
		}
	}
	return out
}

func renderBufferPools(snap *jmx.JMXSnapshot) []string {
	if len(snap.BufferPools) == 0 {
		return nil
	}
	// Skip the "non-volatile memory" pool — almost always zero, just noise.
	keep := []string{"direct", "mapped"}
	out := make([]string, 0, len(keep))
	for _, name := range keep {
		used, ok := snap.BufferPools[name]
		if !ok {
			continue
		}
		out = append(out, fmt.Sprintf("    %-20s %s used",
			capitalize(name),
			styleDim.Render(formatBytes(used)),
		))
	}
	return out
}

// orderedKeys returns the keys of m sorted by the preferred order, with any
// keys not present in preferred appended alphabetically at the end.
func orderedKeys[V any](m map[string]V, preferred []string) []string {
	seen := make(map[string]bool, len(preferred))
	out := make([]string, 0, len(m))
	for _, k := range preferred {
		if _, ok := m[k]; ok {
			out = append(out, k)
			seen[k] = true
		}
	}
	var rest []string
	for k := range m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

// capitalize uppercases the first byte. Sufficient for the small set of
// pure-ASCII labels we use here ("direct" → "Direct", "mapped" → "Mapped").
func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// formatGCRate picks a readable unit so a sparse Old Gen ("once an hour")
// and a busy Young ("a few per second") both display cleanly.
func formatGCRate(perSec float64) string {
	switch {
	case perSec >= 1:
		return fmt.Sprintf("%.2f/s", perSec)
	case perSec*60 >= 1:
		return fmt.Sprintf("%.1f/min", perSec*60)
	default:
		return fmt.Sprintf("%.2f/hr", perSec*3600)
	}
}

// shortGCName trims the verbose collector names so they fit the label column.
func shortGCName(name string) string {
	switch name {
	case "G1 Young Generation":
		return "G1 Young"
	case "G1 Old Generation":
		return "G1 Old"
	case "G1 Concurrent GC":
		return "G1 Concurrent"
	default:
		return name
	}
}

// shortPoolName collapses the unwieldy JMX pool names. The CodeHeap and
// Compressed Class entries are particularly verbose in the upstream output.
func shortPoolName(name string) string {
	switch name {
	case "G1 Survivor Space":
		return "G1 Survivor"
	case "Compressed Class Space":
		return "Compressed Class"
	case "CodeHeap 'profiled nmethods'":
		return "CodeHeap (prof)"
	case "CodeHeap 'non-profiled nmethods'":
		return "CodeHeap (non-prof)"
	case "CodeHeap 'non-nmethods'":
		return "CodeHeap (non-nm)"
	default:
		return name
	}
}
