package tui

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// checkSeverity names sys.checks and sys.node_checks severities: CrateDB's
// SysCheck.Severity is LOW(1), MEDIUM(2), HIGH(3).
func checkSeverity(sev int) (string, lipgloss.Style) {
	switch sev {
	case 1:
		return "LOW", styleDim
	case 2:
		return "WARN", styleHealthYellow
	case 3:
		return "CRIT", styleHealthRed
	}
	return "?", styleDim
}

// nodeCheckFixes says what to do about the checks where that isn't obvious
// from cratedb.NodeCheckTitle.
var nodeCheckFixes = map[int]string{
	5: "free space or add a node; raising the watermark only buys time",
	6: "free space or add a node before the high watermark stops allocation",
	7: "tables on this node are read-only: free space now",
	8: "merge small tables or partitions, fewer shards per table; the limit is a guard rail",
}

// renderClusterStatus shows sys.cluster_health: the cluster-wide health, the
// pending cluster-state tasks and the shard counts. Empty until it answered.
func (m OverviewModel) renderClusterStatus() string {
	h := m.snap.ClusterStatus
	if h == nil {
		return ""
	}
	state := styleHealthGreen
	switch h.Health {
	case "YELLOW":
		state = styleHealthYellow
	case "RED":
		state = styleHealthRed
	}
	count := func(n int64) string {
		if n < 0 {
			return styleHealthRed.Render("unknown")
		}
		if n == 0 {
			return "0"
		}
		return styleHealthYellow.Render(fmt.Sprintf("%d", n))
	}
	lines := []string{
		sectionTitle("Cluster"),
		fmt.Sprintf("  %s   pending tasks %d   missing shards %s   underreplicated %s",
			state.Render(h.Health), h.PendingTasks, count(h.MissingShards), count(h.UnderReplicated)),
	}
	if d := strings.TrimSpace(h.Description); d != "" {
		lines = append(lines, "  "+styleDim.Render(d))
	}
	return strings.Join(lines, "\n")
}

// renderNodeChecks folds sys.node_checks over the nodes: one entry per failing
// check with the nodes it fails on.
func (m OverviewModel) renderNodeChecks() string {
	title := sectionTitle("Node Checks")
	if len(m.snap.NodeChecks) == 0 {
		return title + "\n  " + styleDim.Render("not available")
	}
	names := make(map[string]string, len(m.snap.Nodes))
	for _, n := range m.snap.Nodes {
		names[n.ID] = shortenHostname(n.Name, 24)
	}
	type group struct {
		severity     int
		desc         string
		failed       []string
		acknowledged int
	}
	groups := map[int]*group{}
	nodes := map[string]bool{}
	for _, c := range m.snap.NodeChecks {
		nodes[c.NodeID] = true
		g := groups[c.ID]
		if g == nil {
			g = &group{desc: cratedb.NodeCheckTitle(c.ID, c.Description)}
			groups[c.ID] = g
		}
		if c.Passed {
			continue
		}
		if c.Acknowledged {
			g.acknowledged++
			continue
		}
		name := names[c.NodeID]
		if name == "" {
			name = c.NodeID
		}
		g.failed = append(g.failed, name)
		g.severity = max(g.severity, c.Severity)
	}

	ids := make([]int, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	passed, failedIDs := 0, 0
	var detail []string
	for _, id := range ids {
		g := groups[id]
		if len(g.failed) == 0 {
			passed++
			continue
		}
		failedIDs++
		sort.Strings(g.failed)
		label, style := checkSeverity(g.severity)
		detail = append(detail, fmt.Sprintf("  %s #%d %s on %d of %d nodes",
			style.Render("["+label+"]"), id, g.desc, len(g.failed), len(nodes)))
		detail = append(detail, "         "+strings.Join(g.failed, "  "))
		if fix, ok := nodeCheckFixes[id]; ok {
			detail = append(detail, "         "+styleDim.Render("fix: "+fix))
		}
	}

	var lines []string
	switch {
	case failedIDs == 0:
		lines = []string{title, styleHealthGreen.Render(fmt.Sprintf("  All %d checks passed on %d nodes", passed, len(nodes)))}
	default:
		lines = []string{title, fmt.Sprintf("  %s passed, %s failed",
			styleHealthGreen.Render(fmt.Sprintf("%d", passed)),
			styleHealthRed.Render(fmt.Sprintf("%d", failedIDs)))}
		lines = append(lines, detail...)
	}
	return strings.Join(lines, "\n")
}
