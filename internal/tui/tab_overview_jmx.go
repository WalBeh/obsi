package tui

import (
	"fmt"
	"sort"
	"strings"
)

// breakerOrder fixes the row order so the most operationally critical
// breakers (parent — overall heap pressure) lead, then the per-area
// breakers, then the bookkeeping ones.
var breakerOrder = []string{
	"parent",
	"request",
	"query",
	"fielddata",
	"in_flight_requests",
	"jobs_log",
	"operations_log",
}

// renderCircuitBreakers aggregates the per-pod circuit-breaker readings
// into a cluster-level summary: max used/limit ratio across pods and total
// trips across pods. Returns "" when no JMX data is available so the
// Overview omits the section entirely on non-Cloud setups.
func (m OverviewModel) renderCircuitBreakers() string {
	if len(m.snap.JMX) == 0 {
		return ""
	}

	type rollup struct {
		maxPct    float64
		maxPctPod string
		trips     int64
		tripsPod  string
		unlimited bool
	}
	roll := map[string]*rollup{}
	for podName, snap := range m.snap.JMX {
		for bName, b := range snap.Breakers {
			r := roll[bName]
			if r == nil {
				r = &rollup{}
				roll[bName] = r
			}
			if b.Limit < 0 {
				r.unlimited = true
			} else if b.Limit > 0 {
				pct := float64(b.Used) / float64(b.Limit) * 100
				if pct > r.maxPct {
					r.maxPct = pct
					r.maxPctPod = podName
				}
			}
			if b.Tripped > 0 {
				r.trips += b.Tripped
				if r.tripsPod == "" {
					r.tripsPod = podName
				}
			}
		}
	}
	if len(roll) == 0 {
		return ""
	}

	names := orderedKeys(roll, breakerOrder)

	// Count active issues for the section title — drives whether we emit a
	// muted "all clear" line or pull the user's attention.
	var trippedCount int
	for _, r := range roll {
		if r.trips > 0 {
			trippedCount++
		}
	}

	title := sectionTitle("Circuit Breakers")
	if m.snap.Staleness["jmx"] {
		title += " " + styleStale.Render("(stale)")
	}

	lines := []string{title}
	if trippedCount == 0 {
		lines = append(lines, styleHealthGreen.Render("  No breakers tripped"))
	} else {
		lines = append(lines, styleHighValue.Render(fmt.Sprintf("  %d breaker(s) have tripped", trippedCount)))
	}

	for _, name := range names {
		r := roll[name]
		var usedStr string
		if r.unlimited {
			usedStr = styleDim.Render("       —    (unlimited)")
		} else {
			usedStr = fmt.Sprintf("%s %5.1f%%", metricBar(r.maxPct, 12), r.maxPct)
		}

		var tripStr string
		switch {
		case r.trips > 0:
			tripStr = styleHighValue.Render(fmt.Sprintf("⚠ tripped %d× (%s)", r.trips, shortenHostname(r.tripsPod, 24)))
		default:
			tripStr = styleDim.Render("no trips")
		}

		lines = append(lines, fmt.Sprintf("  %-18s %s   %s", name, usedStr, tripStr))
	}

	return strings.Join(lines, "\n")
}

// renderQueryTypes aggregates the per-pod, per-type query counters into a
// cluster-level table: lifetime totals, failure rate, and average duration
// per query type. Skipped when no JMX data is available, or when every
// type has zero activity.
func (m OverviewModel) renderQueryTypes() string {
	if len(m.snap.JMX) == 0 {
		return ""
	}

	type rollup struct {
		total       int64
		failed      int64
		durationMs  int64
		affectedRow int64
	}
	roll := map[string]*rollup{}
	for _, snap := range m.snap.JMX {
		for qt, q := range snap.QueryStats {
			r := roll[qt]
			if r == nil {
				r = &rollup{}
				roll[qt] = r
			}
			r.total += q.Total
			r.failed += q.Failed
			r.durationMs += q.DurationSumMs
			r.affectedRow += q.AffectedRows
		}
	}
	if len(roll) == 0 {
		return ""
	}

	// Drop types with zero activity to keep the section terse on lightly
	// loaded clusters that would otherwise show eight rows of zeros.
	var names []string
	for k, r := range roll {
		if r.total > 0 {
			names = append(names, k)
		}
	}
	if len(names) == 0 {
		return ""
	}
	// Sort by total descending — busiest type leads, easier to read.
	sort.Slice(names, func(i, j int) bool { return roll[names[i]].total > roll[names[j]].total })

	title := sectionTitle("Query Activity")
	if m.snap.Staleness["jmx"] {
		title += " " + styleStale.Render("(stale)")
	}

	lines := []string{title}
	lines = append(lines, styleHeader.Render(fmt.Sprintf("  %-14s %14s %14s %12s", "TYPE", "TOTAL", "FAILED", "AVG MS")))
	for _, qt := range names {
		r := roll[qt]
		avgMs := float64(r.durationMs) / float64(r.total)

		failedStr := styleDim.Render("0")
		if r.failed > 0 {
			pct := float64(r.failed) / float64(r.total) * 100
			failedStr = styleHighValue.Render(fmt.Sprintf("%d (%.3f%%)", r.failed, pct))
		}

		lines = append(lines, fmt.Sprintf("  %-14s %14s %14s %12.1f",
			qt,
			formatLargeInt(r.total),
			failedStr,
			avgMs,
		))
	}

	return strings.Join(lines, "\n")
}

// formatLargeInt prints integers with thousands separators for readability.
// Long-running CrateDB clusters routinely log query counts in the millions
// and the raw form ("1440066") is much harder to scan than "1,440,066".
func formatLargeInt(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

