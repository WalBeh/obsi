package tui

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// sparkWidth is the width of the replication sparklines; narrower
// terminals drop them.
const (
	sparkWidth    = 20
	sparkMinWidth = 120
)

// sparkline0 draws values on a timeline of total polls as braille dots,
// two samples per character in width characters, scaled from 0 so a flat
// line at a high value reads as high. Zero and gaps stay blank, so an idle
// series draws nothing. A series that started later than the others is
// padded on the left, so its newest point lines up with theirs; with more
// polls than slots each slot is the busiest of an even share of them.
func sparkline0(values []float64, total, width int) string {
	slots := 2 * width
	total = max(total, len(values))
	level := make([]int, slots) // dots per slot, 0-4
	if total > 0 {
		padded := make([]float64, total)
		gap := total - len(values)
		for i := range padded {
			if i < gap {
				padded[i] = math.NaN()
			} else {
				padded[i] = values[i-gap]
			}
		}
		n := min(total, slots)
		buckets := make([]float64, n)
		top := 0.0
		for b := range buckets {
			lo, hi := b*total/n, (b+1)*total/n
			for _, v := range padded[lo:hi] {
				if !math.IsNaN(v) {
					buckets[b] = max(buckets[b], v)
				}
			}
			top = max(top, buckets[b])
		}
		for b, v := range buckets {
			if v > 0 && top > 0 {
				level[slots-n+b] = max(1, int(math.Round(v/top*4)))
			}
		}
	}
	// Braille dots bottom up: 7 3 2 1 on the left, 8 6 5 4 on the right.
	left := []rune{0x40, 0x04, 0x02, 0x01}
	right := []rune{0x80, 0x20, 0x10, 0x08}
	var out strings.Builder
	for c := range width {
		code := rune(0x2800)
		for k := range level[2*c] {
			code |= left[k]
		}
		for k := range level[2*c+1] {
			code |= right[k]
		}
		if code == 0x2800 {
			out.WriteRune(' ')
		} else {
			out.WriteRune(code)
		}
	}
	return out.String()
}

// sparkPeak is the scale for a sparkline: its highest value, so a burst
// of 5/s doesn't read like one of 5,000/s. Empty when there was none.
func sparkPeak(values []float64, unit string) string {
	top := 0.0
	for _, v := range values {
		if !math.IsNaN(v) {
			top = max(top, v)
		}
	}
	if top <= 0 {
		return ""
	}
	return "peak " + formatRecords(int64(top+0.5)) + unit
}

// sparkPeakWidth is the room after a sparkline for its peak.
const sparkPeakWidth = 12

// spanLabel names the time the sparklines cover, e.g. "LAST 20M".
func spanLabel(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("LAST %dH", int(d.Hours()))
	case d >= time.Minute:
		return fmt.Sprintf("LAST %dM", int(d.Minutes()))
	}
	return fmt.Sprintf("LAST %dS", int(d.Seconds()))
}

func (m ReplicationModel) sparks() bool { return m.width >= sparkMinWidth }

// copyLines describes a copy obsi watched finish, and when the chunks in
// flight could be what limits it: per shard, max_concurrent_file_chunks
// chunks of chunk_size each wait one round trip.
func (m ReplicationModel) copyLines(c *store.CopyResult) []string {
	took := formatDuration(c.Wall.Round(time.Second))
	if !c.FromStart {
		took = "≥ " + took + " (copying when obsi started)"
	}
	line := fmt.Sprintf("      copied %s in %s", formatBytes(c.Bytes), took)
	// Wall is as precise as the poll interval: only a long copy gets a rate.
	if c.FromStart && c.Wall >= time.Minute {
		line += fmt.Sprintf(", %s/s", formatRate(float64(c.Bytes)/c.Wall.Seconds()))
	}
	if c.ShardBps > 0 {
		line += fmt.Sprintf(" · %s/s per shard", formatRate(c.ShardBps))
	}
	lines := []string{styleDim.Render(line)}
	chunks, _ := strconv.Atoi(m.repl.Settings["replication.logical.recovery.max_concurrent_file_chunks"])
	chunk := parseByteRate(m.repl.Settings["replication.logical.recovery.chunk_size"])
	if c.Bytes < 100<<20 || c.ShardBps <= 0 || chunks <= 0 || chunk <= 0 {
		return lines
	}
	// Within a region round trips are a few ms; local disk and request
	// overhead alone came to 16ms in the lab. 30ms points at a WAN link.
	rtt := time.Duration(float64(chunks*int(chunk)) / c.ShardBps * float64(time.Second))
	if rtt < 30*time.Millisecond {
		return lines
	}
	text := fmt.Sprintf("▲ %d × %s chunks in flight at %s/s per shard ≈ one round trip every %s. If the network round trip is near that, more max_concurrent_file_chunks (up to 5) or a larger chunk_size speeds up copies; if it's much shorter, disk or CPU is the limit.",
		chunks, formatBytes(chunk), formatRate(c.ShardBps), formatDuration(rtt.Round(time.Millisecond)))
	for _, l := range wrapText(text, max(m.width-10, 40)) {
		lines = append(lines, "      "+styleHealthYellow.Render(l))
	}
	return lines
}

// subscriberTotals is what this cluster replicated while obsi ran, with
// the bytes estimated from each table's average document size.
func (m ReplicationModel) subscriberTotals() string {
	var changes int64
	var bytes float64
	for _, r := range m.repl.Replicas {
		changes += r.Replicated
		if r.Docs > 0 {
			bytes += float64(r.Replicated) * float64(r.Size) / float64(r.Docs)
		}
	}
	if changes == 0 {
		return ""
	}
	return styleDim.Render(fmt.Sprintf("  replicated since obsi started: %s changes (≈ %s from the tables' average document size)",
		formatRecords(changes), formatBytes(int64(bytes))))
}

func (m ReplicationModel) publisherTotals() string {
	var parts []string
	var pulled int64
	for _, h := range m.repl.Subscribers {
		parts = append(parts, fmt.Sprintf("%s %s", h.Subscriber, formatRecords(h.Pulled)))
		pulled += h.Pulled
	}
	if pulled == 0 {
		return ""
	}
	sort.Strings(parts)
	return styleDim.Render("  operations pulled since obsi started: " + strings.Join(parts, " · "))
}

func pulledCell(h store.LeaseHolder) string {
	if h.PulledRate < 0 {
		return fmt.Sprintf("%9s", "—")
	}
	return fmt.Sprintf("%9s", formatRecords(int64(h.PulledRate+0.5)))
}

// tableReplicas is what the Tables data says about a table's replicas on
// this cluster.
type tableReplicas struct {
	setting     string // number_of_replicas, e.g. "1" or "0-1"
	partitioned bool
}

// replicaLines flags a subscribed table whose replicas this cluster can't
// place. number_of_replicas is a non-replicated setting, so a change here
// holds for existing tables and partitions, but a new partition arrives
// with the publisher's count and the table's value here follows it.
func (m ReplicationModel) replicaLines(fqn string, r store.ReplicaTable) []string {
	tr, ok := m.replicas[fqn]
	want, okWant := cratedb.MinReplicas(tr.setting)
	nodes := len(m.nodes)
	if !ok || !okWant || nodes == 0 {
		return nil
	}
	fit := fmt.Sprintf("'0-%d'", want)
	var text string
	style := styleHealthYellow
	switch {
	case r.UnassignedReplicas > 0 && want <= nodes-1 && r.PartitionReplicas > nodes-1:
		// The table fits now (e.g. a range from the publisher); partitions
		// that arrived before keep their count.
		text = fmt.Sprintf("▲ %d replica %s can't be placed: older partitions here keep number_of_replicas %d from when they arrived, on %d %s. ALTER TABLE %s SET (number_of_replicas = %d) here fixes them; new partitions fit.",
			r.UnassignedReplicas, plural(r.UnassignedReplicas, "copy", "copies"), r.PartitionReplicas, nodes, plural(nodes, "node", "nodes"), quoteFQN(fqn), nodes-1)
	case want <= nodes-1:
		return nil
	case r.UnassignedReplicas > 0:
		text = fmt.Sprintf("▲ %d replica %s can't be placed: number_of_replicas %s here on %d %s. Set number_of_replicas = %s on the publisher's table (new partitions arrive with it), or ALTER TABLE %s SET (number_of_replicas = %d) here for what exists.",
			r.UnassignedReplicas, plural(r.UnassignedReplicas, "copy", "copies"), tr.setting, nodes, plural(nodes, "node", "nodes"), fit, quoteFQN(fqn), nodes-1)
		if tr.partitioned {
			text += " New partitions bring the publisher's count again."
		}
	case tr.partitioned:
		style = styleDim
		text = fmt.Sprintf("New partitions will arrive with number_of_replicas %s (the publisher's) and can't place their replicas on %d %s: set %s on the publisher's table.",
			tr.setting, nodes, plural(nodes, "node", "nodes"), fit)
	default:
		return nil
	}
	var lines []string
	for _, l := range wrapText(text, max(m.width-10, 40)) {
		lines = append(lines, "      "+style.Render(l))
	}
	return lines
}
