package tui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// recovery is one copy being built: a replica or primary INITIALIZING on
// its target, or a relocation.
type recovery struct {
	shard    cratedb.ShardInfo
	from, to string // node names; from is "" for a primary recovering locally
	left     string // node a moving copy leaves; "" for a new copy
	size     int64
	since    time.Time // first seen by obsi
	doneAt   time.Time // sys.shards showed it done while sys.allocations still lists it
	share    int64     // bytes/s of max_bytes_per_sec this copy gets, 0 if unknown
}

// fastest is the time the copy needs at its share of the throttle.
func (r recovery) fastest() time.Duration {
	if r.share <= 0 {
		return 0
	}
	return time.Duration(float64(r.size) / float64(r.share) * float64(time.Second)).Round(time.Second)
}

func recoveryKey(s cratedb.ShardInfo) string {
	return fmt.Sprintf("%s.%s/%s/%d/%v", s.SchemaName, s.TableName, s.PartitionIdent, s.ID, s.Primary)
}

// buildRecoveries lists running recoveries from the problem shards. A
// replica copies from its primary, also when it moves (the node it leaves
// isn't the source); sys.shards reports neither progress nor source for
// it, so node and size come from the primary.
func (m *ShardsModel) buildRecoveries(now time.Time) {
	type primary struct {
		node string
		size int64
	}
	primaries := map[string]primary{}
	for _, s := range m.snap.Shards {
		if s.Primary && (s.RoutingState == "STARTED" || s.RoutingState == "RELOCATING") {
			primaries[fmt.Sprintf("%s.%s/%s/%d", s.SchemaName, s.TableName, s.PartitionIdent, s.ID)] = primary{s.NodeName, s.Size}
		}
	}
	fromPrimary := func(r *recovery) {
		if p, ok := primaries[fmt.Sprintf("%s.%s/%s/%d", r.shard.SchemaName, r.shard.TableName, r.shard.PartitionIdent, r.shard.ID)]; ok {
			r.from = p.node
			if p.size > 0 {
				r.size = p.size
			}
		}
	}
	if m.recoverySeen == nil {
		m.recoverySeen = map[string]time.Time{}
	}
	seen := map[string]bool{}
	prev := m.recoveries
	m.recoveries = nil
	m.queued = 0
	for i, s := range m.problemShards {
		for _, f := range m.fixes[i] {
			if f.key == "throttled" {
				m.queued++
			}
		}
		var r recovery
		switch s.RoutingState {
		case "INITIALIZING":
			r = recovery{shard: s, to: s.NodeName, size: s.Size}
			if !s.Primary {
				fromPrimary(&r)
			}
		case "RELOCATING":
			r = recovery{shard: s, from: s.NodeName, left: s.NodeName, to: m.nodeName(s.RelocatingNode), size: s.Size}
			if !s.Primary {
				fromPrimary(&r)
			}
		default:
			continue
		}
		k := recoveryKey(s)
		// sys.shards (5s) shows a finished move before sys.allocations (30s)
		// does; until then the shard is still listed but its target is gone.
		if r.to == "" {
			for _, p := range prev {
				if recoveryKey(p.shard) == k {
					r.from, r.left, r.to, r.size, r.doneAt = p.from, p.left, p.to, p.size, p.doneAt
				}
			}
			if r.doneAt.IsZero() {
				r.doneAt = now
			}
		}
		seen[k] = true
		if _, ok := m.recoverySeen[k]; !ok {
			m.recoverySeen[k] = now
		}
		r.since = m.recoverySeen[k]
		m.recoveries = append(m.recoveries, r)
	}
	sort.SliceStable(m.recoveries, func(i, j int) bool { return m.recoveries[i].since.Before(m.recoveries[j].since) })
	m.setShares()
	// A copy's share grows as others finish; judge it by the smallest.
	if m.minShare == nil {
		m.minShare = map[string]int64{}
	}
	for i, r := range m.recoveries {
		k := recoveryKey(r.shard)
		if old, ok := m.minShare[k]; ok && old > 0 && (r.share == 0 || old < r.share) {
			m.recoveries[i].share = old
		}
		m.minShare[k] = m.recoveries[i].share
	}
	for k := range m.recoverySeen {
		if !seen[k] {
			delete(m.recoverySeen, k)
			delete(m.minShare, k)
		}
	}
	for _, r := range prev {
		if !seen[recoveryKey(r.shard)] {
			ended := now
			if !r.doneAt.IsZero() {
				ended = r.doneAt
			}
			m.pending = append(m.pending, finishedRecovery{recovery: r, ended: ended})
		}
	}
}

// setShares splits max_bytes_per_sec over the running recoveries. It caps
// a node's incoming and outgoing recovery traffic together (on the lab a
// node sending one copy and receiving another gave each half), and the
// busier of the two nodes sets a copy's share. A primary recovering from
// its own disk isn't throttled and gets none.
func (m *ShardsModel) setShares() {
	bw := parseByteRate(m.snap.ClusterSettings.RecoveryMaxBytesPerSec)
	load := map[string]int{}
	for _, r := range m.recoveries {
		if r.from != "" {
			load[r.to]++
			load[r.from]++
		}
	}
	for i, r := range m.recoveries {
		if bw <= 0 || r.from == "" {
			continue
		}
		m.recoveries[i].share = bw / int64(max(load[r.to], load[r.from]))
	}
}

// parseByteRate reads CrateDB byte sizes like "40mb" (binary units).
func parseByteRate(s string) int64 {
	s = strings.ToLower(strings.TrimSpace(s))
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"pb", 1 << 50}, {"tb", 1 << 40}, {"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10}, {"b", 1}} {
		if strings.HasSuffix(s, u.suffix) {
			v, err := strconv.ParseFloat(strings.TrimSuffix(s, u.suffix), 64)
			if err != nil {
				return 0
			}
			return int64(v * float64(u.mult))
		}
	}
	return 0
}

func (m ShardsModel) renderRecovery(now time.Time, height int) []string {
	cs := m.snap.ClusterSettings
	slots := "?"
	if cs.NodeConcurrentRecoveries > 0 {
		slots = strconv.Itoa(cs.NodeConcurrentRecoveries)
	}
	rate := cs.RecoveryMaxBytesPerSec
	if rate == "" {
		rate = "?"
	}
	lines := []string{fmt.Sprintf("  Recovery: %d running · %d queued for a slot · %s in/out per node (node_concurrent_recoveries) · %s per node (indices.recovery.max_bytes_per_sec)",
		len(m.recoveries), m.queued, slots, rate)}

	in, out := map[string]int{}, map[string]int{}
	for _, r := range m.recoveries {
		in[r.to]++
		if r.from != "" {
			out[r.from]++
		}
	}
	var nodes []string
	for id, name := range m.nodeNames {
		if name == "" {
			name = id
		}
		if !contains(nodes, name) {
			nodes = append(nodes, name)
		}
	}
	sort.Strings(nodes)
	throttle := parseByteRate(cs.RecoveryMaxBytesPerSec)
	peaks := nodePeaks(m.history)
	lines = append(lines, "", styleHeader.Render(fmt.Sprintf("  %-20s %7s %7s  %-18s", "NODE", "IN", "OUT", "MOVED, LAST HOUR")))
	for _, n := range nodes {
		row := fmt.Sprintf("  %-20s %7s %7s", truncateString(n, 20), fmt.Sprintf("%d/%s", in[n], slots), fmt.Sprintf("%d/%s", out[n], slots))
		if cs.NodeConcurrentRecoveries > 0 && (in[n] >= cs.NodeConcurrentRecoveries || out[n] >= cs.NodeConcurrentRecoveries) {
			row = styleHealthYellow.Render(row)
		}
		peak := styleDim.Render("—")
		if p, ok := peaks[n]; ok && p.rate > 0 {
			peak = fmt.Sprintf("%s/s, ≤%d at once", formatBytes(p.rate), p.copies)
			if capped(p, throttle) {
				peak = styleHealthYellow.Render(peak)
			}
		}
		lines = append(lines, row+"  "+peak)
	}
	if b := bottleneck(peaks, throttle, cs.RecoveryMaxBytesPerSec); b != "" {
		lines = append(lines, styleHealthYellow.Render("  "+b))
	}

	if len(m.recoveries) == 0 {
		lines = append(lines, "", styleDim.Render("  nothing recovering"))
		return append(lines, m.renderFinished()...)
	}
	back := 0
	for _, r := range m.recoveries {
		if _, ok := m.movedBack(r, r.since); ok {
			back++
		}
	}
	for _, f := range m.finished {
		if _, ok := m.movedBack(f.recovery, f.ended.Add(-f.took)); ok {
			back++
		}
	}
	if back > 0 {
		lines = append(lines, "", styleHealthYellow.Render(fmt.Sprintf(
			"  ↩ %d moves take a shard back to a node it left within the hour: rebalancing goes in circles (disk watermarks? rebalance.enable = none stops it)", back)))
	}
	lines = append(lines, "",
		styleDim.Render("  ELAPSED counts from when obsi first saw the recovery (≥: it may be older). FASTEST is the size at the copy's"),
		styleDim.Render("  share of max_bytes_per_sec, a per-node limit on incoming plus outgoing, split over the busier node's recoveries. Past FASTEST,"),
		styleDim.Render("  the throttle isn't the limit."),
		"", styleHeader.Render(fmt.Sprintf("  %-28s %5s %3s %-29s %10s %9s %8s  %s",
			"TABLE", "SHARD", "P/R", "FROM → TO", "SIZE", "ELAPSED", "FASTEST", "STATUS")))
	room := max(height-len(lines)-2-len(m.renderFinished()), 3)
	for i, r := range m.recoveries {
		if i == room {
			lines = append(lines, styleDim.Render(fmt.Sprintf("  … %d more", len(m.recoveries)-i)))
			break
		}
		pr := "R"
		if r.shard.Primary {
			pr = "P"
		}
		from := r.from
		if from == "" {
			from = "local disk"
		}
		fastest := "?"
		if d := r.fastest(); d > 0 {
			fastest = formatDuration(d)
		} else if r.from == "" {
			fastest = "—"
		}
		elapsed := now.Sub(r.since)
		shown := "new"
		if elapsed >= time.Second {
			shown = "≥ " + formatDuration(elapsed.Round(time.Second))
		}
		status, slow := recoveryStatus(r, elapsed)
		if slow {
			status = styleHealthYellow.Render(status)
		} else {
			status = styleDim.Render(status)
		}
		mark := "  "
		if _, ok := m.movedBack(r, r.since); ok {
			mark = styleHealthYellow.Render("↩ ")
		}
		lines = append(lines, fmt.Sprintf("%s%-28s %5d  %s  %-29s %10s %9s %8s  %s", mark,
			truncateString(r.shard.SchemaName+"."+r.shard.TableName, 28), r.shard.ID, pr,
			truncateString(from+" → "+r.to, 29), formatBytes(r.size),
			shown, fastest, status))
	}
	return append(lines, m.renderFinished()...)
}
