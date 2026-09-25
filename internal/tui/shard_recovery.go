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
	size     int64
	since    time.Time // first seen by obsi
}

func recoveryKey(s cratedb.ShardInfo) string {
	return fmt.Sprintf("%s.%s/%s/%d/%v", s.SchemaName, s.TableName, s.PartitionIdent, s.ID, s.Primary)
}

// buildRecoveries lists running recoveries from the problem shards. A
// replica copies from its primary's node; sys.shards reports neither
// progress nor source for it, so both come from the started primary.
func (m *ShardsModel) buildRecoveries(now time.Time) {
	primaryNode := map[string]string{}
	for _, s := range m.snap.Shards {
		if s.Primary && s.RoutingState == "STARTED" {
			primaryNode[fmt.Sprintf("%s.%s/%s/%d", s.SchemaName, s.TableName, s.PartitionIdent, s.ID)] = s.NodeName
		}
	}
	if m.recoverySeen == nil {
		m.recoverySeen = map[string]time.Time{}
	}
	seen := map[string]bool{}
	m.recoveries = m.recoveries[:0]
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
				r.from = primaryNode[fmt.Sprintf("%s.%s/%s/%d", s.SchemaName, s.TableName, s.PartitionIdent, s.ID)]
			}
		case "RELOCATING":
			r = recovery{shard: s, from: s.NodeName, to: m.nodeName(s.RelocatingNode), size: s.Size}
		default:
			continue
		}
		k := recoveryKey(s)
		seen[k] = true
		if _, ok := m.recoverySeen[k]; !ok {
			m.recoverySeen[k] = now
		}
		r.since = m.recoverySeen[k]
		m.recoveries = append(m.recoveries, r)
	}
	for k := range m.recoverySeen {
		if !seen[k] {
			delete(m.recoverySeen, k)
		}
	}
	sort.SliceStable(m.recoveries, func(i, j int) bool { return m.recoveries[i].since.Before(m.recoveries[j].since) })
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
	lines = append(lines, "", styleHeader.Render(fmt.Sprintf("  %-20s %7s %7s", "NODE", "IN", "OUT")))
	for _, n := range nodes {
		row := fmt.Sprintf("  %-20s %7s %7s", truncateString(n, 20), fmt.Sprintf("%d/%s", in[n], slots), fmt.Sprintf("%d/%s", out[n], slots))
		if cs.NodeConcurrentRecoveries > 0 && (in[n] >= cs.NodeConcurrentRecoveries || out[n] >= cs.NodeConcurrentRecoveries) {
			row = styleHealthYellow.Render(row)
		}
		lines = append(lines, row)
	}

	if len(m.recoveries) == 0 {
		return append(lines, "", styleDim.Render("  nothing recovering"))
	}
	bw := parseByteRate(cs.RecoveryMaxBytesPerSec)
	lines = append(lines, "", styleHeader.Render(fmt.Sprintf("  %-28s %5s %3s %-29s %10s %9s %9s",
		"TABLE", "SHARD", "P/R", "FROM → TO", "SIZE", "RUNNING", "AT LIMIT")))
	room := max(height-len(lines)-2, 3)
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
		atLimit := "?"
		if bw > 0 && in[r.to] > 0 {
			// The node's bandwidth is shared by its running recoveries.
			secs := float64(r.size) / (float64(bw) / float64(in[r.to]))
			atLimit = "≥ " + formatDuration(time.Duration(secs*float64(time.Second)).Round(time.Second))
		}
		running := "new"
		if d := now.Sub(r.since); d >= time.Second {
			running = "≥ " + formatDuration(d.Round(time.Second))
		}
		lines = append(lines, fmt.Sprintf("  %-28s %5d  %s  %-29s %10s %9s %9s",
			truncateString(r.shard.SchemaName+"."+r.shard.TableName, 28), r.shard.ID, pr,
			truncateString(from+" → "+r.to, 29), formatBytes(r.size),
			running, atLimit))
	}
	return lines
}
