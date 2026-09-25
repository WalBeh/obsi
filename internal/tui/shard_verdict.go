package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// shardBucket says what a non-STARTED shard means for the data, worst first.
type shardBucket int

const (
	bucketPrimaryUnassigned shardBucket = iota
	bucketPrimaryRecovering
	bucketRestoring
	bucketReplicaUnassigned
	bucketReplicaRecovering
	bucketMoving
	bucketCount
)

var bucketNames = [bucketCount]string{
	"primary unassigned", "primary recovering", "restoring",
	"replica unassigned", "replica recovering", "moving",
}

// classifyShard buckets a non-STARTED shard. sys.shards only lists the
// source of a relocation, so RELOCATING is the whole move and INITIALIZING
// is always a real recovery.
func classifyShard(s cratedb.ShardInfo) shardBucket {
	switch {
	case s.RoutingState == "RELOCATING":
		return bucketMoving
	case s.RecoveryType == "SNAPSHOT":
		return bucketRestoring
	case s.RoutingState == "UNASSIGNED" && s.Primary:
		return bucketPrimaryUnassigned
	case s.RoutingState == "UNASSIGNED":
		return bucketReplicaUnassigned
	case s.Primary:
		return bucketPrimaryRecovering
	default:
		return bucketReplicaRecovering
	}
}

// problemShards lists the copies that aren't STARTED. sys.allocations is
// the source when it answered: CrateDB 6.3 lists a copy that is recovering
// in sys.shards as UNASSIGNED without a node, and a lost primary as a
// replica. sys.shards fills in what allocations lack (size, move target).
func problemShards(shards []cratedb.ShardInfo, allocs []cratedb.AllocationInfo) []cratedb.ShardInfo {
	var out []cratedb.ShardInfo
	if len(allocs) == 0 {
		for _, s := range markLostPrimaries(shards) {
			if s.RoutingState != "STARTED" {
				out = append(out, s)
			}
		}
		return out
	}

	type shardKey struct {
		schema, table, part string
		id                  int
	}
	type copyKey struct {
		shardKey
		node string
	}
	copies := map[copyKey]cratedb.ShardInfo{}
	primaries := map[shardKey]cratedb.ShardInfo{}
	for _, s := range shards {
		k := shardKey{s.SchemaName, s.TableName, s.PartitionIdent, s.ID}
		if s.NodeID != "" {
			copies[copyKey{k, s.NodeID}] = s
		}
		if s.Primary && s.RoutingState == "STARTED" {
			primaries[k] = s
		}
	}
	for _, a := range allocs {
		if a.CurrentState == "STARTED" {
			continue
		}
		k := shardKey{a.TableSchema, a.TableName, a.PartitionIdent, a.ShardID}
		s := cratedb.ShardInfo{
			SchemaName:     a.TableSchema,
			TableName:      a.TableName,
			PartitionIdent: a.PartitionIdent,
			ID:             a.ShardID,
			Primary:        a.Primary,
			RoutingState:   a.CurrentState,
			NodeID:         a.NodeID,
		}
		if c, ok := copies[copyKey{k, a.NodeID}]; ok && a.NodeID != "" && c.RoutingState == a.CurrentState {
			s.NodeName, s.Size, s.NumDocs = c.NodeName, c.Size, c.NumDocs
			s.Relocating, s.RelocatingNode = c.Relocating, c.RelocatingNode
			s.RecoveryType, s.RecoveryStage, s.RecoveryPercent = c.RecoveryType, c.RecoveryStage, c.RecoveryPercent
		} else if p, ok := primaries[k]; ok && !a.Primary {
			// What a replica has to copy.
			s.Size, s.NumDocs = p.Size, p.NumDocs
		}
		out = append(out, s)
	}
	return out
}

// markLostPrimaries returns shards with Primary set on one UNASSIGNED copy
// of every shard that has no assigned primary. sys.shards reports a lost
// primary as primary = false (CrateDB 6.3; sys.allocations and sys.health
// have it right), which would hide the worst case of all.
func markLostPrimaries(shards []cratedb.ShardInfo) []cratedb.ShardInfo {
	key := func(s cratedb.ShardInfo) string {
		return fmt.Sprintf("%s.%s/%s/%d", s.SchemaName, s.TableName, s.PartitionIdent, s.ID)
	}
	hasPrimary := map[string]bool{}
	for _, s := range shards {
		if s.Primary && s.RoutingState != "UNASSIGNED" {
			hasPrimary[key(s)] = true
		}
	}
	out := make([]cratedb.ShardInfo, len(shards))
	copy(out, shards)
	for i, s := range out {
		if s.RoutingState == "UNASSIGNED" && !hasPrimary[key(s)] {
			out[i].Primary = true
			hasPrimary[key(s)] = true
		}
	}
	return out
}

func bucketStyle(b shardBucket) lipgloss.Style {
	switch b {
	case bucketPrimaryUnassigned, bucketPrimaryRecovering:
		return styleHealthRed
	case bucketMoving:
		return styleDim
	default:
		return styleHealthYellow
	}
}

// bucketState is the list's STATE column: shorter than the bucket name, the
// P/R column carries the rest.
func bucketState(b shardBucket) string {
	switch b {
	case bucketPrimaryUnassigned, bucketReplicaUnassigned:
		return "UNASSIGNED"
	case bucketPrimaryRecovering, bucketReplicaRecovering:
		return "RECOVERING"
	case bucketRestoring:
		return "RESTORING"
	default:
		return "MOVING"
	}
}

// shardVerdict is the one-line answer to "is my data OK": the worst bucket
// decides the headline.
func shardVerdict(counts [bucketCount]int) (string, lipgloss.Style) {
	switch {
	case counts[bucketPrimaryUnassigned] > 0:
		return fmt.Sprintf("Data unavailable: %d primar%s unassigned",
			counts[bucketPrimaryUnassigned], plural(counts[bucketPrimaryUnassigned], "y", "ies")), styleHealthRed
	case counts[bucketPrimaryRecovering] > 0:
		return fmt.Sprintf("Recovering: %d primar%s not available until done",
			counts[bucketPrimaryRecovering], plural(counts[bucketPrimaryRecovering], "y", "ies")), styleHealthRed
	case counts[bucketRestoring] > 0:
		return fmt.Sprintf("Restoring from snapshot: %d shard%s",
			counts[bucketRestoring], plural(counts[bucketRestoring], "", "s")), styleHealthYellow
	case counts[bucketReplicaUnassigned] > 0:
		return fmt.Sprintf("Fewer copies: %d replica%s unassigned, all data available",
			counts[bucketReplicaUnassigned], plural(counts[bucketReplicaUnassigned], "", "s")), styleHealthYellow
	case counts[bucketReplicaRecovering] > 0:
		return fmt.Sprintf("Restoring copies: %d replica%s recovering, all data available",
			counts[bucketReplicaRecovering], plural(counts[bucketReplicaRecovering], "", "s")), styleHealthYellow
	case counts[bucketMoving] > 0:
		return fmt.Sprintf("Rebalancing: %d shard%s moving, all data available",
			counts[bucketMoving], plural(counts[bucketMoving], "", "s")), styleHealthGreen
	}
	return "", styleDim
}

func bucketCounts(counts [bucketCount]int) string {
	var parts []string
	for b, n := range counts {
		if n > 0 {
			parts = append(parts, bucketStyle(shardBucket(b)).Render(fmt.Sprintf("%d %s", n, bucketNames[b])))
		}
	}
	return strings.Join(parts, " · ")
}

var recoveryTypeNames = map[string]string{
	"STORE":          "from local disk",
	"EXISTING_STORE": "from local disk",
	"EMPTY_STORE":    "as a new empty shard",
	"PEER":           "copying from the primary",
	"SNAPSHOT":       "from a snapshot",
	"LOCAL_SHARDS":   "from local shards (resize)",
}

// bucketMeaning explains the selected shard in the detail panel.
func bucketMeaning(s cratedb.ShardInfo, b shardBucket, nodeName func(string) string) string {
	switch b {
	case bucketPrimaryUnassigned:
		return "primary unassigned: reads and writes to this shard fail"
	case bucketPrimaryRecovering:
		return strings.TrimSpace("primary recovering "+recoveryTypeName(s.RecoveryType)) + ": not available until STARTED"
	case bucketRestoring:
		return "restoring from a snapshot"
	case bucketReplicaUnassigned:
		return "replica unassigned: the primary still serves the data, one copy fewer"
	case bucketReplicaRecovering:
		return strings.TrimSpace("replica recovering " + recoveryTypeName(s.RecoveryType))
	default:
		return "moving to " + nodeName(s.RelocatingNode) + ": all copies stay available until the move is done"
	}
}

func recoveryTypeName(t string) string {
	if n, ok := recoveryTypeNames[t]; ok {
		return n
	}
	return strings.ToLower(t)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
