package tui

import (
	"strings"
	"testing"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestGroupDecisions(t *testing.T) {
	groups := groupDecisions([]cratedb.AllocationDecision{
		{NodeName: "lab1", Explanations: []string{"a copy of this shard is already allocated to this node [[.partitioned.t.1][0], node[a], [R]]"}},
		{NodeName: "lab2", Explanations: []string{"a copy of this shard is already allocated to this node [[.partitioned.t.1][0], node[b], [P]]"}},
		{NodeName: "lab3", Explanations: []string{"the node is above the high watermark"}},
	})
	if len(groups) != 2 || strings.Join(groups[0].nodes, ",") != "lab1,lab2" || groups[1].nodes[0] != "lab3" {
		t.Fatalf("groups = %+v", groups)
	}
	if groups[0].explanation != "a copy of this shard is already allocated to this node" {
		t.Errorf("explanation = %q", groups[0].explanation)
	}
}

// Shard 0 of two partitions must each get its own reasons.
func TestShardDetailPerPartition(t *testing.T) {
	shard := func(part string) cratedb.ShardInfo {
		return cratedb.ShardInfo{SchemaName: "doc", TableName: "t", PartitionIdent: part, ID: 0, RoutingState: "UNASSIGNED"}
	}
	alloc := func(part, why string) cratedb.AllocationInfo {
		return cratedb.AllocationInfo{TableSchema: "doc", TableName: "t", PartitionIdent: part, ShardID: 0, CurrentState: "UNASSIGNED", Explanation: why}
	}
	m := NewShardsModel(120, 40).Refresh(store.StoreSnapshot{
		Shards:      []cratedb.ShardInfo{shard("p1"), shard("p2")},
		Allocations: []cratedb.AllocationInfo{alloc("p1", "reason one"), alloc("p2", "reason two")},
	})
	d1, d2 := m.renderDetail(m.problemShards[0]), m.renderDetail(m.problemShards[1])
	if !strings.Contains(d1, "reason one") || strings.Contains(d1, "reason two") || !strings.Contains(d2, "reason two") {
		t.Errorf("reasons mixed across partitions:\n%s\n---\n%s", d1, d2)
	}
}

func TestClassifyShard(t *testing.T) {
	for _, tc := range []struct {
		s    cratedb.ShardInfo
		want shardBucket
	}{
		{cratedb.ShardInfo{RoutingState: "UNASSIGNED", Primary: true}, bucketPrimaryUnassigned},
		{cratedb.ShardInfo{RoutingState: "UNASSIGNED"}, bucketReplicaUnassigned},
		{cratedb.ShardInfo{RoutingState: "INITIALIZING", Primary: true, RecoveryType: "EXISTING_STORE"}, bucketPrimaryRecovering},
		{cratedb.ShardInfo{RoutingState: "INITIALIZING", RecoveryType: "PEER"}, bucketReplicaRecovering},
		{cratedb.ShardInfo{RoutingState: "INITIALIZING", Primary: true, RecoveryType: "SNAPSHOT"}, bucketRestoring},
		// A relocating source keeps the recovery type of its own, older recovery.
		{cratedb.ShardInfo{RoutingState: "RELOCATING", Primary: true, RecoveryType: "EMPTY_STORE"}, bucketMoving},
	} {
		if got := classifyShard(tc.s); got != tc.want {
			t.Errorf("classifyShard(%+v) = %s, want %s", tc.s, bucketNames[got], bucketNames[tc.want])
		}
	}
}

func TestShardVerdict(t *testing.T) {
	var c [bucketCount]int
	c[bucketMoving] = 3
	if v, _ := shardVerdict(c); v != "Rebalancing: 3 shards moving, all data available" {
		t.Errorf("moving only: %q", v)
	}
	c[bucketReplicaUnassigned] = 1
	if v, _ := shardVerdict(c); v != "Fewer copies: 1 replica unassigned, all data available" {
		t.Errorf("replica: %q", v)
	}
	c[bucketPrimaryUnassigned] = 2
	if v, _ := shardVerdict(c); v != "Data unavailable: 2 primaries unassigned" {
		t.Errorf("primary: %q", v)
	}
}

// As seen on CrateDB 6.3 after a node holding the only copy left: the lost
// primary shows as primary = false in sys.shards.
func TestMarkLostPrimaries(t *testing.T) {
	sh := func(id int, primary bool, state string) cratedb.ShardInfo {
		return cratedb.ShardInfo{SchemaName: "doc", TableName: "t", ID: id, Primary: primary, RoutingState: state}
	}
	got := markLostPrimaries([]cratedb.ShardInfo{
		sh(0, true, "STARTED"), sh(0, false, "UNASSIGNED"), // replica of a live primary
		sh(1, false, "UNASSIGNED"), sh(1, false, "UNASSIGNED"), // primary lost, two copies listed
	})
	if got[1].Primary {
		t.Error("replica of a started primary marked primary")
	}
	if !got[2].Primary || got[3].Primary {
		t.Errorf("want exactly one copy of shard 1 as primary, got %v %v", got[2].Primary, got[3].Primary)
	}
	m := NewShardsModel(120, 40).Refresh(store.StoreSnapshot{Shards: []cratedb.ShardInfo{sh(1, false, "UNASSIGNED")}})
	if m.buckets[bucketPrimaryUnassigned] != 1 || !strings.Contains(m.renderSummary(), "Data unavailable") {
		t.Errorf("summary = %q", m.renderSummary())
	}
}

// Shape seen on CrateDB 6.3 while a new replica recovers: sys.shards lists
// it as UNASSIGNED without a node, sys.allocations as INITIALIZING on lab2.
func TestProblemShardsFromAllocations(t *testing.T) {
	shards := []cratedb.ShardInfo{
		{SchemaName: "doc", TableName: "big", ID: 1, Primary: true, RoutingState: "STARTED", NodeID: "n3", NodeName: "lab3", Size: 1000},
		{SchemaName: "doc", TableName: "big", ID: 1, RoutingState: "UNASSIGNED"},
		{SchemaName: "doc", TableName: "t", ID: 0, Primary: true, RoutingState: "RELOCATING", NodeID: "n1", NodeName: "lab1", RelocatingNode: "n3", Size: 50},
		{SchemaName: "doc", TableName: "x", ID: 0, NodeID: "n2", NodeName: "lab2", RoutingState: "STARTED"},
	}
	allocs := []cratedb.AllocationInfo{
		{TableSchema: "doc", TableName: "big", ShardID: 1, CurrentState: "INITIALIZING", NodeID: "n2"},
		{TableSchema: "doc", TableName: "t", ShardID: 0, Primary: true, CurrentState: "RELOCATING", NodeID: "n1"},
	}
	m := NewShardsModel(120, 40).Refresh(store.StoreSnapshot{Shards: shards, Allocations: allocs})
	if m.buckets[bucketReplicaRecovering] != 1 || m.buckets[bucketMoving] != 1 || m.buckets[bucketReplicaUnassigned] != 0 {
		t.Fatalf("buckets = %v", m.buckets)
	}
	rec := m.problemShards[0]
	if rec.NodeName != "lab2" || rec.Size != 1000 {
		t.Errorf("recovering replica = %+v, want on lab2 with the primary's size", rec)
	}
	if d := m.renderDetail(m.problemShards[1]); !strings.Contains(d, "moving to lab3") {
		t.Errorf("detail:\n%s", d)
	}
}
