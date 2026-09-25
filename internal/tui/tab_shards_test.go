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
		return cratedb.AllocationInfo{TableSchema: "doc", TableName: "t", PartitionIdent: part, ShardID: 0, Explanation: why}
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
