package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestParseByteRate(t *testing.T) {
	for in, want := range map[string]int64{"40mb": 40 << 20, "500kb": 500 << 10, "1.5gb": 3 << 29, "100b": 100, "": 0, "fast": 0} {
		if got := parseByteRate(in); got != want {
			t.Errorf("parseByteRate(%q) = %d, want %d", in, got, want)
		}
	}
}

// A replica recovering on lab1 copies from its primary on lab2; a
// relocation goes from its node to relocating_node. One more copy waits
// for a slot.
func TestRecoveryView(t *testing.T) {
	shards := []cratedb.ShardInfo{
		{SchemaName: "doc", TableName: "big", ID: 2, Primary: true, RoutingState: "STARTED", NodeID: "n2", NodeName: "lab2", Size: 100 << 20},
		{SchemaName: "doc", TableName: "big", ID: 2, RoutingState: "UNASSIGNED"},
		{SchemaName: "doc", TableName: "t", ID: 0, Primary: true, RoutingState: "RELOCATING", NodeID: "n3", NodeName: "lab3", RelocatingNode: "n1", Size: 10 << 20},
		{SchemaName: "doc", TableName: "x", ID: 0, Primary: true, RoutingState: "STARTED", NodeID: "n1", NodeName: "lab1"},
	}
	allocs := []cratedb.AllocationInfo{
		{TableSchema: "doc", TableName: "big", ShardID: 2, CurrentState: "INITIALIZING", NodeID: "n1"},
		{TableSchema: "doc", TableName: "t", ShardID: 0, Primary: true, CurrentState: "RELOCATING", NodeID: "n3"},
		{TableSchema: "doc", TableName: "q", ShardID: 0, CurrentState: "UNASSIGNED",
			Decisions: []cratedb.AllocationDecision{decision("lab1", "reached the limit of incoming shard recoveries [2]")}},
	}
	snap := store.StoreSnapshot{Shards: shards, Allocations: allocs,
		ClusterSettings: cratedb.ClusterSettings{NodeConcurrentRecoveries: 2, RecoveryMaxBytesPerSec: "1mb"}}
	m := NewShardsModel(160, 50).Refresh(snap)
	if len(m.recoveries) != 2 || m.queued != 1 {
		t.Fatalf("recoveries=%d queued=%d", len(m.recoveries), m.queued)
	}
	first := m.recoveries[0].since

	m, _ = m.HandleKey(keyRune('v'))
	out := strings.Join(m.renderRecovery(first.Add(90*time.Second), 50), "\n")
	for _, want := range []string{
		"2 running · 1 queued for a slot",
		"lab2 → lab1", "lab3 → lab1",
		"≥ 1m30s",
		// lab1 takes in both, so each gets 512KB/s: 100MB -> 200s.
		"≥ 3m20s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("recovery view missing %q:\n%s", want, out)
		}
	}

	// First-seen times survive refreshes and are dropped once done.
	m = m.Refresh(snap)
	if !m.recoveries[0].since.Equal(first) {
		t.Error("first-seen time reset on refresh")
	}
	m = m.Refresh(store.StoreSnapshot{Shards: shards[:1]})
	if len(m.recoverySeen) != 0 {
		t.Errorf("recoverySeen kept %v", m.recoverySeen)
	}
}
