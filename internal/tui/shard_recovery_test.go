package tui

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
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
		"3m20s", "within the throttle's time",
		// 10MB at 512KB/s is 20s; 90s seen means something else limits it.
		"20.0s", "slower than the throttle",
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

	// Both left the running list: their new copies' records are read next.
	if len(m.pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(m.pending))
	}
	copied, replayed := m.pending[0], m.pending[1]
	copied.took, copied.recovered, copied.files = 400*time.Second, 100<<20, 20 // 256KB/s, half its share
	replayed.took, replayed.recovered, replayed.files = 2*time.Second, 208, 1
	m = m.addFinished(FinishedRecoveriesMsg{Done: []finishedRecovery{copied, replayed}})
	out = strings.Join(m.renderRecovery(time.Now(), 50), "\n")
	for _, want := range []string{
		"Finished while the Shards tab was open",
		"256.0KB/s", "below its 512.0KB/s share: disk or network",
		"rebuilt from operations, no files copied",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("finished list missing %q:\n%s", want, out)
		}
	}
}

// The limit covers a node's incoming plus outgoing traffic: lab3 sends
// two copies, so each gets half even though each target takes one. lab1
// receives one and sends one (shape seen on the lab: 5mb/s gave 2.5MB/s
// each).
func TestRecoveryShareBusierNode(t *testing.T) {
	m := ShardsModel{snap: store.StoreSnapshot{ClusterSettings: cratedb.ClusterSettings{RecoveryMaxBytesPerSec: "100mb"}},
		recoveries: []recovery{{from: "lab3", to: "lab1", size: 100 << 20}, {from: "lab3", to: "lab2", size: 100 << 20}, {to: "lab1"}}}
	m.setShares()
	if m.recoveries[0].share != 50<<20 || m.recoveries[1].share != 50<<20 || m.recoveries[2].share != 0 {
		t.Errorf("shares = %d %d %d", m.recoveries[0].share, m.recoveries[1].share, m.recoveries[2].share)
	}
	if got := m.recoveries[0].fastest(); got != 2*time.Second {
		t.Errorf("fastest = %v", got)
	}

	m.recoveries = []recovery{{from: "lab3", to: "lab1"}, {from: "lab1", to: "lab3"}}
	m.setShares()
	if m.recoveries[0].share != 50<<20 {
		t.Errorf("swap share = %d, want half", m.recoveries[0].share)
	}
}

// A moving replica copies from the primary, not from the node it leaves.
func TestRecoveryMovingReplicaSource(t *testing.T) {
	snap := store.StoreSnapshot{Shards: []cratedb.ShardInfo{
		{SchemaName: "doc", TableName: "t", ID: 1, Primary: true, RoutingState: "STARTED", NodeID: "n3", NodeName: "lab3", Size: 350},
		{SchemaName: "doc", TableName: "t", ID: 1, RoutingState: "RELOCATING", NodeID: "n2", NodeName: "lab2", RelocatingNode: "n1", Size: 250},
		{SchemaName: "doc", TableName: "x", ID: 0, Primary: true, RoutingState: "STARTED", NodeID: "n1", NodeName: "lab1"},
	}}
	m := NewShardsModel(160, 50).Refresh(snap)
	if len(m.recoveries) != 1 || m.recoveries[0].from != "lab3" || m.recoveries[0].to != "lab1" || m.recoveries[0].size != 350 {
		t.Fatalf("recoveries = %+v", m.recoveries)
	}
}

// After a full restart (the reported case): primaries open from their own
// disk, limited by node_initial_primaries_recoveries, not by
// node_concurrent_recoveries or max_bytes_per_sec; the rest wait with the
// decider message checked on the lab.
func TestRecoveryViewLocalPrimaries(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	shards := []cratedb.ShardInfo{
		{SchemaName: "doc", TableName: "sensor", ID: 0, Primary: true, RoutingState: "INITIALIZING", NodeID: "n1", NodeName: "lab1", RecoveryType: "EXISTING_STORE", RecoveryStage: "TRANSLOG"},
		{SchemaName: "doc", TableName: "sensor", ID: 1, Primary: true, RoutingState: "INITIALIZING", NodeID: "n1", NodeName: "lab1", RecoveryType: "EXISTING_STORE", RecoveryStage: "DONE", Size: 40 << 20},
		{SchemaName: "doc", TableName: "sensor", ID: 2, RoutingState: "UNASSIGNED"},
		{SchemaName: "doc", TableName: "x", ID: 0, Primary: true, RoutingState: "STARTED", NodeID: "n2", NodeName: "lab2"},
	}
	allocs := []cratedb.AllocationInfo{
		{TableSchema: "doc", TableName: "sensor", ShardID: 0, Primary: true, CurrentState: "INITIALIZING", NodeID: "n1"},
		{TableSchema: "doc", TableName: "sensor", ShardID: 1, Primary: true, CurrentState: "INITIALIZING", NodeID: "n1"},
		{TableSchema: "doc", TableName: "sensor", ShardID: 2, Primary: true, CurrentState: "UNASSIGNED",
			Decisions: []cratedb.AllocationDecision{decision("lab1", "reached the limit of ongoing initial primary recoveries [2], cluster setting [cluster.routing.allocation.node_initial_primaries_recoveries=2]")}},
	}
	snap := store.StoreSnapshot{Shards: shards, Allocations: allocs, ClusterSettings: cratedb.ClusterSettings{
		NodeConcurrentRecoveries: 8, NodeInitialPrimariesRecoveries: 2, RecoveryMaxBytesPerSec: "0b"}}
	m := NewShardsModel(220, 60).Refresh(snap)
	if m.queuedLocal != 1 || m.queued != 0 {
		t.Fatalf("queuedLocal=%d queued=%d", m.queuedLocal, m.queued)
	}
	if out := m.View(); !strings.Contains(out, "waiting for a primary recovery slot (node_initial_primaries_recoveries)") {
		t.Errorf("no reason for the queued primary:\n%s", out)
	}
	m, _ = m.HandleKey(keyRune('v'))
	out := strings.Join(m.renderRecovery(time.Now(), 60), "\n")
	for _, want := range []string{
		"Copies: 0 running · 0 queued for a slot · 8 in/out per node (node_concurrent_recoveries) · unlimited per node",
		"Primaries opening (own disk, snapshot or replication): 2 running · 1 queued · 2 per node (node_initial_primaries_recoveries)",
		"OPENING", "0/8     0/8     2/2",
		"own disk: replaying the translog", "own disk: done, about to start",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("recovery view missing %q:\n%s", want, out)
		}
	}
	// No size while the files aren't open; 40MB once they are.
	if !regexp.MustCompile(`sensor\s+0\s+P\s+local disk → lab1\s+\?`).MatchString(out) || !strings.Contains(out, "40.0MB") {
		t.Errorf("sizes:\n%s", out)
	}
}

func TestRateLabel(t *testing.T) {
	for in, want := range map[string]string{"40mb": "40mb", "0b": "unlimited", "0": "unlimited", "-1": "unlimited", "0mb": "unlimited", "": "?"} {
		if got := rateLabel(in); got != want {
			t.Errorf("rateLabel(%q) = %q, want %q", in, got, want)
		}
	}
}
