package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// Shape from a real cluster: four copies left n2 at once and together got
// ~188 MB/s under a 400mb throttle (the volume's read cap), ~170 MB/s
// averaged over the time n2 was busy; right after, the balancer moved two
// of them back to n2.
func TestRecoveryBottleneckAndMovesBack(t *testing.T) {
	now := time.Now()
	shard := func(id int) cratedb.ShardInfo {
		return cratedb.ShardInfo{SchemaName: "doc", TableName: "sensor", ID: id, Primary: true}
	}
	done := func(id int, to string, mbps float64, took time.Duration) finishedRecovery {
		size := int64(mbps * float64(1<<20) * took.Seconds())
		return finishedRecovery{
			recovery: recovery{shard: shard(id), from: "n2", left: "n2", to: to, size: size, share: 100 << 20},
			ended:    now.Add(-time.Minute), took: took, recovered: size, files: 40,
		}
	}
	m := NewShardsModel(200, 60)
	m.snap = store.StoreSnapshot{ClusterSettings: cratedb.ClusterSettings{RecoveryMaxBytesPerSec: "400mb", NodeConcurrentRecoveries: 4}}
	m.nodeNames = map[string]string{"a": "n0", "b": "n1", "c": "n2"}
	m = m.addFinished(FinishedRecoveriesMsg{Done: []finishedRecovery{
		done(5, "n1", 63.9, 351*time.Second),
		done(0, "n0", 41.2, 479*time.Second),
		done(6, "n0", 41.2, 477*time.Second),
		done(7, "n0", 41.4, 477*time.Second),
	}})

	peaks := nodePeaks(m.history)
	if p := peaks["n2"]; p.copies != 4 || p.rate>>20 < 168 || p.rate>>20 > 172 {
		t.Errorf("n2 = %d MB/s ×%d, want ~170 ×4", p.rate>>20, p.copies)
	}
	if p := peaks["n0"]; p.copies != 3 {
		t.Errorf("n0 peak copies = %d, want 3", p.copies)
	}

	m.recoveries = []recovery{
		{shard: shard(6), from: "n0", left: "n0", to: "n2", size: 19 << 30, since: now},
		{shard: shard(7), from: "n0", left: "n0", to: "n2", size: 19 << 30, since: now},
		{shard: shard(9), from: "n1", left: "n1", to: "n2", size: 1 << 30, since: now},
	}
	out := strings.Join(m.renderRecovery(now, 60), "\n")
	for _, want := range []string{
		"MOVED, LAST HOUR",
		"▲ n2 moved 170.",
		"with up to 4 copies at once, under the 400mb/s throttle",
		"↩ 2 moves take a shard back",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("view lacks %q:\n%s", want, out)
		}
	}
	if strings.Count(out, "↩") != 3 { // summary + two rows, not shard 9
		t.Errorf("↩ count = %d:\n%s", strings.Count(out, "↩"), out)
	}
}

// Copies one after another don't add up.
func TestNodePeaksSequential(t *testing.T) {
	now := time.Now()
	f := func(end time.Duration) finishedRecovery {
		return finishedRecovery{recovery: recovery{from: "a", to: "b"}, ended: now.Add(end), took: time.Minute, recovered: 60 << 20, files: 5}
	}
	if p := nodePeaks([]finishedRecovery{f(0), f(-time.Minute)})["a"]; p.copies != 1 || p.rate != 1<<20 {
		t.Errorf("peak = %+v, want 1 MB/s ×1", p)
	}
}

// sys.shards (5s) shows a finished move before sys.allocations (30s): the
// shard stays listed for a while without a target. The target seen earlier
// is kept, so the new copy's record is looked up on the right node.
func TestRecoveryKeepsTargetWhileAllocationsLag(t *testing.T) {
	moving := cratedb.ShardInfo{SchemaName: "doc", TableName: "big", ID: 1, Primary: true, RoutingState: "RELOCATING", NodeID: "n3", NodeName: "lab3", RelocatingNode: "n1", Size: 350}
	other := cratedb.ShardInfo{SchemaName: "doc", TableName: "x", ID: 0, Primary: true, RoutingState: "STARTED", NodeID: "n1", NodeName: "lab1"}
	alloc := []cratedb.AllocationInfo{{TableSchema: "doc", TableName: "big", ShardID: 1, Primary: true, CurrentState: "RELOCATING", NodeID: "n3"}}

	m := NewShardsModel(160, 50).Refresh(store.StoreSnapshot{Shards: []cratedb.ShardInfo{moving, other}, Allocations: alloc})
	done := moving
	done.RoutingState, done.NodeID, done.NodeName, done.RelocatingNode = "STARTED", "n1", "lab1", ""
	m = m.Refresh(store.StoreSnapshot{Shards: []cratedb.ShardInfo{done, other}, Allocations: alloc})
	if len(m.recoveries) != 1 || m.recoveries[0].to != "lab1" {
		t.Fatalf("while allocations lag: %+v", m.recoveries)
	}
	doneAt := m.recoveries[0].doneAt
	time.Sleep(10 * time.Millisecond)
	m = m.Refresh(store.StoreSnapshot{Shards: []cratedb.ShardInfo{done, other}})
	if len(m.pending) != 1 || m.pending[0].to != "lab1" || m.pending[0].left != "lab3" {
		t.Errorf("pending = %+v", m.pending)
	}
	// Ended when sys.shards showed it done, not when allocations caught up.
	if doneAt.IsZero() || !m.pending[0].ended.Equal(doneAt) {
		t.Errorf("ended %v, want %v", m.pending[0].ended, doneAt)
	}
}

// A copy's rate changes as others finish: on the lab one ran at 2MB/s
// beside another and at 6MB/s after it, averaging 3; adding averages gave
// 7MB/s under a 6mb throttle. Bytes over busy time gives 6.
func TestNodePeaksVaryingRates(t *testing.T) {
	now := time.Now()
	a := finishedRecovery{recovery: recovery{from: "lab3", to: "lab1"}, ended: now.Add(-29 * time.Second), took: 87 * time.Second, recovered: 348 << 20, files: 9}
	b := finishedRecovery{recovery: recovery{from: "lab1", to: "lab2"}, ended: now, took: 116 * time.Second, recovered: 348 << 20, files: 9}
	p := nodePeaks([]finishedRecovery{a, b})["lab1"]
	if p.copies != 2 || p.rate>>20 != 6 {
		t.Errorf("lab1 = %d MB/s ×%d, want 6 ×2", p.rate>>20, p.copies)
	}
}
