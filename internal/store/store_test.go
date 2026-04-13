package store

import (
	"slices"
	"testing"
	"time"

	"github.com/WalBeh/obsi/internal/config"
	"github.com/WalBeh/obsi/internal/cratedb"
)

func TestRingBuf(t *testing.T) {
	t.Run("push and slice under capacity", func(t *testing.T) {
		rb := NewRingBuf[int](5)
		rb.Push(1)
		rb.Push(2)
		rb.Push(3)

		got := rb.Slice()
		want := []int{1, 2, 3}
		if !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if rb.Len() != 3 {
			t.Errorf("Len=%d, want 3", rb.Len())
		}
	})

	t.Run("wraps around at capacity", func(t *testing.T) {
		rb := NewRingBuf[int](3)
		for i := 1; i <= 5; i++ {
			rb.Push(i)
		}
		got := rb.Slice()
		want := []int{3, 4, 5}
		if !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if rb.Len() != 3 {
			t.Errorf("Len=%d, want 3", rb.Len())
		}
	})

	t.Run("Last returns most recent", func(t *testing.T) {
		rb := NewRingBuf[string](3)
		if _, ok := rb.Last(); ok {
			t.Error("Last on empty buffer should return ok=false")
		}
		rb.Push("a")
		rb.Push("b")
		if v, ok := rb.Last(); !ok || v != "b" {
			t.Errorf("Last=%q ok=%v, want 'b' true", v, ok)
		}
	})

	t.Run("empty slice is nil", func(t *testing.T) {
		rb := NewRingBuf[float64](10)
		if s := rb.Slice(); s != nil {
			t.Errorf("expected nil slice for empty buffer, got %v", s)
		}
	})
}

func TestStoreIORateDerivation(t *testing.T) {
	cfg := config.DefaultConfig()
	st := New(10, cfg.Collectors)

	// First sample: establishes baseline counters
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1", FSReads: 100, FSWrites: 50, FSBytesRead: 1000, FSBytesWritten: 500}},
	})

	// First sample should have zero rates (no previous to diff against)
	snap := st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if snap.Nodes[0].ReadIOPS != 0 {
		t.Errorf("first sample: expected ReadIOPS=0, got %f", snap.Nodes[0].ReadIOPS)
	}

	// Backdate previous sample to simulate exactly 2 seconds elapsed
	st.mu.Lock()
	st.prevIOTime = st.prevIOTime.Add(-2 * time.Second)
	st.mu.Unlock()

	// Second sample: counters increased by known amounts
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1", FSReads: 300, FSWrites: 150, FSBytesRead: 5000, FSBytesWritten: 2500}},
	})

	snap = st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	n := snap.Nodes[0]
	// Delta: 200 reads / ~2s ≈ 100 IOPS, 100 writes / ~2s ≈ 50 IOPS
	// Delta: 4000 bytes read / ~2s ≈ 2000 B/s, 2000 bytes written / ~2s ≈ 1000 B/s
	// Use 1% tolerance because UpdateNodes calls time.Now() internally,
	// so elapsed is not exactly 2s.
	assertApprox(t, "ReadIOPS", n.ReadIOPS, 100, 0.01)
	assertApprox(t, "WriteIOPS", n.WriteIOPS, 50, 0.01)
	assertApprox(t, "ReadThroughput", n.ReadThroughput, 2000, 0.01)
	assertApprox(t, "WriteThroughput", n.WriteThroughput, 1000, 0.01)
}

func TestStoreIORateClampedToZero(t *testing.T) {
	cfg := config.DefaultConfig()
	st := New(10, cfg.Collectors)

	// Simulate counter reset (e.g. node restart): new counters lower than previous
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", FSReads: 1000, FSWrites: 500, FSBytesRead: 10000, FSBytesWritten: 5000}},
	})

	st.mu.Lock()
	st.prevIOTime = st.prevIOTime.Add(-1 * time.Second)
	st.mu.Unlock()

	// Counters went DOWN (node restarted, counters reset)
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", FSReads: 10, FSWrites: 5, FSBytesRead: 100, FSBytesWritten: 50}},
	})

	snap := st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	n := snap.Nodes[0]
	// max(..., 0) should clamp negative deltas to zero
	if n.ReadIOPS != 0 {
		t.Errorf("expected ReadIOPS=0 after counter reset, got %f", n.ReadIOPS)
	}
	if n.WriteIOPS != 0 {
		t.Errorf("expected WriteIOPS=0 after counter reset, got %f", n.WriteIOPS)
	}
}

func TestStoreRejectionDeltas(t *testing.T) {
	cfg := config.DefaultConfig()
	st := New(10, cfg.Collectors)

	pools := func(writeRej, searchRej int64) []cratedb.ThreadPoolStats {
		return []cratedb.ThreadPoolStats{
			{Name: "write", Rejected: writeRej, Active: 1},
			{Name: "search", Rejected: searchRej, Active: 1},
			{Name: "generic", Rejected: 0, Active: 1},
		}
	}

	// First sample: baseline — delta should be 0
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", ThreadPools: pools(10, 5)}},
	})
	snap := st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if snap.Nodes[0].ThreadPoolNewRejections != 0 {
		t.Errorf("first sample: expected 0 new rejections, got %d", snap.Nodes[0].ThreadPoolNewRejections)
	}

	// Second sample: 3 new write rejections + 2 new search rejections = 5 total
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", ThreadPools: pools(13, 7)}},
	})
	snap = st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if snap.Nodes[0].ThreadPoolNewRejections != 5 {
		t.Errorf("expected 5 new rejections (3 write + 2 search), got %d", snap.Nodes[0].ThreadPoolNewRejections)
	}

	// Third sample: no new rejections — delta should be 0 again
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", ThreadPools: pools(13, 7)}},
	})
	snap = st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if snap.Nodes[0].ThreadPoolNewRejections != 0 {
		t.Errorf("third sample: expected 0 new rejections, got %d", snap.Nodes[0].ThreadPoolNewRejections)
	}
}

func TestStoreNodeDisappearance(t *testing.T) {
	cfg := config.DefaultConfig()
	st := New(10, cfg.Collectors)

	// Two nodes present
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1"}},
		{NodeInfo: cratedb.NodeInfo{ID: "n2", Name: "node-2"}},
	})
	snap := st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if len(snap.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(snap.Nodes))
	}

	// node-2 disappears: should be marked Gone, not silently dropped
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1"}},
	})
	snap = st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if len(snap.Nodes) != 2 {
		t.Fatalf("expected 2 nodes (1 active + 1 gone), got %d", len(snap.Nodes))
	}

	goneCount := 0
	for _, n := range snap.Nodes {
		if n.Gone {
			goneCount++
			if n.Name != "node-2" {
				t.Errorf("expected gone node to be node-2, got %s", n.Name)
			}
		}
	}
	if goneCount != 1 {
		t.Errorf("expected 1 gone node, got %d", goneCount)
	}

	// node-2 comes back: should no longer be Gone
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1"}},
		{NodeInfo: cratedb.NodeInfo{ID: "n2", Name: "node-2"}},
	})
	snap = st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	for _, n := range snap.Nodes {
		if n.Gone {
			t.Errorf("node %s still marked Gone after reappearing", n.Name)
		}
	}
}

func TestStoreNodeDisappearanceTimeout(t *testing.T) {
	cfg := config.DefaultConfig()
	st := New(10, cfg.Collectors)

	// Two nodes present
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1"}},
		{NodeInfo: cratedb.NodeInfo{ID: "n2", Name: "node-2"}},
	})

	// node-2 disappears
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1"}},
	})

	// Verify n2 is still tracked as Gone
	snap := st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if len(snap.Nodes) != 2 {
		t.Fatalf("expected 2 nodes (1 active + 1 gone), got %d", len(snap.Nodes))
	}

	// Backdate n2's LastSeen to exceed nodeDisappearanceTimeout (5 min)
	st.mu.Lock()
	if prev, ok := st.knownNodes["n2"]; ok {
		prev.LastSeen = time.Now().Add(-6 * time.Minute)
		st.knownNodes["n2"] = prev
	}
	st.mu.Unlock()

	// Next update should purge n2 entirely — not even shown as Gone
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1"}},
	})
	snap = st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if len(snap.Nodes) != 1 {
		t.Errorf("expected 1 node after timeout purge, got %d", len(snap.Nodes))
		for _, n := range snap.Nodes {
			t.Logf("  node: %s gone=%v", n.Name, n.Gone)
		}
	}

	// Verify internal tracking maps are also cleaned up
	st.mu.RLock()
	_, inKnown := st.knownNodes["n2"]
	_, inHistory := st.nodeHistories["n2"]
	_, inIO := st.prevIOSample["n2"]
	_, inRej := st.prevRejected["n2"]
	st.mu.RUnlock()

	if inKnown {
		t.Error("n2 still in knownNodes after timeout")
	}
	if inHistory {
		t.Error("n2 still in nodeHistories after timeout")
	}
	if inIO {
		t.Error("n2 still in prevIOSample after timeout")
	}
	if inRej {
		t.Error("n2 still in prevRejected after timeout")
	}
}

func TestUpdateShardsPartialMerge(t *testing.T) {
	cfg := config.DefaultConfig()
	st := New(10, cfg.Collectors)

	// Simulate a full shard collection: 3 STARTED + 1 INITIALIZING
	st.UpdateTables(nil, 0, []cratedb.ShardInfo{
		{ID: 0, SchemaName: "doc", TableName: "t1", RoutingState: "STARTED", NumDocs: 100},
		{ID: 1, SchemaName: "doc", TableName: "t1", RoutingState: "STARTED", NumDocs: 200},
		{ID: 2, SchemaName: "doc", TableName: "t1", RoutingState: "STARTED", NumDocs: 300},
		{ID: 3, SchemaName: "doc", TableName: "t1", RoutingState: "INITIALIZING", NumDocs: 0},
	})

	// Fast-path update: only non-STARTED shards, now shard 3 has recovered
	st.UpdateShardsPartial([]cratedb.ShardInfo{
		// shard 3 is now RELOCATING (still not STARTED)
		{ID: 3, SchemaName: "doc", TableName: "t1", RoutingState: "RELOCATING", NumDocs: 50},
	})

	snap := st.Snapshot(1, SnapshotHint{IncludeShards: true})

	// Should have 3 STARTED (kept from full) + 1 RELOCATING (from partial) = 4
	if len(snap.Shards) != 4 {
		t.Fatalf("expected 4 shards after partial merge, got %d", len(snap.Shards))
	}

	startedCount := 0
	var relocating *cratedb.ShardInfo
	for i := range snap.Shards {
		if snap.Shards[i].RoutingState == "STARTED" {
			startedCount++
		}
		if snap.Shards[i].RoutingState == "RELOCATING" {
			relocating = &snap.Shards[i]
		}
	}
	if startedCount != 3 {
		t.Errorf("expected 3 STARTED shards preserved, got %d", startedCount)
	}
	if relocating == nil {
		t.Fatal("expected 1 RELOCATING shard from partial update")
	}
	if relocating.NumDocs != 50 {
		t.Errorf("RELOCATING shard NumDocs: got %d, want 50", relocating.NumDocs)
	}

	// Another partial update: shard 3 is now fully recovered (empty non-STARTED list)
	st.UpdateShardsPartial(nil)
	snap = st.Snapshot(1, SnapshotHint{IncludeShards: true})
	// Should have only the 3 STARTED shards — the old RELOCATING is gone
	if len(snap.Shards) != 3 {
		t.Errorf("expected 3 shards after empty partial update, got %d", len(snap.Shards))
	}
}

func TestSnapshotHintSelectivity(t *testing.T) {
	cfg := config.DefaultConfig()
	st := New(10, cfg.Collectors)

	st.UpdateClusterSettings(cratedb.ClusterSettings{MaxShardsPerNode: 1000})
	st.UpdateNodes([]NodeSnapshot{
		{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "node-1"}},
	})
	st.UpdateActiveQueries([]cratedb.ActiveQuery{
		{ID: "q1", Stmt: "SELECT 1"},
	})

	// Requesting only cluster should not copy nodes or queries
	snap := st.Snapshot(1, SnapshotHint{IncludeCluster: true})
	if snap.ClusterSettings.MaxShardsPerNode != 1000 {
		t.Error("cluster settings not included")
	}
	if len(snap.Nodes) != 0 {
		t.Error("nodes copied despite IncludeNodes=false")
	}
	if len(snap.ActiveQueries) != 0 {
		t.Error("queries copied despite IncludeQueries=false")
	}

	// Requesting only nodes should not copy cluster settings
	snap = st.Snapshot(1, SnapshotHint{IncludeNodes: true})
	if len(snap.Nodes) != 1 {
		t.Error("nodes not included")
	}
	if snap.ClusterSettings.MaxShardsPerNode != 0 {
		t.Error("cluster settings copied despite IncludeCluster=false")
	}
}

func assertApprox(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	if want == 0 {
		if got != 0 {
			t.Errorf("%s: got %f, want 0", name, got)
		}
		return
	}
	if diff/want > tolerance {
		t.Errorf("%s: got %f, want ≈%f (tolerance %.0f%%)", name, got, want, tolerance*100)
	}
}
