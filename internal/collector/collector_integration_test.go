//go:build integration

package collector_test

import (
	"context"
	"os"
	"testing"

	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
	"github.com/waltergrande/cratedb-observer/internal/testutil"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.Cleanup()
	os.Exit(code)
}

type testHarness struct {
	Registry *cratedb.Registry
	Store    *store.Store
	Tracker  *collector.QueryTracker
	Cfg      *config.Config
	Ctx      context.Context
}

func setup(t *testing.T) *testHarness {
	t.Helper()
	cdb := testutil.StartCrateDB(t)
	cfg := config.DefaultConfig()

	reg := cratedb.NewRegistry(
		cdb.Endpoint, "crate", "",
		cfg.Connection.Timeout.Duration,
		cfg.Connection.QueryTimeout.Duration,
		cfg.Connection.HeartbeatInterval.Duration,
		cfg.Connection.NodeRefreshInterval.Duration,
		false,
	)

	ctx := context.Background()
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	tracker := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
	reg.SetRecorder(tracker)

	st := store.New(cfg.TUI.SparklineHistory, cfg.Collectors)

	return &testHarness{
		Registry: reg,
		Store:    st,
		Tracker:  tracker,
		Cfg:      cfg,
		Ctx:      ctx,
	}
}

func TestNodesCollectorMapsAllFields(t *testing.T) {
	// The nodes collector has a 30-column SQL query where column order must
	// match the row[N] indices exactly. A mismatch silently corrupts fields.
	// This test verifies that real CrateDB data lands in the correct struct fields.
	h := setup(t)
	c := collector.NewNodesCollector(h.Cfg.Collectors["nodes"], h.Tracker)

	if err := c.Collect(h.Ctx, h.Registry, h.Store); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	snap := h.Store.Snapshot(1, store.SnapshotHint{IncludeNodes: true})
	if len(snap.Nodes) < 1 {
		t.Fatal("expected at least 1 node")
	}

	node := snap.Nodes[0]

	// These fields come from different column positions in the SQL.
	// If any are zero/empty, the column mapping is likely wrong.
	checks := []struct {
		name string
		ok   bool
	}{
		{"ID", node.ID != ""},
		{"Name", node.Name != ""},
		{"Hostname", node.Hostname != ""},
		{"Version", node.Version != ""},
		{"HeapMax", node.HeapMax > 0},
		{"HeapUsed", node.HeapUsed > 0},
		{"FSTotal", node.FSTotal > 0},
		{"NumCPUs", node.NumCPUs > 0},
		// CPUPercent can legitimately be 0 on idle systems, so skip
	}
	for _, c := range checks {
		if !c.ok {
			t.Errorf("field %s is zero/empty — column mapping may be wrong", c.name)
		}
	}

	t.Logf("node: %s version=%s cpus=%d heap=%dMB fs=%dGB",
		node.Name, node.Version, node.NumCPUs,
		node.HeapMax/(1024*1024),
		node.FSTotal/(1024*1024*1024),
	)
}

func TestShardsCollectorEndToEnd(t *testing.T) {
	// Verifies the full pipeline: create table → collect shards → verify
	// the table appears with correct shard counts and settings.
	h := setup(t)

	_, err := h.Registry.Query(h.Ctx, `CREATE TABLE IF NOT EXISTS test_shards_e2e (id INT PRIMARY KEY, name TEXT) CLUSTERED INTO 4 SHARDS WITH (number_of_replicas=0)`)
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = h.Registry.Query(context.Background(), `DROP TABLE IF EXISTS test_shards_e2e`)
	})

	c := collector.NewShardsCollector(h.Cfg.Collectors["shards"], h.Tracker)
	if err := c.Collect(h.Ctx, h.Registry, h.Store); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	snap := h.Store.Snapshot(1, store.SnapshotHint{IncludeTables: true, IncludeShards: true})

	var found *cratedb.TableInfo
	for i := range snap.Tables {
		if snap.Tables[i].TableName == "test_shards_e2e" {
			found = &snap.Tables[i]
			break
		}
	}
	if found == nil {
		t.Fatal("test_shards_e2e not found in table list")
	}

	if found.PrimaryShards != 4 {
		t.Errorf("expected 4 primary shards (as configured), got %d", found.PrimaryShards)
	}
	if found.ReplicaShards != 0 {
		t.Errorf("expected 0 replica shards (number_of_replicas=0), got %d", found.ReplicaShards)
	}
	if found.Settings.NumberOfShards != 4 {
		t.Errorf("expected Settings.NumberOfShards=4, got %d", found.Settings.NumberOfShards)
	}
	if found.Settings.NumberOfReplicas != "0" {
		t.Errorf("expected Settings.NumberOfReplicas='0', got %q", found.Settings.NumberOfReplicas)
	}

	t.Logf("test_shards_e2e: %d primary, %d replica, settings.shards=%d settings.replicas=%s",
		found.PrimaryShards, found.ReplicaShards,
		found.Settings.NumberOfShards, found.Settings.NumberOfReplicas,
	)
}

func TestShardsCollectorFastPath(t *testing.T) {
	// On a single-node cluster, setting number_of_replicas=1 creates UNASSIGNED
	// replica shards. This lets us exercise the fast-path collection and verify
	// that UpdateShardsPartial correctly merges non-STARTED shards with
	// the STARTED shards from the last full collection.
	h := setup(t)

	_, err := h.Registry.Query(h.Ctx, `CREATE TABLE IF NOT EXISTS test_fastpath (id INT PRIMARY KEY, val TEXT) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas=1)`)
	if err != nil {
		t.Fatalf("create test table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = h.Registry.Query(context.Background(), `DROP TABLE IF EXISTS test_fastpath`)
	})

	c := collector.NewShardsCollector(h.Cfg.Collectors["shards"], h.Tracker)

	// Full collection first — populates all shards including STARTED primaries
	if err := c.Collect(h.Ctx, h.Registry, h.Store); err != nil {
		t.Fatalf("full Collect failed: %v", err)
	}

	snap := h.Store.Snapshot(1, store.SnapshotHint{IncludeShards: true})
	totalAfterFull := len(snap.Shards)
	if totalAfterFull == 0 {
		t.Fatal("expected shards after full collection")
	}

	// Count STARTED vs non-STARTED from the full collection
	startedCount := 0
	nonStartedCount := 0
	for _, s := range snap.Shards {
		if s.RoutingState == "STARTED" {
			startedCount++
		} else {
			nonStartedCount++
		}
	}
	t.Logf("after full collect: %d total, %d STARTED, %d non-STARTED", totalAfterFull, startedCount, nonStartedCount)

	if nonStartedCount == 0 {
		t.Skip("no UNASSIGNED shards — single-node may have auto-allocated replicas; need replicas=1 on single node")
	}

	// Fast-path collection: only queries non-STARTED shards
	if err := c.CollectFastPath(h.Ctx, h.Registry, h.Store); err != nil {
		t.Fatalf("CollectFastPath failed: %v", err)
	}

	snap = h.Store.Snapshot(1, store.SnapshotHint{IncludeShards: true})

	// After fast-path merge: STARTED shards should be preserved from full,
	// non-STARTED shards replaced with fresh data from fast-path
	newStarted := 0
	newNonStarted := 0
	for _, s := range snap.Shards {
		if s.RoutingState == "STARTED" {
			newStarted++
		} else {
			newNonStarted++
		}
	}
	if newStarted != startedCount {
		t.Errorf("STARTED shards changed after fast-path: was %d, now %d (should be preserved)", startedCount, newStarted)
	}
	t.Logf("after fast-path: %d STARTED (preserved), %d non-STARTED (refreshed)", newStarted, newNonStarted)

	// Verify allocations were queried for the non-STARTED shards
	snap = h.Store.Snapshot(1, store.SnapshotHint{IncludeShards: true})
	if len(snap.Allocations) == 0 {
		t.Logf("note: no allocations returned (may require CrateDB 4.2+)")
	} else {
		for _, a := range snap.Allocations {
			if a.CurrentState == "STARTED" {
				t.Errorf("allocation for STARTED shard should not appear: %+v", a)
			}
		}
		t.Logf("allocations: %d entries", len(snap.Allocations))
	}
}

func TestQueryTrackerRecordsAllCollectors(t *testing.T) {
	// Verifies that running all collectors produces tracker stats for each
	// expected query label — catches wiring issues between collectors and tracker.
	h := setup(t)

	// Create a table so the shards collector has work to do
	_, _ = h.Registry.Query(h.Ctx, `CREATE TABLE IF NOT EXISTS test_tracker (id INT PRIMARY KEY) WITH (number_of_replicas=0)`)
	t.Cleanup(func() {
		_, _ = h.Registry.Query(context.Background(), `DROP TABLE IF EXISTS test_tracker`)
	})

	collectors := collector.DefaultCollectors(h.Cfg.Collectors, h.Tracker)
	for _, c := range collectors {
		if err := c.Collect(h.Ctx, h.Registry, h.Store); err != nil {
			t.Logf("collector %s: %v", c.Name(), err)
		}
	}

	stats := h.Tracker.Snapshot()
	statMap := make(map[string]*collector.QueryStat, len(stats))
	for i := range stats {
		statMap[stats[i].Label] = &stats[i]
	}

	// These labels must have been executed at least once
	required := []string{
		collector.QueryClusterSettings,
		collector.QuerySummit,
		collector.QueryClusterChecks,
		collector.QueryTableHealth,
		collector.QueryNodes,
		collector.QueryActiveJobs,
		collector.QueryShards,
		collector.QueryTables,
	}
	for _, label := range required {
		s, ok := statMap[label]
		if !ok {
			t.Errorf("query %q not found in tracker", label)
			continue
		}
		if s.ExecCount == 0 {
			t.Errorf("query %q was never executed", label)
		}
		if s.ExecCount > 0 && s.LastDur <= 0 {
			t.Errorf("query %q: ExecCount=%d but LastDur=%v", label, s.ExecCount, s.LastDur)
		}
	}
}
