//go:build shardlab

package tui

import (
	"context"
	"fmt"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// TestShardlab prints the Shards tab for the local shardlab cluster, see
// docs/shardlab.md. go test -tags shardlab -run Shardlab -v ./internal/tui
func TestShardlab(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	ctx := context.Background()
	reg := cratedb.NewRegistry("http://127.0.0.1:44200", "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	st := store.New(10, cfg.Collectors)
	tracker := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
	for _, c := range []collector.Collector{
		collector.NewClusterCollector(cfg.Collectors["cluster"], tracker),
		collector.NewNodesCollector(cfg.Collectors["nodes"], tracker),
	} {
		if err := c.Collect(ctx, reg, st); err != nil {
			t.Fatal(err)
		}
	}
	shards := collector.NewShardsCollector(cfg.Collectors["shards"], tracker)
	if err := shards.Collect(ctx, reg, st); err != nil {
		t.Fatal(err)
	}
	if err := shards.CollectFastPath(ctx, reg, st); err != nil { // stuck check
		t.Fatal(err)
	}
	snap := st.Snapshot(1, store.SnapshotHint{IncludeShards: true, IncludeCluster: true, IncludeNodes: true})
	fmt.Println(NewShardsModel(160, 50).Refresh(snap).View())
}

// TestChangeslab runs a few config changes on the shardlab and prints the
// changes board. go test -tags shardlab -run Changeslab -v ./internal/tui
func TestChangeslab(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	ctx := context.Background()
	reg := cratedb.NewRegistry("http://127.0.0.1:44200", "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	st := store.New(10, cfg.Collectors)
	tracker := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
	changes := collector.NewChangesCollector(cfg.Collectors["changes"], tracker)
	run := func(stmt string, args ...interface{}) {
		if _, err := reg.Query(ctx, stmt, args...); err != nil {
			t.Logf("%s: %v", stmt, err)
		}
	}
	run(`CREATE TABLE IF NOT EXISTS doc.changelab (a INT) WITH (number_of_replicas = 0)`)
	if err := changes.Collect(ctx, reg, st); err != nil {
		t.Fatal(err)
	}
	run(`SET GLOBAL TRANSIENT "indices.recovery.max_bytes_per_sec" = ?`+cratedb.ChangeTag, "123mb")
	run(`SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 'primaries'`)
	run(`SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 7`)
	run(`SET search_path TO doc`)
	run(`ALTER TABLE doc.changelab SET (number_of_replicas = 1, refresh_interval = 5000)`)
	run(`CREATE USER changelab_u WITH (password = 'not-shown')`)
	run(`DROP USER changelab_u`)
	run(`CREATE PUBLICATION changelab_p FOR TABLE doc.changelab`)
	run(`CREATE SUBSCRIPTION changelab_s CONNECTION 'crate://changelab-nohost:4300?user=u&password=not-shown' PUBLICATION changelab_p`)
	run(`DROP PUBLICATION changelab_p`)
	time.Sleep(500 * time.Millisecond)
	if err := changes.Collect(ctx, reg, st); err != nil {
		t.Fatal(err)
	}
	run(`RESET GLOBAL "indices.recovery.max_bytes_per_sec", "cluster.routing.allocation.enable"`)
	time.Sleep(500 * time.Millisecond)
	for range 2 { // the second finds nothing new; no unseen diffs either
		if err := changes.Collect(ctx, reg, st); err != nil {
			t.Fatal(err)
		}
	}
	run(`DROP TABLE doc.changelab`)
	snap := st.Snapshot(1, store.SnapshotHint{IncludeQueries: true})
	m := NewQueriesModel(170, 40).Refresh(snap)
	for _, r := range "cjjj" { // down to the ALTER TABLE
		m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	fmt.Println(m.View())
	for _, s := range tracker.Snapshot() {
		if s.Category == "changes" {
			fmt.Printf("%s: %d runs, last %s, %d rows\n", s.Label, s.ExecCount, s.LastDur, s.LastRows)
		}
	}
}

// TestReplicationlab prints the Replication tab for the publisher (44200)
// and the subscriber (44210) after ./scenarios.sh replication.
// go test -tags shardlab -run Replicationlab -v ./internal/tui
func TestReplicationlab(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	ctx := context.Background()
	cfg := config.DefaultConfig()
	type side struct {
		reg *cratedb.Registry
		st  *store.Store
		c   *collector.ReplicationCollector
		tr  *collector.QueryTracker
	}
	var sides []side
	for _, url := range []string{"http://127.0.0.1:44200", "http://127.0.0.1:44210"} {
		reg := cratedb.NewRegistry(url, "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
		if err := reg.Bootstrap(ctx); err != nil {
			t.Logf("%s: %v", url, err) // sub-down
			continue
		}
		tr := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
		sides = append(sides, side{reg, store.New(10, cfg.Collectors), collector.NewReplicationCollector(cfg.Collectors["replication"], tr), tr})
	}
	collect := func() {
		for _, s := range sides {
			if err := s.c.Collect(ctx, s.reg, s.st); err != nil {
				t.Fatal(err)
			}
		}
	}
	collect()
	if _, err := sides[0].reg.Query(ctx, `INSERT INTO doc.repl SELECT g, md5(g::TEXT) FROM generate_series(1, 2000) g`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	collect()
	for _, s := range sides {
		snap := s.st.Snapshot(1, store.SnapshotHint{})
		fmt.Printf("==== %s\n%s\n", s.reg.Status().ClusterName, NewReplicationModel(120, 40).Refresh(snap).View())
		for _, a := range snap.Alerts.History {
			fmt.Println("alert:", a.Message)
		}
		for _, q := range s.tr.Snapshot() {
			if q.Category == "replication" && q.ExecCount > 0 {
				fmt.Printf("%s: %d runs, last %s, %d rows\n", q.Label, q.ExecCount, q.LastDur, q.LastRows)
			}
		}
	}
}
