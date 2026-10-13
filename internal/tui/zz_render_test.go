//go:build shardlab

package tui

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestZZRender(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	ctx := context.Background()
	cfg := config.DefaultConfig()
	for _, url := range []string{"http://127.0.0.1:44210", "http://127.0.0.1:44200", "http://127.0.0.1:44230"} {
		reg := cratedb.NewRegistry(url, "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
		if err := reg.Bootstrap(ctx); err != nil {
			t.Fatal(err)
		}
		st := store.New(10, cfg.Collectors)
		tr := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
		rc := collector.NewReplicationCollector(cfg.Collectors["replication"], tr)
		for _, c := range []collector.Collector{collector.NewNodesCollector(cfg.Collectors["nodes"], tr), collector.NewShardsCollector(cfg.Collectors["shards"], tr),
			rc, collector.NewSnapshotsCollector(cfg.Collectors["snapshots"], tr), collector.NewHealthCollector(cfg.Collectors["health"], tr)} {
			c.Collect(ctx, reg, st)
		}
		time.Sleep(1500 * time.Millisecond)
		rc.Collect(ctx, reg, st)
		snap := st.Snapshot(1, store.SnapshotHint{IncludeTables: true, IncludeNodes: true, IncludeCluster: true, IncludeHealth: true})
		fmt.Println("=====", url)
		if url == "http://127.0.0.1:44230" {
			fmt.Println(NewSnapshotsModel(140, 60).Refresh(snap).View())
			continue
		}
		fmt.Println(NewReplicationModel(140, 60).Refresh(snap).View())
	}
}
