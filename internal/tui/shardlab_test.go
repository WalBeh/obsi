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
