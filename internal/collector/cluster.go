package collector

import (
	"context"
	"time"

	"github.com/WalBeh/obsi/internal/config"
	"github.com/WalBeh/obsi/internal/cratedb"
	"github.com/WalBeh/obsi/internal/store"
)

type ClusterCollector struct {
	interval        time.Duration
	lastSummitFetch time.Time
	tracker         *QueryTracker
}

func NewClusterCollector(cfg config.CollectorConfig, tracker *QueryTracker) *ClusterCollector {
	return &ClusterCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *ClusterCollector) Name() string           { return "cluster" }
func (c *ClusterCollector) Interval() time.Duration { return c.interval }

func (c *ClusterCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	resp, err := trackedQuery(ctx, c.tracker, QueryClusterSettings, reg, `SELECT
		settings['cluster']['max_shards_per_node'] AS max_shards_per_node,
		settings['cluster']['routing']['allocation']['enable'] AS alloc_enable,
		settings['cluster']['routing']['allocation']['node_concurrent_recoveries'] AS node_concurrent_recoveries,
		settings['cluster']['routing']['allocation']['cluster_concurrent_rebalance'] AS cluster_concurrent_rebalance,
		settings['indices']['recovery']['max_bytes_per_sec'] AS recovery_max_bytes,
		settings['cluster']['routing']['allocation']['disk']['watermark']['low'] AS wm_low,
		settings['cluster']['routing']['allocation']['disk']['watermark']['high'] AS wm_high,
		settings['cluster']['routing']['allocation']['disk']['watermark']['flood_stage'] AS wm_flood
	FROM sys.cluster`)
	if err != nil {
		return err
	}

	if len(resp.Rows) == 0 {
		return nil
	}

	row := resp.Rows[0]
	settings := cratedb.ClusterSettings{
		MaxShardsPerNode:           int(cratedb.ToFloat64(row[0])),
		AllocationEnable:           cratedb.ToString(row[1]),
		NodeConcurrentRecoveries:   int(cratedb.ToFloat64(row[2])),
		ClusterConcurrentRebalance: int(cratedb.ToFloat64(row[3])),
		RecoveryMaxBytesPerSec:     cratedb.ToString(row[4]),
		DiskWatermarkLow:           cratedb.ToString(row[5]),
		DiskWatermarkHigh:          cratedb.ToString(row[6]),
		DiskWatermarkFlood:         cratedb.ToString(row[7]),
	}

	st.UpdateClusterSettings(settings)

	// Random summit — only fetch every 5 minutes (ORDER BY random() is a full scan)
	if time.Since(c.lastSummitFetch) > SummitRefreshInterval {
		summitResp, err := trackedQuery(ctx, c.tracker, QuerySummit, reg, `SELECT mountain, height, region, country, first_ascent FROM sys.summits ORDER BY random() LIMIT 1`)
		if err == nil && len(summitResp.Rows) > 0 {
			r := summitResp.Rows[0]
			st.UpdateSummit(cratedb.Summit{
				Mountain:    cratedb.ToString(r[0]),
				Height:      int(cratedb.ToFloat64(r[1])),
				Region:      cratedb.ToString(r[2]),
				Country:     cratedb.ToString(r[3]),
				FirstAscent: int(cratedb.ToFloat64(r[4])),
			})
			c.lastSummitFetch = time.Now()
		}
	}

	return nil
}
