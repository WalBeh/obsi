package collector

import (
	"context"
	"time"

	"github.com/WalBeh/obsi/internal/config"
	"github.com/WalBeh/obsi/internal/cratedb"
	"github.com/WalBeh/obsi/internal/store"
)

type HealthCollector struct {
	interval time.Duration
	tracker  *QueryTracker
}

func NewHealthCollector(cfg config.CollectorConfig, tracker *QueryTracker) *HealthCollector {
	return &HealthCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *HealthCollector) Name() string           { return "health" }
func (c *HealthCollector) Interval() time.Duration { return c.interval }

func (c *HealthCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	// Fetch cluster checks
	checksResp, err := trackedQuery(ctx, c.tracker, QueryClusterChecks, reg, `SELECT id, severity, description, passed FROM sys.checks ORDER BY severity DESC, passed`)
	if err != nil {
		return err
	}

	checks := make([]cratedb.ClusterCheck, 0, len(checksResp.Rows))
	for _, row := range checksResp.Rows {
		check := cratedb.ClusterCheck{
			ID:          int(cratedb.ToFloat64(row[0])),
			Severity:    int(cratedb.ToFloat64(row[1])),
			Description: cratedb.ToString(row[2]),
			Passed:      cratedb.ToBool(row[3]),
		}
		checks = append(checks, check)
	}

	// Fetch table health
	healthResp, err := trackedQuery(ctx, c.tracker, QueryTableHealth, reg, `SELECT table_schema, table_name, health, missing_shards, underreplicated_shards, partition_ident FROM sys.health ORDER BY health, table_schema, table_name`)
	if err != nil {
		return err
	}

	health := make([]cratedb.TableHealth, 0, len(healthResp.Rows))
	for _, row := range healthResp.Rows {
		h := cratedb.TableHealth{
			TableSchema:     cratedb.ToString(row[0]),
			TableName:       cratedb.ToString(row[1]),
			Health:          cratedb.ToString(row[2]),
			MissingShards:   cratedb.ToInt64(row[3]),
			UnderReplicated: cratedb.ToInt64(row[4]),
			Partition:       cratedb.ToString(row[5]),
		}
		health = append(health, h)
	}

	st.UpdateClusterHealth(checks, health)
	return nil
}

