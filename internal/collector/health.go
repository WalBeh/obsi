package collector

import (
	"context"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

type HealthCollector struct {
	interval time.Duration
}

func NewHealthCollector(cfg config.CollectorConfig) *HealthCollector {
	return &HealthCollector{interval: cfg.Interval.Duration}
}

func (c *HealthCollector) Name() string           { return "health" }
func (c *HealthCollector) Interval() time.Duration { return c.interval }

func (c *HealthCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	// Fetch cluster checks
	checksResp, err := reg.Query(ctx, `SELECT id, severity, description, passed FROM sys.checks ORDER BY severity DESC, passed`)
	if err != nil {
		return err
	}

	checks := make([]cratedb.ClusterCheck, 0, len(checksResp.Rows))
	for _, row := range checksResp.Rows {
		check := cratedb.ClusterCheck{
			ID:          int(toFloat64(row[0])),
			Severity:    int(toFloat64(row[1])),
			Description: toString(row[2]),
			Passed:      toBool(row[3]),
		}
		checks = append(checks, check)
	}

	// Fetch table health
	healthResp, err := reg.Query(ctx, `SELECT table_schema, table_name, health, missing_shards, underreplicated_shards, partition_ident FROM sys.health ORDER BY health, table_schema, table_name`)
	if err != nil {
		return err
	}

	health := make([]cratedb.TableHealth, 0, len(healthResp.Rows))
	for _, row := range healthResp.Rows {
		h := cratedb.TableHealth{
			TableSchema:     toString(row[0]),
			TableName:       toString(row[1]),
			Health:          toString(row[2]),
			MissingShards:   int64(toFloat64(row[3])),
			UnderReplicated: int64(toFloat64(row[4])),
			Partition:       toString(row[5]),
		}
		health = append(health, h)
	}

	st.UpdateClusterHealth(checks, health)
	return nil
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	}
	return 0
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}
