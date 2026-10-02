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
	tracker  *QueryTracker
}

func NewHealthCollector(cfg config.CollectorConfig, tracker *QueryTracker) *HealthCollector {
	return &HealthCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *HealthCollector) Name() string            { return "health" }
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
	healthResp, err := trackedQuery(ctx, c.tracker, QueryTableHealth, reg, `SELECT table_schema, table_name, health, missing_shards, underreplicated_shards, partition_ident FROM sys.health WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog', 'blob') ORDER BY health, table_schema, table_name`)
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

	// Both tables are extra: a failure here (missing table on an old version,
	// no privilege) leaves the checks and table health above intact.
	var cluster *cratedb.ClusterHealth
	if resp, err := trackedQuery(ctx, c.tracker, QueryClusterHealth, reg, clusterHealthQuery); err == nil {
		cluster = parseClusterHealth(resp.Rows)
	}
	var nodeChecks []cratedb.NodeCheck
	if resp, err := trackedQuery(ctx, c.tracker, QueryNodeChecks, reg, nodeChecksQuery); err == nil {
		nodeChecks = parseNodeChecks(resp.Rows)
	}
	st.UpdateClusterStatus(cluster, nodeChecks)
	return nil
}

const clusterHealthQuery = `SELECT health, description, pending_tasks, missing_shards, underreplicated_shards FROM sys.cluster_health`

const nodeChecksQuery = `SELECT id, node_id, severity, description, passed, acknowledged FROM sys.node_checks ORDER BY id, node_id`

func parseClusterHealth(rows [][]interface{}) *cratedb.ClusterHealth {
	if len(rows) == 0 || len(rows[0]) < 5 {
		return nil
	}
	r := rows[0]
	return &cratedb.ClusterHealth{
		Health:          cratedb.ToString(r[0]),
		Description:     cratedb.ToString(r[1]),
		PendingTasks:    cratedb.ToInt64(r[2]),
		MissingShards:   cratedb.ToInt64(r[3]),
		UnderReplicated: cratedb.ToInt64(r[4]),
	}
}

func parseNodeChecks(rows [][]interface{}) []cratedb.NodeCheck {
	out := make([]cratedb.NodeCheck, 0, len(rows))
	for _, r := range rows {
		if len(r) < 6 {
			continue
		}
		out = append(out, cratedb.NodeCheck{
			ID:           int(cratedb.ToInt64(r[0])),
			NodeID:       cratedb.ToString(r[1]),
			Severity:     int(cratedb.ToInt64(r[2])),
			Description:  cratedb.ToString(r[3]),
			Passed:       cratedb.ToBool(r[4]),
			Acknowledged: cratedb.ToBool(r[5]),
		})
	}
	return out
}
