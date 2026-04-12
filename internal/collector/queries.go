package collector

import (
	"context"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

type QueriesCollector struct {
	interval time.Duration
	tracker  *QueryTracker
}

func NewQueriesCollector(cfg config.CollectorConfig, tracker *QueryTracker) *QueriesCollector {
	return &QueriesCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *QueriesCollector) Name() string           { return "queries" }
func (c *QueriesCollector) Interval() time.Duration { return c.interval }

func (c *QueriesCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	resp, err := trackedQuery(ctx, c.tracker, QueryActiveJobs, reg, `SELECT
		id, node['name'] AS node_name, started, stmt, username
	FROM sys.jobs
	ORDER BY started ASC`)
	if err != nil {
		return err
	}

	queries := make([]cratedb.ActiveQuery, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		q := cratedb.ActiveQuery{
			ID:       toString(row[0]),
			Node:     toString(row[1]),
			Stmt:     toString(row[3]),
			Username: toString(row[4]),
		}

		// CrateDB returns timestamps as milliseconds since epoch
		if ts := toFloat64(row[2]); ts > 0 {
			q.Started = time.UnixMilli(int64(ts))
		}

		queries = append(queries, q)
	}

	st.UpdateActiveQueries(queries)
	return nil
}
