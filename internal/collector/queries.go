package collector

import (
	"context"
	"sort"
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

func (c *QueriesCollector) Name() string            { return "queries" }
func (c *QueriesCollector) Interval() time.Duration { return c.interval }

// LEFT JOIN preserves jobs that have no sys.operations row yet (planning
// phase) — the join-side columns come back as NULL for those rows.
const activeJobsQuery = `SELECT
	j.id, j.node['name'] AS node_name, j.started, j.stmt, j.username,
	o.name, o.used_bytes, o.node['name'] AS op_node, o.started AS op_started
FROM sys.jobs j
LEFT JOIN sys.operations o ON o.job_id = j.id
ORDER BY j.started ASC, o.used_bytes DESC`

func (c *QueriesCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	resp, err := trackedQuery(ctx, c.tracker, QueryActiveJobs, reg, activeJobsQuery)
	if err != nil {
		return err
	}

	queries := aggregateActiveQueries(resp.Rows)
	st.UpdateActiveQueries(queries)
	return nil
}

// aggregateActiveQueries groups (job, operation) rows back into one ActiveQuery
// per job_id, summing used_bytes and sorting operations by memory desc.
// Kept as a pure function so it can be tested without a live database.
func aggregateActiveQueries(rows [][]interface{}) []cratedb.ActiveQuery {
	type pos struct{ idx int }
	byID := make(map[string]*pos, len(rows))
	out := make([]cratedb.ActiveQuery, 0, len(rows))

	for _, row := range rows {
		id := cratedb.ToString(row[0])
		p, ok := byID[id]
		if !ok {
			q := cratedb.ActiveQuery{
				ID:       id,
				Node:     cratedb.ToString(row[1]),
				Stmt:     cratedb.ToString(row[3]),
				Username: cratedb.ToString(row[4]),
			}
			if ts := cratedb.ToFloat64(row[2]); ts > 0 {
				q.Started = time.UnixMilli(int64(ts))
			}
			out = append(out, q)
			byID[id] = &pos{idx: len(out) - 1}
			p = byID[id]
		}

		// Operation columns are NULL when LEFT JOIN found no match.
		if row[5] == nil {
			continue
		}
		op := cratedb.Operation{
			Name:      cratedb.ToString(row[5]),
			UsedBytes: cratedb.ToInt64(row[6]),
			NodeName:  cratedb.ToString(row[7]),
		}
		if ts := cratedb.ToFloat64(row[8]); ts > 0 {
			op.Started = time.UnixMilli(int64(ts))
		}
		q := &out[p.idx]
		q.UsedBytes += op.UsedBytes
		q.Operations = append(q.Operations, op)
	}

	// Defensive: ORDER BY in SQL puts ops in desc order per job, but be
	// explicit so the contract holds regardless of how rows arrive.
	for i := range out {
		sort.SliceStable(out[i].Operations, func(a, b int) bool {
			return out[i].Operations[a].UsedBytes > out[i].Operations[b].UsedBytes
		})
	}
	return out
}
