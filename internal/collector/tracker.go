package collector

import (
	"context"
	"sync"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// Query labels — the canonical names used in the query log overlay
// and (future) markdown documentation. Adding or removing a label here
// is the single source of truth; the markdown doc will reference these.
const (
	QueryClusterSettings = "cluster.settings"
	QuerySummit          = "cluster.summit"
	QueryClusterChecks   = "health.checks"
	QueryTableHealth     = "health.tables"
	QueryNodes           = "nodes.info"
	QueryActiveJobs      = "queries.active_jobs"
	QueryShards          = "shards.all"
	QueryShardsFastPath  = "shards.fast_path"
	QueryTables          = "shards.tables"
	QueryViewCount       = "shards.view_count"
	QueryAllocations     = "shards.allocations"
	QueryHeartbeat     = cratedb.QueryLabelHeartbeat
	QueryBootstrap     = cratedb.QueryLabelBootstrap
	QueryNodeDiscovery = cratedb.QueryLabelNodeDiscovery
)

// QueryStat holds execution statistics for a single query label.
type QueryStat struct {
	Label       string
	Category    string // collector or subsystem name (e.g. "cluster", "registry")
	ExecCount   int64
	LastExec    time.Time
	LastDur     time.Duration
	LastRows    int64
	TotalRows   int64
	LastErr     error
	LastErrTime time.Time
	ErrCount    int64
	Interval    time.Duration // base polling interval (0 = one-shot or event-driven)
}

// AvgRows returns the average row count across all executions.
func (s *QueryStat) AvgRows() float64 {
	if s.ExecCount == 0 {
		return 0
	}
	return float64(s.TotalRows) / float64(s.ExecCount)
}

// QueryTracker records per-query execution statistics.
// Safe for concurrent use.
type QueryTracker struct {
	mu    sync.RWMutex
	stats map[string]*QueryStat
}

// NewQueryTracker creates a tracker pre-populated with all known query labels.
// Collector intervals are sourced from cfg so the overlay reflects the user's
// actual configuration, not hardcoded defaults.
func NewQueryTracker(cfg map[string]config.CollectorConfig) *QueryTracker {
	t := &QueryTracker{
		stats: make(map[string]*QueryStat),
	}

	ci := func(name string) time.Duration {
		if c, ok := cfg[name]; ok {
			return c.Interval.Duration
		}
		return 0
	}

	defs := []queryDef{
		{QueryClusterSettings, "cluster", ci("cluster")},
		{QuerySummit, "cluster", SummitRefreshInterval},
		{QueryClusterChecks, "health", ci("health")},
		{QueryTableHealth, "health", ci("health")},
		{QueryNodes, "nodes", ci("nodes")},
		{QueryActiveJobs, "queries", ci("queries")},
		{QueryShards, "shards", ci("shards")},
		{QueryShardsFastPath, "shards", FastPathInterval},
		{QueryTables, "shards", ci("shards")},
		{QueryViewCount, "shards", ci("shards")},
		{QueryAllocations, "shards", ci("shards")},
		{QueryHeartbeat, "registry", FastPathInterval}, // approximates default HeartbeatInterval
		{QueryBootstrap, "registry", 0},
		{QueryNodeDiscovery, "registry", 0},
	}

	for _, def := range defs {
		t.stats[def.label] = &QueryStat{
			Label:    def.label,
			Category: def.category,
			Interval: def.interval,
		}
	}
	return t
}

type queryDef struct {
	label    string
	category string
	interval time.Duration // 0 = event-driven / one-shot
}

// Record logs a successful query execution.
func (t *QueryTracker) Record(label string, dur time.Duration, rows int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.getOrCreate(label)
	s.ExecCount++
	s.LastExec = time.Now()
	s.LastDur = dur
	s.LastRows = rows
	s.TotalRows += rows
}

// RecordError logs a failed query execution.
func (t *QueryTracker) RecordError(label string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.getOrCreate(label)
	s.ExecCount++
	s.LastExec = time.Now()
	s.LastErr = err
	s.LastErrTime = time.Now()
	s.ErrCount++
}

// Snapshot returns a copy of all stats, safe for reading without locks.
func (t *QueryTracker) Snapshot() []QueryStat {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]QueryStat, 0, len(t.stats))
	for _, s := range t.stats {
		cp := *s
		out = append(out, cp)
	}
	return out
}

// trackedQuery executes a query via the registry and records timing/row stats.
func trackedQuery(ctx context.Context, t *QueryTracker, label string, reg *cratedb.Registry, stmt string, args ...interface{}) (*cratedb.SQLResponse, error) {
	start := time.Now()
	resp, err := reg.Query(ctx, stmt, args...)
	dur := time.Since(start)
	if err != nil {
		t.RecordError(label, err)
		return nil, err
	}
	t.Record(label, dur, int64(len(resp.Rows)))
	return resp, nil
}

func (t *QueryTracker) getOrCreate(label string) *QueryStat {
	s, ok := t.stats[label]
	if !ok {
		s = &QueryStat{Label: label, Category: "unknown"}
		t.stats[label] = s
	}
	return s
}
