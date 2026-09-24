package collector

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// JobsLogCollector reads sys.jobs_log for the slowest board. It only queries
// while the board is open (SetView), since every poll fans out to all nodes.
type JobsLogCollector struct {
	interval time.Duration
	tracker  *QueryTracker
	active   atomic.Bool
	mode     atomic.Int32

	// running/dirty collapse overlapping triggers (key mashing f/g) into at
	// most one follow-up poll instead of one query per keypress.
	running atomic.Bool
	dirty   atomic.Bool
}

func NewJobsLogCollector(cfg config.CollectorConfig, tracker *QueryTracker) *JobsLogCollector {
	return &JobsLogCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *JobsLogCollector) Name() string            { return "jobs_log" }
func (c *JobsLogCollector) Interval() time.Duration { return c.interval }

// SetView records whether the board is open and in which mode. Returns true
// when either changed.
func (c *JobsLogCollector) SetView(active bool, mode store.JobsLogMode) bool {
	wasActive := c.active.Swap(active)
	oldMode := c.mode.Swap(int32(mode))
	return wasActive != active || store.JobsLogMode(oldMode) != mode
}

// jobsLogDuration is ended - started in ms. Plain bigint so max/avg work.
const jobsLogDuration = `(CAST(ended AS BIGINT) - CAST(started AS BIGINT))`

// Args: observed-since (epoch ms), stuck threshold (ms). Jobs over the stuck
// threshold (closed WITH HOLD cursors) are left out like everywhere else,
// and so is obsi's own tagged polling.
const jobsLogWhere = `ended >= ? AND ` + jobsLogDuration + ` <= ? AND stmt NOT LIKE ` + cratedb.QueryTagLike

var (
	jobsLogLimit = strconv.Itoa(store.SlowestLimit)

	jobsLogSlowestQuery = `SELECT id, node['name'], username, stmt, started, ended, error, classification['type']
FROM sys.jobs_log
WHERE ` + jobsLogWhere + `
ORDER BY ` + jobsLogDuration + ` DESC
LIMIT ` + jobsLogLimit

	jobsLogFailedQuery = `SELECT id, node['name'], username, stmt, started, ended, error, classification['type']
FROM sys.jobs_log
WHERE ` + jobsLogWhere + ` AND error IS NOT NULL
ORDER BY ended DESC
LIMIT ` + jobsLogLimit

	jobsLogGroupedQuery = `SELECT stmt, count(*), count(error), max(` + jobsLogDuration + `), avg(` + jobsLogDuration + `), max(ended)
FROM sys.jobs_log
WHERE ` + jobsLogWhere + `
GROUP BY stmt
ORDER BY max(` + jobsLogDuration + `) DESC
LIMIT ` + jobsLogLimit
)

const jobsLogCoverageQuery = `SELECT node['name'], min(ended), count(*) FROM sys.jobs_log GROUP BY node['name']`

const statsEnabledQuery = `SELECT settings['stats']['enabled'] FROM sys.cluster`

func (c *JobsLogCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	if !c.active.Load() {
		return nil
	}
	if !c.running.CompareAndSwap(false, true) {
		c.dirty.Store(true)
		return nil
	}
	defer c.running.Store(false)
	for {
		c.dirty.Store(false)
		if err := c.collectOnce(ctx, reg, st); err != nil || !c.dirty.Load() || !c.active.Load() {
			return err
		}
	}
}

func (c *JobsLogCollector) collectOnce(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	mode := store.JobsLogMode(c.mode.Load())

	covResp, err := trackedQuery(ctx, c.tracker, QueryJobsLogCoverage, reg, jobsLogCoverageQuery)
	if err != nil {
		st.SetJobsLogError(err.Error())
		return err
	}
	coverage := parseJobLogCoverage(covResp.Rows)
	if len(coverage) == 0 {
		// An empty log is either stats switched off or a cluster that has
		// run nothing yet; only the former is worth reporting.
		if resp, err := trackedQuery(ctx, c.tracker, QueryStatsEnabled, reg, statsEnabledQuery); err == nil &&
			len(resp.Rows) > 0 && resp.Rows[0][0] == false {
			st.SetJobsLogError("stats.enabled = false")
			return nil
		}
	}

	args := []interface{}{st.ObservedSince().UnixMilli(), store.StuckThreshold.Milliseconds()}
	var entries []cratedb.JobLogEntry
	var groups []cratedb.JobLogGroup
	switch mode {
	case store.JobsLogGrouped:
		resp, err := trackedQuery(ctx, c.tracker, QueryJobsLogGroups, reg, jobsLogGroupedQuery, args...)
		if err != nil {
			st.SetJobsLogError(err.Error())
			return err
		}
		groups = parseJobLogGroups(resp.Rows)
	default:
		stmt := jobsLogSlowestQuery
		if mode == store.JobsLogFailed {
			stmt = jobsLogFailedQuery
		}
		resp, err := trackedQuery(ctx, c.tracker, QueryJobsLogEntries, reg, stmt, args...)
		if err != nil {
			st.SetJobsLogError(err.Error())
			return err
		}
		entries = parseJobLogEntries(resp.Rows)
	}
	st.UpdateJobsLog(mode, entries, groups, coverage)
	return nil
}

func msTime(v interface{}) time.Time {
	if ms := cratedb.ToInt64(v); ms > 0 {
		return time.UnixMilli(ms)
	}
	return time.Time{}
}

func parseJobLogEntries(rows [][]interface{}) []cratedb.JobLogEntry {
	out := make([]cratedb.JobLogEntry, 0, len(rows))
	for _, row := range rows {
		out = append(out, cratedb.JobLogEntry{
			ID:       cratedb.ToString(row[0]),
			Node:     cratedb.ToString(row[1]),
			Username: cratedb.ToString(row[2]),
			Stmt:     cratedb.ToString(row[3]),
			Started:  msTime(row[4]),
			Ended:    msTime(row[5]),
			Error:    cratedb.ToString(row[6]),
			Type:     cratedb.ToString(row[7]),
		})
	}
	return out
}

func parseJobLogGroups(rows [][]interface{}) []cratedb.JobLogGroup {
	out := make([]cratedb.JobLogGroup, 0, len(rows))
	for _, row := range rows {
		out = append(out, cratedb.JobLogGroup{
			Stmt:      cratedb.ToString(row[0]),
			Count:     cratedb.ToInt64(row[1]),
			Failed:    cratedb.ToInt64(row[2]),
			Max:       time.Duration(cratedb.ToInt64(row[3])) * time.Millisecond,
			Avg:       time.Duration(cratedb.ToFloat64(row[4]) * float64(time.Millisecond)),
			LastEnded: msTime(row[5]),
		})
	}
	return out
}

func parseJobLogCoverage(rows [][]interface{}) []cratedb.JobLogCoverage {
	out := make([]cratedb.JobLogCoverage, 0, len(rows))
	for _, row := range rows {
		out = append(out, cratedb.JobLogCoverage{
			Node:    cratedb.ToString(row[0]),
			Oldest:  msTime(row[1]),
			Entries: cratedb.ToInt64(row[2]),
		})
	}
	return out
}
