package collector

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// ChangesCollector records configuration changes from sys.jobs_log while
// obsi runs, plus what the log still holds from before. The log is a ring
// per node, so it's polled on a short interval and gaps are reported; the
// cluster settings are read on every poll for old → new values.
type ChangesCollector struct {
	interval time.Duration
	tracker  *QueryTracker

	// Collect runs from the ticker and from TriggerCollector.
	mu       sync.Mutex
	polled   bool
	since    time.Time
	seen     map[string]time.Time
	newest   map[string]time.Time // per node, max(ended) on the last poll
	cluster  map[string]string
	tables   map[string]map[string]string
	unpaired []pendingDiff
}

// pendingDiff is a cluster setting change without its statement. The
// statement can land in the log just after the log read, so it gets one
// more poll before it's reported as unseen.
type pendingDiff struct {
	diff store.SettingDiff
	at   time.Time
}

func NewChangesCollector(cfg config.CollectorConfig, tracker *QueryTracker) *ChangesCollector {
	return &ChangesCollector{
		interval: cfg.Interval.Duration,
		tracker:  tracker,
		seen:     make(map[string]time.Time),
		newest:   make(map[string]time.Time),
	}
}

func (c *ChangesCollector) Name() string            { return "changes" }
func (c *ChangesCollector) Interval() time.Duration { return c.interval }

// Overlap re-reads the tail of the last poll: jobs on different nodes (and
// clocks) don't end in log order. Rows are deduplicated by id.
const changesOverlap = 5 * time.Second

const changesPage = 500

const changesCoverageQuery = `SELECT node['name'], min(ended), max(ended) FROM sys.jobs_log GROUP BY node['name']`

// Statements that fail before analysis (unknown table, typo) are classified
// UNDEFINED; kept so failed attempts show up.
var changesLogQuery = `SELECT id, node['name'], username, stmt, ended, error
FROM sys.jobs_log
WHERE classification['type'] IN ('DDL', 'MANAGEMENT', 'UNDEFINED') AND ended >= ?
ORDER BY ended
LIMIT ` + strconv.Itoa(changesPage)

const changesClusterQuery = `SELECT settings FROM sys.cluster`

const changesTablesQuery = `SELECT table_schema, table_name, closed, number_of_replicas, settings
FROM information_schema.tables
WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('sys', 'information_schema', 'pg_catalog')`

func (c *ChangesCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	covResp, err := trackedQuery(ctx, c.tracker, QueryChangesCoverage, reg, changesCoverageQuery)
	if err != nil {
		st.SetChangesError(err.Error())
		return err
	}
	if len(covResp.Rows) == 0 {
		if resp, err := trackedQuery(ctx, c.tracker, QueryStatsEnabled, reg, statsEnabledQuery); err == nil &&
			len(resp.Rows) > 0 && resp.Rows[0][0] == false {
			st.SetChangesError("stats.enabled = false")
			return nil
		}
	}
	gaps, logStart, floor := c.coverage(covResp.Rows)

	var changes []*store.Change
	for {
		resp, err := trackedQuery(ctx, c.tracker, QueryChangesLog, reg, changesLogQuery, c.since.UnixMilli())
		if err != nil {
			st.SetChangesError(err.Error())
			return err
		}
		last := c.since
		for _, row := range resp.Rows {
			ended := msTime(row[4])
			last = ended
			id := cratedb.ToString(row[0])
			if _, dup := c.seen[id]; dup {
				continue
			}
			c.seen[id] = ended
			if ch := parseChange(row); ch != nil {
				changes = append(changes, ch)
			}
		}
		if len(resp.Rows) < changesPage || !last.After(c.since) {
			break
		}
		c.since = last
	}
	if f := floor.Add(-changesOverlap); f.After(c.since) {
		c.since = f
	}
	for id, ended := range c.seen {
		if ended.Before(c.since) {
			delete(c.seen, id)
		}
	}

	resp, err := trackedQuery(ctx, c.tracker, QueryChangesCluster, reg, changesClusterQuery)
	if err != nil {
		st.SetChangesError(err.Error())
		return err
	}
	if len(resp.Rows) > 0 {
		cur := make(map[string]string)
		flattenSettings("", resp.Rows[0][0], cur)
		if c.cluster != nil {
			changes = c.pairClusterDiffs(changes, diffSettings(c.cluster, cur), time.Now())
		}
		c.cluster = cur
	}

	if err := c.tableDiffs(ctx, reg, changes); err != nil {
		st.SetChangesError(err.Error())
		return err
	}
	c.polled = true

	out := make([]store.Change, len(changes))
	for i, ch := range changes {
		out[i] = *ch
	}
	st.AddChanges(out, gaps, logStart)
	return nil
}

// coverage compares each node's oldest log entry against its newest one on
// the previous poll: if the oldest is newer, entries between them rotated
// out unread. floor is the oldest per-node newest entry, the next poll's
// lower bound.
func (c *ChangesCollector) coverage(rows [][]interface{}) (gaps []store.ChangeGap, logStart, floor time.Time) {
	for _, row := range rows {
		node, oldest, newest := cratedb.ToString(row[0]), msTime(row[1]), msTime(row[2])
		if prev, ok := c.newest[node]; ok && oldest.After(prev) {
			gaps = append(gaps, store.ChangeGap{Node: node, From: prev, To: oldest})
		}
		c.newest[node] = newest
		if oldest.After(logStart) {
			logStart = oldest
		}
		if floor.IsZero() || newest.Before(floor) {
			floor = newest
		}
	}
	return gaps, logStart, floor
}

func parseChange(row []interface{}) *store.Change {
	raw := cratedb.ToString(row[3])
	kind := cratedb.ClassifyChange(raw)
	if kind == "" {
		return nil
	}
	stmt := cratedb.Redact(raw)
	obsi := strings.HasSuffix(stmt, cratedb.ChangeTag)
	return &store.Change{
		ID:       cratedb.ToString(row[0]),
		Node:     cratedb.ToString(row[1]),
		Username: cratedb.ToString(row[2]),
		Stmt:     strings.TrimSuffix(stmt, cratedb.ChangeTag),
		Ended:    msTime(row[4]),
		Error:    cratedb.ToString(row[5]),
		Kind:     kind,
		Obsi:     obsi,
	}
}

// pairClusterDiffs hangs each changed setting on the newest SET/RESET GLOBAL
// naming it (or a prefix of it). Diffs left over from the previous poll
// that still have no statement become unseen changes.
func (c *ChangesCollector) pairClusterDiffs(changes []*store.Change, diffs []store.SettingDiff, now time.Time) []*store.Change {
	pending := c.unpaired
	c.unpaired = nil
	for _, d := range diffs {
		pending = append(pending, pendingDiff{d, now})
	}
	for _, p := range pending {
		if ch := newestNaming(changes, p.diff.Key); ch != nil {
			ch.Diffs = append(ch.Diffs, p.diff)
			continue
		}
		if p.at.Before(now) {
			changes = append(changes, &store.Change{
				Kind: cratedb.ChangeCluster, Ended: p.at, Unseen: true,
				Diffs: []store.SettingDiff{p.diff},
			})
			continue
		}
		c.unpaired = append(c.unpaired, p)
	}
	return changes
}

func newestNaming(changes []*store.Change, key string) *store.Change {
	var best *store.Change
	for _, ch := range changes {
		if ch.Kind != cratedb.ChangeCluster || ch.Error != "" || ch.Unseen {
			continue
		}
		if best != nil && !ch.Ended.After(best.Ended) {
			continue
		}
		for _, n := range cratedb.SettingNames(ch.Stmt) {
			if n == key || strings.HasPrefix(key, n+".") {
				best = ch
				break
			}
		}
	}
	return best
}

// tableDiffs takes a baseline of every table's settings on the first poll,
// then re-reads the tables named by new ALTER TABLE statements. Partition
// statements and tables without a baseline get no diff.
func (c *ChangesCollector) tableDiffs(ctx context.Context, reg *cratedb.Registry, changes []*store.Change) error {
	if !c.polled {
		resp, err := trackedQuery(ctx, c.tracker, QueryChangesTables, reg, changesTablesQuery)
		if err != nil {
			return err
		}
		c.tables = parseTableSettings(resp.Rows)
		return nil
	}
	newest := make(map[string]*store.Change)
	for _, ch := range changes {
		if ch.Kind != cratedb.ChangeTable || ch.Error != "" {
			continue
		}
		schema, table, partition, ok := cratedb.AlteredTable(ch.Stmt)
		if !ok || partition {
			continue
		}
		if schema == "" {
			schema = "doc"
		}
		key := schema + "." + table
		if prev := newest[key]; prev == nil || ch.Ended.After(prev.Ended) {
			newest[key] = ch
		}
	}
	for key, ch := range newest {
		schema, table, _ := strings.Cut(key, ".")
		resp, err := trackedQuery(ctx, c.tracker, QueryChangesTables, reg,
			changesTablesQuery+` AND table_schema = ? AND table_name = ?`, schema, table)
		if err != nil {
			return err
		}
		cur := parseTableSettings(resp.Rows)[key]
		if old, ok := c.tables[key]; ok && cur != nil {
			ch.Diffs = diffSettings(old, cur)
		}
		c.tables[key] = cur
	}
	return nil
}

func parseTableSettings(rows [][]interface{}) map[string]map[string]string {
	out := make(map[string]map[string]string, len(rows))
	for _, row := range rows {
		s := map[string]string{
			"closed":             strconv.FormatBool(cratedb.ToBool(row[2])),
			"number_of_replicas": cratedb.ToString(row[3]),
		}
		flattenSettings("", row[4], s)
		out[cratedb.ToString(row[0])+"."+cratedb.ToString(row[1])] = s
	}
	return out
}

// flattenSettings turns a settings object into dotted keys. Empty objects
// (an unset allocation filter) produce no key.
func flattenSettings(prefix string, v interface{}, out map[string]string) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, sub := range t {
			if prefix != "" {
				k = prefix + "." + k
			}
			flattenSettings(k, sub, out)
		}
	case string:
		out[prefix] = t
	case float64:
		out[prefix] = strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		out[prefix] = strconv.FormatBool(t)
	case nil:
		out[prefix] = "null"
	default:
		b, _ := json.Marshal(t)
		out[prefix] = string(b)
	}
}

func diffSettings(old, cur map[string]string) []store.SettingDiff {
	var diffs []store.SettingDiff
	for k, v := range cur {
		if old[k] != v {
			diffs = append(diffs, store.SettingDiff{Key: k, Old: old[k], New: v})
		}
	}
	for k, v := range old {
		if _, ok := cur[k]; !ok {
			diffs = append(diffs, store.SettingDiff{Key: k, Old: v})
		}
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].Key < diffs[j].Key })
	return diffs
}
