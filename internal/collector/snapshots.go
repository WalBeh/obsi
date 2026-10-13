package collector

import (
	"context"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// SnapshotsTick is how often the repositories and running restores are
// read. Both are cluster state and cheap.
const SnapshotsTick = time.Minute

// sys.snapshots reads every repository (S3, Azure, ...) on each query: the
// repository index plus two blobs per snapshot, whatever the WHERE or
// LIMIT. So it's read every configured interval, and sooner only when it
// can have changed: a repository came, went or changed, a snapshot was
// running, or someone asked (R).
const (
	repositoriesQuery = `SELECT name, type, settings FROM sys.repositories`
	restoresQuery     = `SELECT repository, name, state, shards['state'] FROM sys.snapshot_restore`
	snapshotsQuery    = `SELECT repository, name, state, started, finished, version, reason, total_shards,
  include_global_state, tables, table_partitions['table_schema'], table_partitions['table_name'],
  table_partitions['values'], failures
FROM sys.snapshots`
)

type SnapshotsCollector struct {
	interval time.Duration // between sys.snapshots reads
	tracker  *QueryTracker

	mu       sync.Mutex // Collect runs from the poll loop and from R
	listedAt time.Time
	repoKey  string
	running  bool // the last listing had a snapshot IN_PROGRESS
	force    atomic.Bool
	reset    atomic.Bool // set by Reset, which mustn't wait for a slow listing
}

func NewSnapshotsCollector(cfg config.CollectorConfig, tracker *QueryTracker) *SnapshotsCollector {
	return &SnapshotsCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *SnapshotsCollector) Name() string            { return "snapshots" }
func (c *SnapshotsCollector) Interval() time.Duration { return min(c.interval, SnapshotsTick) }

// Force makes the next Collect read sys.snapshots.
func (c *SnapshotsCollector) Force() { c.force.Store(true) }

func (c *SnapshotsCollector) Reset() { c.reset.Store(true) }

func (c *SnapshotsCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reset.Swap(false) {
		c.listedAt, c.repoKey, c.running = time.Time{}, "", false
	}
	resp, err := trackedQuery(ctx, c.tracker, QuerySnapshotRepos, reg, repositoriesQuery)
	if err != nil {
		st.SetSnapshotsError(false, err.Error())
		return err
	}
	repos := parseRepositories(resp.Rows)
	var restores []cratedb.SnapshotRestore
	if resp, err := trackedQuery(ctx, c.tracker, QuerySnapshotRestores, reg, restoresQuery); err == nil {
		restores = parseRestores(resp.Rows)
	}
	st.UpdateRepositories(repos, restores, c.interval)

	key := repositoriesKey(repos)
	force := c.force.Swap(false)
	if len(repos) == 0 {
		c.listedAt, c.repoKey, c.running = time.Now(), key, false
		return nil
	}
	if !force && !c.running && key == c.repoKey && time.Since(c.listedAt) < c.interval {
		return nil
	}
	start := time.Now()
	resp, err = trackedQuery(ctx, c.tracker, QuerySnapshots, reg, snapshotsQuery)
	// A failed read waits for the interval too: it is the expensive one,
	// e.g. timing out on a slow bucket. While a snapshot runs it's retried.
	c.listedAt, c.repoKey = time.Now(), key
	if err != nil {
		st.SetSnapshotsError(true, err.Error())
		return err
	}
	snaps := parseSnapshots(resp.Rows)
	st.UpdateSnapshots(snaps, time.Since(start))
	c.running = slices.ContainsFunc(snaps, func(s cratedb.SnapshotInfo) bool { return s.State == "IN_PROGRESS" })
	return nil
}

// repositoriesKey changes when a repository is added, dropped or altered.
func repositoriesKey(repos []cratedb.Repository) string {
	var b strings.Builder
	for _, r := range repos {
		b.WriteString(r.Name + "\x00" + r.Type)
		for _, k := range slices.Sorted(maps.Keys(r.Settings)) {
			b.WriteString("\x00" + k + "=" + r.Settings[k])
		}
		b.WriteString("\x01")
	}
	return b.String()
}

func parseRepositories(rows [][]interface{}) []cratedb.Repository {
	out := make([]cratedb.Repository, 0, len(rows))
	for _, row := range rows {
		r := cratedb.Repository{Name: cratedb.ToString(row[0]), Type: cratedb.ToString(row[1]), Settings: map[string]string{}}
		flattenSettings("", row[2], r.Settings)
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func parseRestores(rows [][]interface{}) []cratedb.SnapshotRestore {
	out := make([]cratedb.SnapshotRestore, 0, len(rows))
	for _, row := range rows {
		r := cratedb.SnapshotRestore{Repository: cratedb.ToString(row[0]), Name: cratedb.ToString(row[1]), State: cratedb.ToString(row[2])}
		for _, s := range textArray(row[3]) {
			r.Shards++
			switch s {
			case "SUCCESS":
				r.Done++
			case "FAILURE":
				r.Failed++
			}
		}
		out = append(out, r)
	}
	return out
}

func parseSnapshots(rows [][]interface{}) []cratedb.SnapshotInfo {
	out := make([]cratedb.SnapshotInfo, 0, len(rows))
	for _, row := range rows {
		s := cratedb.SnapshotInfo{
			Repository:  cratedb.ToString(row[0]),
			Name:        cratedb.ToString(row[1]),
			State:       cratedb.ToString(row[2]),
			Started:     msTime(row[3]),
			Finished:    msTime(row[4]),
			Version:     cratedb.ToString(row[5]),
			Reason:      cratedb.ToString(row[6]),
			TotalShards: int(cratedb.ToInt64(row[7])),
			GlobalState: cratedb.ToBool(row[8]),
			Tables:      textArray(row[9]),
			Failures:    textArray(row[13]),
		}
		schemas, names := textArray(row[10]), textArray(row[11])
		values, _ := row[12].([]interface{})
		for i := range min(len(schemas), len(names)) {
			p := cratedb.SnapshotPartition{Table: schemas[i] + "." + names[i]}
			if i < len(values) {
				p.Values = textArray(values[i])
			}
			s.Partitions = append(s.Partitions, p)
		}
		out = append(out, s)
	}
	// Newest first; a running one (no Finished) or one CrateDB couldn't
	// read (no Started) by what is known.
	sort.SliceStable(out, func(i, j int) bool { return out[i].Started.After(out[j].Started) })
	return out
}

// textArray reads a text array; NULL entries become "".
func textArray(v interface{}) []string {
	a, _ := v.([]interface{})
	out := make([]string, 0, len(a))
	for _, e := range a {
		out = append(out, cratedb.ToString(e))
	}
	return out
}
