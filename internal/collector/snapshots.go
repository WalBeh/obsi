package collector

import (
	"context"
	"strconv"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// SnapshotsLimit is how many snapshots the Overview lists.
const SnapshotsLimit = 10

// sys.snapshots lists the repository (S3, Azure, ...) on every read, so
// it's slow and polled rarely; sys.repositories is cluster state and cheap.
const (
	repositoriesQuery = `SELECT count(*) FROM sys.repositories`
	snapshotsQuery    = `SELECT repository, name, state, started, finished, array_length(failures, 1)
FROM sys.snapshots
ORDER BY started DESC
LIMIT `
)

type SnapshotsCollector struct {
	interval time.Duration
	tracker  *QueryTracker
}

func NewSnapshotsCollector(cfg config.CollectorConfig, tracker *QueryTracker) *SnapshotsCollector {
	return &SnapshotsCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *SnapshotsCollector) Name() string            { return "snapshots" }
func (c *SnapshotsCollector) Interval() time.Duration { return c.interval }

func (c *SnapshotsCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	resp, err := trackedQuery(ctx, c.tracker, QuerySnapshotRepos, reg, repositoriesQuery)
	if err != nil {
		st.SetSnapshotsError(err.Error())
		return err
	}
	repos := int(cratedb.ToInt64(resp.Rows[0][0]))
	if repos == 0 {
		st.UpdateSnapshots(0, nil)
		return nil
	}
	resp, err = trackedQuery(ctx, c.tracker, QuerySnapshots, reg, snapshotsQuery+strconv.Itoa(SnapshotsLimit))
	if err != nil {
		st.SetSnapshotsError(err.Error())
		return err
	}
	st.UpdateSnapshots(repos, parseSnapshots(resp.Rows))
	return nil
}

func parseSnapshots(rows [][]interface{}) []cratedb.SnapshotInfo {
	out := make([]cratedb.SnapshotInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, cratedb.SnapshotInfo{
			Repository: cratedb.ToString(row[0]),
			Name:       cratedb.ToString(row[1]),
			State:      cratedb.ToString(row[2]),
			Started:    msTime(row[3]),
			Finished:   msTime(row[4]),
			Failures:   int(cratedb.ToInt64(row[5])),
		})
	}
	return out
}
