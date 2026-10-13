package store

import (
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// SnapshotsState is what the snapshot polls saw. Repositories and
// restores are cluster state, read every poll; the snapshot listing reads
// the repositories themselves and is refreshed less often (ListedAt).
type SnapshotsState struct {
	Repositories []cratedb.Repository
	Restores     []cratedb.SnapshotRestore
	Snapshots    []cratedb.SnapshotInfo // newest first
	ListedAt     time.Time              // last sys.snapshots read
	ListTook     time.Duration
	ListEvery    time.Duration // configured time between sys.snapshots reads
	Err          string        // the last repositories poll failed
	ListErr      string        // the last sys.snapshots read failed
	UpdatedAt    time.Time
}

// UpdateRepositories stores the repositories and running restores.
func (s *Store) UpdateRepositories(repos []cratedb.Repository, restores []cratedb.SnapshotRestore, listEvery time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.snapshots.Repositories, s.snapshots.Restores = repos, restores
	s.snapshots.Err, s.snapshots.UpdatedAt, s.snapshots.ListEvery = "", now, listEvery
	if len(repos) == 0 {
		s.snapshots.Snapshots, s.snapshots.ListErr = nil, ""
		s.alerts.sync("snapshots", now, nil)
	}
	s.lastUpdated["snapshots"] = now
}

// UpdateSnapshots stores the latest snapshot listing.
func (s *Store) UpdateSnapshots(snaps []cratedb.SnapshotInfo, took time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.snapshots.Snapshots, s.snapshots.ListedAt, s.snapshots.ListTook, s.snapshots.ListErr = snaps, now, took, ""
	s.alerts.sync("snapshots", now, snapshotAlerts(snaps))
}

// SetSnapshotsError keeps the last data and records why a poll failed:
// the repositories poll, or with list the sys.snapshots read.
func (s *Store) SetSnapshotsError(list bool, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if list {
		s.snapshots.ListErr = msg
	} else {
		s.snapshots.Err = msg
	}
}

func (st SnapshotsState) copy() SnapshotsState {
	st.Repositories = copySlice(st.Repositories)
	st.Restores = copySlice(st.Restores)
	st.Snapshots = copySlice(st.Snapshots)
	return st
}

// snapshotAlerts raises when the newest finished snapshot of a repository
// failed or is partial; a later good one clears it.
func snapshotAlerts(snaps []cratedb.SnapshotInfo) []Alert {
	var out []Alert
	seen := map[string]bool{}
	for _, sn := range snaps {
		if sn.State == "IN_PROGRESS" || seen[sn.Repository] {
			continue
		}
		seen[sn.Repository] = true
		switch sn.State {
		case "FAILED":
			out = append(out, Alert{Key: sn.Repository, Level: AlertCrit, Message: "snapshot " + sn.Repository + "/" + sn.Name + " FAILED"})
		case "PARTIAL":
			out = append(out, Alert{Key: sn.Repository, Level: AlertWarn, Message: "snapshot " + sn.Repository + "/" + sn.Name + " PARTIAL"})
		}
	}
	sortAlerts(out)
	return out
}
