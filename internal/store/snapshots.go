package store

import (
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// SnapshotsState is what the last snapshots poll saw. Repositories 0 with
// UpdatedAt set means the cluster has no repository at all.
type SnapshotsState struct {
	Repositories int
	Snapshots    []cratedb.SnapshotInfo // newest first
	Err          string
	UpdatedAt    time.Time
}

// UpdateSnapshots stores the latest snapshot listing.
func (s *Store) UpdateSnapshots(repos int, snaps []cratedb.SnapshotInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.snapshots = SnapshotsState{Repositories: repos, Snapshots: snaps, UpdatedAt: now}
	s.lastUpdated["snapshots"] = now
	s.alerts.sync("snapshots", now, snapshotAlerts(snaps))
}

// SetSnapshotsError keeps the last listing and records why the poll failed.
func (s *Store) SetSnapshotsError(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots.Err = msg
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
