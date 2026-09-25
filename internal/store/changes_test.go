package store

import (
	"testing"
	"time"
)

func TestAddChangesNewestFirst(t *testing.T) {
	s := New(10, nil)
	t0 := time.Unix(1790232600, 0)
	s.AddChanges([]Change{{ID: "b", Ended: t0.Add(2 * time.Second)}, {ID: "a", Ended: t0}}, nil, t0.Add(-time.Hour))
	s.AddChanges([]Change{{ID: "c", Ended: t0.Add(time.Second)}, {ID: "d", Ended: t0.Add(3 * time.Second)}}, []ChangeGap{{Node: "n1"}}, t0)

	snap := s.Snapshot(1, SnapshotHint{IncludeQueries: true}).Changes
	var ids string
	for _, c := range snap.Changes {
		ids += c.ID
	}
	if ids != "dbca" {
		t.Errorf("order = %s, want dbca", ids)
	}
	if !snap.LogStart.Equal(t0.Add(-time.Hour)) {
		t.Errorf("LogStart moved to %v; it's the first poll's", snap.LogStart)
	}
	if len(snap.Gaps) != 1 || !snap.Usable() {
		t.Errorf("gaps=%v usable=%v", snap.Gaps, snap.Usable())
	}
}
