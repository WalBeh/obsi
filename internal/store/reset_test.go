package store

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func TestResetKeepsOnlyAlertHistory(t *testing.T) {
	s := New(10, nil)
	s.UpdateActiveQueries([]cratedb.ActiveQuery{{ID: "j1", Started: time.Now()}})
	s.AddChanges([]Change{{ID: "c1", Ended: time.Now()}}, nil, time.Now())
	s.ObserveConnection(false)
	s.ObserveCluster(cratedb.RegistryStatus{ClusterName: "a",
		Foreign: &cratedb.ClusterIdentity{ID: "b-id", Name: "b"}})

	all := SnapshotHint{IncludeQueries: true, IncludeNodes: true, IncludeShards: true, IncludeCluster: true, IncludeHealth: true, IncludeTables: true}
	if a := s.Snapshot(1, all).Alerts; a.Firing != 2 || !strings.Contains(a.History[0].Message, "different cluster behind the endpoint: b (b-id), was a ()") {
		t.Fatalf("before reset: %+v", a)
	}

	s.Reset("a", "b")
	snap := s.Snapshot(1, all)
	if len(snap.ActiveQueries) != 0 || len(snap.Changes.Changes) != 0 || len(snap.SlowestQueries) != 0 {
		t.Errorf("old cluster's data survived: %d queries, %d changes", len(snap.ActiveQueries), len(snap.Changes.Changes))
	}
	a := snap.Alerts
	if a.Firing != 0 || len(a.History) != 3 || a.History[0].Message != "switched to cluster b (was a)" {
		t.Errorf("alerts after reset: %+v", a)
	}
	for _, h := range a.History {
		if h.Firing() {
			t.Errorf("still firing after reset: %s", h.Message)
		}
	}
}
