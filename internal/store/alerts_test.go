package store

import (
	"strings"
	"testing"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func firing(st AlertsState) []string {
	var out []string
	for _, a := range st.History {
		if a.Firing() {
			out = append(out, a.Message)
		}
	}
	return out
}

// A table that stays YELLOW raises once; turning RED replaces it; GREEN clears.
func TestHealthAlertsTransitions(t *testing.T) {
	s := New(10, nil)
	yellow := []cratedb.TableHealth{
		{TableSchema: "doc", TableName: "t", Health: "YELLOW", UnderReplicated: 2, Partition: "p1"},
		{TableSchema: "doc", TableName: "t", Health: "YELLOW", UnderReplicated: 1, Partition: "p2"},
		{TableSchema: "doc", TableName: "ok", Health: "GREEN"},
	}
	s.UpdateClusterHealth(nil, yellow)
	s.UpdateClusterHealth(nil, yellow)
	st := s.Snapshot(1, SnapshotHint{}).Alerts
	if st.Raised != 1 || st.Firing != 1 {
		t.Fatalf("raised=%d firing=%d, want 1/1", st.Raised, st.Firing)
	}
	if got := st.History[0].Message; got != "table doc.t YELLOW (2 partitions), 3 underreplicated shards" {
		t.Errorf("message = %q", got)
	}

	s.UpdateClusterHealth(nil, []cratedb.TableHealth{{TableSchema: "doc", TableName: "t", Health: "RED", MissingShards: 1}})
	st = s.Snapshot(1, SnapshotHint{}).Alerts
	if st.Raised != 2 || st.Firing != 1 || st.History[0].Level != AlertCrit || st.History[1].Firing() {
		t.Fatalf("RED did not replace YELLOW: %+v", st)
	}

	s.UpdateClusterHealth([]cratedb.ClusterCheck{{ID: 3, Severity: 3, Description: "bad", Passed: false}}, nil)
	st = s.Snapshot(1, SnapshotHint{}).Alerts
	if got := firing(st); len(got) != 1 || got[0] != "check failed: bad" {
		t.Errorf("firing = %v, want only the failing check", got)
	}
}

func TestNodeAlerts(t *testing.T) {
	s := New(10, nil)
	s.UpdateClusterSettings(cratedb.ClusterSettings{DiskWatermarkLow: "85%", DiskWatermarkHigh: "90%", DiskWatermarkFlood: "95%"})
	node := func(id string, heapPct, diskPct int64) NodeSnapshot {
		return NodeSnapshot{NodeInfo: cratedb.NodeInfo{ID: id, Name: id, HeapUsed: heapPct, HeapMax: 100, FSUsed: diskPct, FSTotal: 100}}
	}

	s.UpdateNodes([]NodeSnapshot{node("a", 90, 50), node("b", 10, 92)})
	got := strings.Join(firing(s.Snapshot(1, SnapshotHint{}).Alerts), "|")
	if !strings.Contains(got, "node a heap above 85%") || !strings.Contains(got, "node b disk past high watermark (90%)") {
		t.Fatalf("firing = %s", got)
	}

	// Heap at 82% keeps the alert (clears below 80%); b leaves.
	s.UpdateNodes([]NodeSnapshot{node("a", 82, 50)})
	got = strings.Join(firing(s.Snapshot(1, SnapshotHint{}).Alerts), "|")
	if !strings.Contains(got, "heap") || !strings.Contains(got, "node b left the cluster") || strings.Contains(got, "disk") {
		t.Fatalf("firing = %s", got)
	}

	// The store forgetting b must not clear its alert; b coming back does.
	s.mu.Lock()
	delete(s.knownNodes, "b")
	s.mu.Unlock()
	s.UpdateNodes([]NodeSnapshot{node("a", 70, 50)})
	if got := firing(s.Snapshot(1, SnapshotHint{}).Alerts); len(got) != 1 || got[0] != "node b left the cluster" {
		t.Fatalf("firing = %v, want only b gone", got)
	}
	s.UpdateNodes([]NodeSnapshot{node("a", 70, 50), node("b", 10, 50)})
	if st := s.Snapshot(1, SnapshotHint{}).Alerts; st.Firing != 0 {
		t.Fatalf("firing = %v, want none", firing(st))
	}
}

func TestConnectionAlert(t *testing.T) {
	s := New(10, nil)
	s.ObserveConnection(true)
	s.ObserveConnection(false)
	s.ObserveConnection(false)
	s.ObserveConnection(true)
	st := s.Snapshot(1, SnapshotHint{}).Alerts
	if st.Raised != 1 || st.Firing != 0 || st.History[0].Cleared.IsZero() {
		t.Fatalf("state = %+v", st)
	}
}

// The newest finished snapshot per repository decides; IN_PROGRESS is skipped.
func TestSnapshotAlerts(t *testing.T) {
	s := New(10, nil)
	s.UpdateSnapshots(2, []cratedb.SnapshotInfo{
		{Repository: "a", Name: "a3", State: "IN_PROGRESS"},
		{Repository: "a", Name: "a2", State: "FAILED"},
		{Repository: "b", Name: "b2", State: "SUCCESS"},
		{Repository: "b", Name: "b1", State: "FAILED"},
	})
	if got := firing(s.Snapshot(1, SnapshotHint{}).Alerts); len(got) != 1 || got[0] != "snapshot a/a2 FAILED" {
		t.Fatalf("firing = %v", got)
	}
	s.UpdateSnapshots(2, []cratedb.SnapshotInfo{{Repository: "a", Name: "a3", State: "SUCCESS"}})
	if st := s.Snapshot(1, SnapshotHint{}).Alerts; st.Firing != 0 {
		t.Errorf("firing = %v, want none after a good snapshot", firing(st))
	}
}
