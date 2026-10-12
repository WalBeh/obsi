package store

import (
	"slices"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func TestReplicationRateAndCopy(t *testing.T) {
	s := New(10, nil)
	subs := func(state string) []cratedb.Subscription {
		return []cratedb.Subscription{{Name: "s", Tables: []cratedb.SubscribedTable{{Schema: "doc", Table: "a", State: state}}}}
	}
	s.UpdateReplication(subs("d"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 1}}, nil)
	r := s.Snapshot(1, SnapshotHint{}).Replication.Replicas["doc.a"]
	if r.CopySince.IsZero() || r.Rate != -1 {
		t.Fatalf("copying: %+v", r)
	}
	since := r.CopySince

	// Copy done: no rate yet, the shard count changed under it.
	s.UpdateReplication(subs("r"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 2, SeqNo: 100}}, nil)
	r = s.Snapshot(1, SnapshotHint{}).Replication.Replicas["doc.a"]
	if !r.CopySince.IsZero() || r.Rate != -1 || !r.LastChange.IsZero() {
		t.Fatalf("after copy: %+v (copy since %s)", r, since)
	}

	s.mu.Lock()
	s.replication.UpdatedAt = s.replication.UpdatedAt.Add(-10 * time.Second)
	s.mu.Unlock()
	s.UpdateReplication(subs("r"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 2, SeqNo: 150}}, nil)
	r = s.Snapshot(1, SnapshotHint{}).Replication.Replicas["doc.a"]
	if r.Rate < 4.9 || r.Rate > 5.1 || r.LastChange.IsZero() {
		t.Fatalf("rate: %+v", r)
	}
	last := r.LastChange

	// A dropped partition lowers SeqNo: no rate, last change kept.
	s.UpdateReplication(subs("r"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 2, SeqNo: 120}}, nil)
	r = s.Snapshot(1, SnapshotHint{}).Replication.Replicas["doc.a"]
	if r.Rate != -1 || !r.LastChange.Equal(last) {
		t.Fatalf("after drop: %+v", r)
	}
}

func TestReplicationAlerts(t *testing.T) {
	s := New(10, nil)
	failed := []cratedb.Subscription{{Name: "s", Tables: []cratedb.SubscribedTable{
		{Schema: "doc", Table: "a", State: "r"},
		{Schema: "doc", Table: "b", State: "e", Reason: "Relation already exists"},
	}}}
	now := time.Now()
	holders := []LeaseHolder{
		{Subscriber: "fresh", LastSeen: now, Expires: now.Add(2 * time.Minute), Period: 2 * time.Minute},
		{Subscriber: "gone", LastSeen: now.Add(-90 * time.Second), Expires: now.Add(30 * time.Second), Period: 2 * time.Minute},
	}
	s.UpdateReplication(failed, nil, nil, holders)
	got := firing(s.Snapshot(1, SnapshotHint{}).Alerts)
	want := []string{
		"replication of doc.b failed (subscription s): Relation already exists",
		"subscriber gone hasn't pulled for half its retention lease period (2m0s)",
	}
	if len(got) != len(want) || !slices.Contains(got, want[0]) || !slices.Contains(got, want[1]) {
		t.Fatalf("firing = %q", got)
	}
	s.UpdateReplication(nil, nil, nil, holders[:1])
	if st := s.Snapshot(1, SnapshotHint{}).Alerts; st.Firing != 0 {
		t.Errorf("still firing: %q", firing(st))
	}
}

func TestReplicationThroughput(t *testing.T) {
	s := New(4, nil)
	subs := func(state string) []cratedb.Subscription {
		return []cratedb.Subscription{{Name: "s", Tables: []cratedb.SubscribedTable{{Schema: "doc", Table: "a", State: state}}}}
	}
	back := func(d time.Duration) {
		s.mu.Lock()
		s.replication.UpdatedAt = s.replication.UpdatedAt.Add(-d)
		s.mu.Unlock()
	}
	holder := func(retained, behind int64, known int) []LeaseHolder {
		return []LeaseHolder{{Subscriber: "dr", On: []string{"doc.a"}, Retained: retained, Known: known, Behind: behind}}
	}

	// Seen initializing, then copying, then in sync with 200MB copied.
	s.UpdateReplication(subs("i"), nil, nil, holder(0, 0, 0))
	s.UpdateReplication(subs("d"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 1}}, holder(0, 0, 0))
	s.UpdateReplication(subs("r"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 2, SeqNo: 100,
		CopiedBytes: 200 << 20, CopyShardMs: 20000}}, holder(100, 0, 2))
	r := s.Snapshot(1, SnapshotHint{}).Replication.Replicas["doc.a"]
	if r.Copy == nil || r.Copy.Bytes != 200<<20 || !r.Copy.FromStart || r.Copy.ShardBps != 10<<20 {
		t.Fatalf("copy = %+v", r.Copy)
	}

	back(10 * time.Second)
	s.UpdateReplication(subs("r"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 2, SeqNo: 150, CopiedBytes: 200 << 20}}, holder(150, 20, 2))
	st := s.Snapshot(1, SnapshotHint{}).Replication
	r, h := st.Replicas["doc.a"], st.Subscribers[0]
	if r.Replicated != 50 || r.Copy == nil {
		t.Errorf("replicated = %d, copy %+v", r.Replicated, r.Copy)
	}
	if h.Pulled != 50 || h.PulledRate < 4.9 || h.PulledRate > 5.1 {
		t.Errorf("pulled = %d at %.1f/s", h.Pulled, h.PulledRate)
	}
	if len(h.History) != 4 || h.History[3] != 20 || len(r.History) != 4 {
		t.Errorf("history: behind %v, rate %v", h.History, r.History)
	}

	// A lease leaving 0 isn't pulling.
	back(10 * time.Second)
	s.UpdateReplication(subs("r"), nil, map[string]ReplicaShards{"doc.a": {Shards: 2, Started: 2, SeqNo: 150}}, holder(5000, 0, 3))
	if h := s.Snapshot(1, SnapshotHint{}).Replication.Subscribers[0]; h.PulledRate != -1 || h.Pulled != 50 {
		t.Errorf("after a lease left 0: %.1f/s, %d", h.PulledRate, h.Pulled)
	}

	// Gone tables and subscribers lose their series.
	s.UpdateReplication(nil, nil, nil, nil)
	s.mu.Lock()
	n := len(s.replSeries)
	s.mu.Unlock()
	if n != 0 {
		t.Errorf("%d series kept", n)
	}
}
