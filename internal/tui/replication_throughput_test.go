package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestSparkline0(t *testing.T) {
	// 6 samples into 3 characters: the max of each pair, scaled from 0.
	if got := sparkline0([]float64{0, 8, 1, 2, 4, 4}, 3); got != "█▂▄" {
		t.Errorf("got %q", got)
	}
	if got := sparkline0([]float64{5, 5}, 4); got != "██  " {
		t.Errorf("flat high line: %q", got)
	}
	if got := sparkline0(nil, 3); got != "   " {
		t.Errorf("empty: %q", got)
	}
}

func throughputSnap() store.StoreSnapshot {
	now := time.Now()
	return store.StoreSnapshot{Replication: store.ReplicationState{
		UpdatedAt: now, HistorySpan: 20 * time.Minute,
		Settings:      map[string]string{"replication.logical.recovery.chunk_size": "1mb", "replication.logical.recovery.max_concurrent_file_chunks": "2"},
		Subscriptions: []cratedb.Subscription{{Name: "s", Tables: []cratedb.SubscribedTable{{Schema: "doc", Table: "big", State: "r"}}}},
		Replicas: map[string]store.ReplicaTable{"doc.big": {
			ReplicaShards: store.ReplicaShards{Shards: 2, Started: 2, Docs: 1000, Size: 1 << 20},
			Rate:          3, Replicated: 4000, History: []float64{0, 0, 10, 3},
			Copy: &store.CopyResult{Bytes: 1200 << 20, Wall: 220 * time.Second, FromStart: true, ShardBps: 1 << 20},
		}},
		Subscribers: []store.LeaseHolder{{Subscriber: "dr", Tables: 1, Behind: 300, PulledRate: 12, Pulled: 9000,
			History: []float64{0, 100, 200, 300}, LastSeen: now, Expires: now.Add(time.Hour), Period: 12 * time.Hour}},
		Publications: []cratedb.Publication{{Name: "p", Tables: []string{"doc.x"}}},
	}}
}

func TestReplicationThroughputView(t *testing.T) {
	view := NewReplicationModel(160, 60).Refresh(throughputSnap()).View()
	for _, want := range []string{
		"LAST 20M", "▁▁█▃",
		"copied 1.2GB in 3m40s, 5.5MB/s · 1.0MB/s per shard",
		"2 × 1.0MB chunks in flight at 1.0MB/s per shard ≈ one round trip every 2.0s",
		"replicated since obsi started: 4.0K changes (≈ 4.0MB",
		"BEHIND, 20M", "PULLED/s", "▁▃▅█", "operations pulled since obsi started: dr 9.0K",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("view lacks %q:\n%s", want, view)
		}
	}
	narrow := NewReplicationModel(100, 60).Refresh(throughputSnap()).View()
	if strings.Contains(narrow, "LAST 20M") || strings.Contains(narrow, "▁▃▅█") {
		t.Errorf("sparklines on a narrow terminal:\n%s", narrow)
	}
}
