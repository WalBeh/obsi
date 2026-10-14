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
	// Two samples per character, dots from the bottom, scaled from 0:
	// 8 is four dots, 1 and 2 round to one, 4 to two; 0 stays blank.
	if got := sparkline0([]float64{0, 8, 1, 2, 4, 4}, 6, 3); got != "⢸⣀⣤" {
		t.Errorf("got %q", got)
	}
	if got := sparkline0([]float64{5, 5}, 2, 4); got != "   ⣿" {
		t.Errorf("flat high line, right-aligned: %q", got)
	}
	if got := sparkline0([]float64{0, 0, 0, 0}, 4, 2); got != "  " {
		t.Errorf("idle series draws nothing: %q", got)
	}
	if got := sparkline0(nil, 0, 3); got != "   " {
		t.Errorf("empty: %q", got)
	}
	// Review finding: 21..119 samples kept 20 characters only at 120.
	for _, n := range []int{21, 41, 61, 119, 120} {
		vals := make([]float64, n)
		vals[n-1] = 1
		if got := []rune(sparkline0(vals, n, 20)); len(got) != 20 || got[19] == ' ' {
			t.Errorf("%d samples: %q", n, string(got))
		}
	}
	// A series that started 4 polls ago, on a timeline of 8: right-aligned.
	if got := sparkline0([]float64{1, 2, 3, 4}, 8, 4); got != "  ⣠⣾" {
		t.Errorf("late series: %q", got)
	}
	if sparkPeak([]float64{0, 95, 3}, "/s") != "peak 95/s" || sparkPeak([]float64{0, 0}, "") != "" {
		t.Error("sparkPeak")
	}
}

func throughputSnap() store.StoreSnapshot {
	now := time.Now()
	return store.StoreSnapshot{Replication: store.ReplicationState{
		UpdatedAt: now, HistorySpan: 20 * time.Minute, HistoryLen: 4,
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
		"LAST 20M", "⣇ peak 10/s",
		"copied 1.2GB in 3m40s, 5.5MB/s · 1.0MB/s per shard",
		"2 × 1.0MB chunks in flight at 1.0MB/s per shard ≈ one round trip every 2.0s",
		"replicated since obsi started: 4.0K changes (≈ 4.0MB",
		"BEHIND, 20M", "PULLED/s", "⢀⣾ peak 300", "operations pulled since obsi started: dr 9.0K",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("view lacks %q:\n%s", want, view)
		}
	}
	narrow := NewReplicationModel(100, 60).Refresh(throughputSnap()).View()
	if strings.Contains(narrow, "LAST 20M") || strings.Contains(narrow, "peak 300") {
		t.Errorf("sparklines on a narrow terminal:\n%s", narrow)
	}
}

func TestReplicaLines(t *testing.T) {
	snap := func(setting string, unassigned int, partitionReplicas ...int) store.StoreSnapshot {
		pr := 0
		if len(partitionReplicas) > 0 {
			pr = partitionReplicas[0]
		}
		now := time.Now()
		return store.StoreSnapshot{
			Nodes: []store.NodeSnapshot{{NodeInfo: cratedb.NodeInfo{ID: "a", Name: "sub1", Version: "6.4.5"}}},
			Tables: []cratedb.TableInfo{{SchemaName: "doc", TableName: "p",
				Settings: cratedb.TableSettings{NumberOfReplicas: setting, PartitionedBy: []string{"day"}}}},
			Replication: store.ReplicationState{UpdatedAt: now,
				Subscriptions: []cratedb.Subscription{{Name: "s", Tables: []cratedb.SubscribedTable{{Schema: "doc", Table: "p", State: "r"}}}},
				Replicas:      map[string]store.ReplicaTable{"doc.p": {ReplicaShards: store.ReplicaShards{Shards: 2, Started: 2, UnassignedReplicas: unassigned, PartitionReplicas: pr}, Rate: -1}},
			},
		}
	}
	view := func(s store.StoreSnapshot) string {
		return strings.Join(strings.Fields(NewReplicationModel(160, 60).Refresh(s).View()), " ")
	}
	got := view(snap("1", 2))
	for _, want := range []string{"2 replica copies can't be placed: number_of_replicas 1 here on 1 node",
		"number_of_replicas = '0-1' on the publisher's table", `ALTER TABLE "doc"."p" SET (number_of_replicas = 0) here`,
		"New partitions bring the publisher's count again"} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %q:\n%s", want, got)
		}
	}
	if got := view(snap("1", 0)); !strings.Contains(got, "New partitions will arrive with number_of_replicas 1") {
		t.Errorf("future partitions not flagged:\n%s", got)
	}
	if got := view(snap("0-1", 1, 1)); !strings.Contains(got, "older partitions here keep number_of_replicas 1 from when they arrived") ||
		strings.Contains(got, "on the publisher's table") {
		t.Errorf("older partitions:\n%s", got)
	}
	if got := view(snap("0-1", 0)); strings.Contains(got, "replica") && strings.Contains(got, "can't place") {
		t.Errorf("a range that fits is flagged:\n%s", got)
	}
}
