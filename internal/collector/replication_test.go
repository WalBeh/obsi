package collector

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestParseSubscriptions(t *testing.T) {
	got := parseSubscriptions([][]interface{}{
		{"empty", "crate://h:4300?user=*&password=*&mode=sniff", []interface{}{"p0"}, nil, nil, nil},
		{"s1", "crate://h:4300?user=*&mode=sniff", []interface{}{"p1", "p2"}, "doc.a", "r", nil},
		{"s1", "crate://h:4300?user=*&mode=sniff", []interface{}{"p1", "p2"}, `"My"."b"`, "e", "Relation already exists"},
	})
	want := []cratedb.Subscription{
		{Name: "empty", ConnInfo: "crate://h:4300?user=*&password=*&mode=sniff", Publications: []string{"p0"}},
		{Name: "s1", ConnInfo: "crate://h:4300?user=*&mode=sniff", Publications: []string{"p1", "p2"}, Tables: []cratedb.SubscribedTable{
			{Schema: "doc", Table: "a", State: "r"},
			{Schema: "My", Table: "b", State: "e", Reason: "Relation already exists"},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}

func TestParsePublications(t *testing.T) {
	got := parsePublications([][]interface{}{
		{"all", true, "doc", "a"},
		{"all", true, "doc", "b"},
		{"none", false, nil, nil},
	})
	want := []cratedb.Publication{{Name: "all", AllTables: true, Tables: []string{"doc.a", "doc.b"}}, {Name: "none"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// A shard still being copied has no STARTED primary row; partitions count
// separately; replicas don't add to docs or seq numbers.
func TestParseReplicaShards(t *testing.T) {
	got := parseReplicaShards([][]interface{}{
		// table, partition, id, primary, state, docs, max_seq_no, size, copied bytes, copy ms
		{"doc.a", "", float64(0), true, "STARTED", float64(10), float64(9), float64(1000), float64(900), float64(40)},
		{"doc.a", "", float64(0), false, "STARTED", float64(10), float64(9), float64(1000), float64(1000), float64(50)},
		{"doc.a", "", float64(1), false, "UNASSIGNED", float64(0), nil, nil, nil, nil},
		{"doc.p", "04132", float64(0), true, "STARTED", float64(3), float64(2), float64(300), float64(300), float64(10)},
		{"doc.p", "04134", float64(0), true, "STARTED", float64(0), float64(-1), float64(100), nil, nil},
	})
	want := map[string]store.ReplicaShards{
		"doc.a": {Shards: 2, Started: 1, Docs: 10, SeqNo: 10, Size: 1000, CopiedBytes: 900, CopyShardMs: 40},
		"doc.p": {Shards: 2, Started: 2, Docs: 3, SeqNo: 3, Size: 400, CopiedBytes: 300, CopyShardMs: 10},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestParseLeases(t *testing.T) {
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC).Local()
	// The publisher's clock is 10s ahead of obsi's.
	cluster := now.Add(10 * time.Second).UnixMilli()
	lease := func(sub string, seq int64, age time.Duration) map[string]interface{} {
		return map[string]interface{}{
			"id": "logical_replication:" + sub + ":[t][0]", "source": "logical_replication:" + sub,
			"retaining_seq_no": float64(seq), "timestamp": float64(cluster - age.Milliseconds()),
		}
	}
	peer := map[string]interface{}{"id": "peer_recovery/x", "source": "peer recovery", "retaining_seq_no": float64(0), "timestamp": float64(cluster)}
	rows := [][]interface{}{
		{float64(cluster), "doc.a", float64(100), []interface{}{peer, lease("dr", 100, time.Second), lease("old", 40, 3*time.Hour)}},
		{float64(cluster), "doc.b", float64(50), []interface{}{lease("dr", 45, 5*time.Second)}},
		// Copied, nothing pulled since: the lease says 0.
		{float64(cluster), "doc.b", float64(30), []interface{}{lease("dr", 0, 5*time.Second)}},
	}
	if got := leasedTables(rows); !reflect.DeepEqual(got, []string{"doc.a", "doc.b"}) {
		t.Errorf("leasedTables = %v", got)
	}
	period := func(fqn string) time.Duration {
		if fqn == "doc.b" {
			return time.Minute
		}
		return 12 * time.Hour
	}
	got := parseLeases(rows, now, period, map[string]bool{"doc.a": true})
	want := []store.LeaseHolder{
		{Subscriber: "dr", Tables: 2, On: []string{"doc.a", "doc.b"}, Unpublished: 1, Behind: 5, Unknown: 1, Retained: 145, Known: 2, LastSeen: now.Add(-time.Second),
			Expires: now.Add(-5*time.Second + time.Minute), Period: time.Minute},
		{Subscriber: "old", Tables: 1, On: []string{"doc.a"}, Behind: 60, Retained: 40, Known: 1, LastSeen: now.Add(-3 * time.Hour),
			Expires: now.Add(9 * time.Hour), Period: 12 * time.Hour},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
}

func TestParseLeasePeriod(t *testing.T) {
	ddl := `CREATE TABLE IF NOT EXISTS "doc"."t" (...) WITH (
   "soft_deletes.retention_lease.period" = 120000,
   "store.type" = 'fs'
)`
	if got := parseLeasePeriod(ddl); got != 2*time.Minute {
		t.Errorf("got %s", got)
	}
	if got := parseLeasePeriod(`CREATE TABLE "doc"."t" (a INT)`); got != defaultLeasePeriod {
		t.Errorf("default: got %s", got)
	}
}

// Review findings on phase 4: Reset doesn't wait for a running poll, and
// tables whose period can't be read don't block the others.
func TestReplicationCollectorResetAndFailedReads(t *testing.T) {
	cfg := config.DefaultConfig()
	c := NewReplicationCollector(cfg.Collectors["replication"], NewQueryTracker(cfg.Collectors, cfg.Connection))
	c.mu.Lock()
	done := make(chan struct{})
	go func() { c.Reset(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Reset waited for the collector's lock")
	}
	c.mu.Unlock()

	reg := cratedb.NewRegistry("http://127.0.0.1:1", "crate", "", 100*time.Millisecond, 100*time.Millisecond, time.Hour, time.Hour, false)
	var tables []string
	for i := range maxPeriodReads + 5 {
		tables = append(tables, fmt.Sprintf("doc.t%02d", i))
	}
	ctx := context.Background()
	c.refreshPeriods(ctx, reg, append(tables, tables[0]))
	if len(c.periods) != maxPeriodReads {
		t.Fatalf("first poll recorded %d tables, want %d", len(c.periods), maxPeriodReads)
	}
	c.refreshPeriods(ctx, reg, tables)
	if len(c.periods) != len(tables) {
		t.Errorf("second poll didn't move on: %d of %d recorded", len(c.periods), len(tables))
	}
	if c.period("doc.t00") != defaultLeasePeriod {
		t.Errorf("failed read period = %s", c.period("doc.t00"))
	}
}
