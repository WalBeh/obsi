package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func replicationSnap(now time.Time) store.StoreSnapshot {
	return store.StoreSnapshot{Replication: store.ReplicationState{
		UpdatedAt: now,
		Subscriptions: []cratedb.Subscription{{
			Name: "s1", ConnInfo: "crate://pub:4300?user=*&password=*&mode=sniff", Publications: []string{"p1"},
			Tables: []cratedb.SubscribedTable{
				{Schema: "doc", Table: "synced", State: "r"},
				{Schema: "doc", Table: "copying", State: "d"},
				{Schema: "doc", Table: "stuck", State: "i"},
				{Schema: "doc", Table: "broken", State: "e", Reason: "Relation already exists"},
			},
		}},
		Replicas: map[string]store.ReplicaTable{
			"doc.synced":  {ReplicaShards: store.ReplicaShards{Shards: 3, Started: 3, Docs: 1500}, Rate: 12, LastChange: now.Add(-2 * time.Second)},
			"doc.copying": {ReplicaShards: store.ReplicaShards{Shards: 4, Started: 1}, Rate: -1, CopySince: now.Add(-90 * time.Second)},
			"doc.stuck":   {Rate: -1, CopySince: now.Add(-6 * time.Minute)},
			"doc.broken":  {ReplicaShards: store.ReplicaShards{Shards: 1, Started: 1}, Rate: 0},
		},
		Publications: []cratedb.Publication{{Name: "all", AllTables: true, Tables: []string{"doc.a", "doc.b"}}},
		Subscribers: []store.LeaseHolder{
			{Subscriber: "dr", Tables: 2, Behind: 4200, LastSeen: now.Add(-7 * time.Hour), Expires: now.Add(5 * time.Hour), Period: 12 * time.Hour},
		},
	}}
}

func TestReplicationView(t *testing.T) {
	now := time.Now()
	view := NewReplicationModel(130, 60).Refresh(replicationSnap(now)).View()
	for _, want := range []string{
		"s1  ← crate://pub:4300 (sniff)   publications p1",
		"in sync",
		"1.5K",
		"2s ago",
		"1/4", "for 1m30s",
		"no copy started yet",
		"FAILED", "Relation already exists",
		"all  all tables (2)",
		"4.2K", "7h0m ago", "expires in 4h59m of 12h0m",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("view lacks %q:\n%s", want, view)
		}
	}
	for _, line := range strings.Split(view, "\n") {
		if strings.Contains(line, "doc.broken") && !strings.HasSuffix(line, "—  —") {
			t.Errorf("failed table shows a rate: %q", line)
		}
		if strings.Contains(line, "doc.synced") && strings.Contains(line, "no copy") {
			t.Errorf("hint on the wrong row: %q", line)
		}
	}
}

func TestReplicationViewEmptyAndScroll(t *testing.T) {
	now := time.Now()
	empty := NewReplicationModel(100, 20).Refresh(store.StoreSnapshot{Replication: store.ReplicationState{UpdatedAt: now}}).View()
	if !strings.Contains(empty, "No publications or subscriptions") {
		t.Errorf("empty view: %s", empty)
	}

	m := NewReplicationModel(130, 8).Refresh(replicationSnap(now))
	if !strings.Contains(m.View(), "↓") {
		t.Fatalf("no scroll marker:\n%s", m.View())
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnd})
	lines := strings.Split(m.View(), "\n")
	if len(lines) != 8 || !strings.Contains(lines[0], "↑") || strings.Contains(m.View(), "↓") {
		t.Errorf("at the end:\n%s", m.View())
	}
}

func TestTablesDetailMarksSubscribed(t *testing.T) {
	m := NewTablesModel(120, 40)
	m.snap = replicationSnap(time.Now())
	if got := m.renderDetail(cratedb.TableInfo{SchemaName: "doc", TableName: "synced"}); !strings.Contains(got, "⇠ subscription s1, read-only") {
		t.Errorf("subscribed table not marked:\n%s", got)
	}
	if got := m.renderDetail(cratedb.TableInfo{SchemaName: "doc", TableName: "local"}); strings.Contains(got, "⇠") {
		t.Errorf("local table marked:\n%s", got)
	}
}
