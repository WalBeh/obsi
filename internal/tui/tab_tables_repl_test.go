package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func markerSnap() store.StoreSnapshot {
	now := time.Now()
	return store.StoreSnapshot{
		Tables: []cratedb.TableInfo{
			{SchemaName: "doc", TableName: "pulled"}, {SchemaName: "doc", TableName: "broken"},
			{SchemaName: "doc", TableName: "served"}, {SchemaName: "doc", TableName: "local"},
			{SchemaName: "doc", TableName: "a_table_name_that_is_far_too_long_for_the_column"},
		},
		Replication: store.ReplicationState{UpdatedAt: now, Superuser: true,
			Subscriptions: []cratedb.Subscription{{Name: "s1", Tables: []cratedb.SubscribedTable{
				{Schema: "doc", Table: "pulled", State: "r"}, {Schema: "doc", Table: "broken", State: "e", Reason: "x"},
				{Schema: "doc", Table: "a_table_name_that_is_far_too_long_for_the_column", State: "r"},
			}}},
			Publications: []cratedb.Publication{{Name: "p1", Tables: []string{"doc.served"}}},
			Subscribers: []store.LeaseHolder{{Subscriber: "dr", Tables: 1, On: []string{"doc.served"}, Behind: 1500,
				LastSeen: now, Expires: now.Add(12 * time.Hour), Period: 12 * time.Hour}},
		},
	}
}

func TestTablesReplicationMarkers(t *testing.T) {
	m := NewTablesModel(160, 40).Refresh(markerSnap())
	view := m.View()
	rows := map[string]string{}
	for _, l := range strings.Split(view, "\n") {
		for _, name := range []string{"doc.pulled", "doc.broken", "doc.served", "doc.local", "doc.a_table_name"} {
			if strings.Contains(l, name) {
				rows[name] = l
			}
		}
	}
	for name, mark := range map[string]string{"doc.pulled": "⇠", "doc.broken": "⇠", "doc.served": "⇢", "doc.a_table_name": "⇠"} {
		if !strings.Contains(rows[name], mark) {
			t.Errorf("%s lacks %s: %q", name, mark, rows[name])
		}
	}
	if strings.ContainsAny(rows["doc.local"], "⇠⇢") {
		t.Errorf("local table marked: %q", rows["doc.local"])
	}
	if !strings.Contains(view, "⇠ subscribed") {
		t.Errorf("no legend:\n%s", view)
	}
	served := cratedb.TableInfo{SchemaName: "doc", TableName: "served"}
	if got := m.renderDetail(served); !strings.Contains(got, "⇢ published in p1, pulled by dr") {
		t.Errorf("detail:\n%s", got)
	}
}

func TestOverviewReplication(t *testing.T) {
	m := NewOverviewModel(160, 60, true)
	m.snap = markerSnap()
	got := m.renderReplication()
	for _, want := range []string{"Replication", "pulls 3 tables (s1)", "1 failed", "serves p1 to dr, 1.5K behind"} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %q:\n%s", want, got)
		}
	}
	m.snap = store.StoreSnapshot{}
	if got := m.renderReplication(); got != "" {
		t.Errorf("without replication: %q", got)
	}
}
