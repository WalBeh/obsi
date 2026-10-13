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
		"FAILED", "Relation already exists", "Don't create another subscription",
		"all  all tables (2)",
		"4.2K", "7h0m ago", "expires in 4h59m of 12h0m",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("view lacks %q:\n%s", want, view)
		}
	}
	for _, line := range strings.Split(view, "\n") {
		if f := strings.Fields(line); strings.HasPrefix(line, "    doc.broken") && (f[len(f)-1] != "—" || f[len(f)-2] != "—") {
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
	if !strings.Contains(m.View(), "more lines") {
		t.Fatalf("no scroll marker:\n%s", m.View())
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnd})
	lines := strings.Split(m.View(), "\n")
	if len(lines) != 8 || !strings.Contains(lines[0], "↑ ") || strings.Count(m.View(), "more lines") != 1 {
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

func TestReplicationLimitedUser(t *testing.T) {
	now := time.Now()
	st := store.ReplicationState{UpdatedAt: now, User: "mon",
		Errs: map[string]string{"leases": "SchemaUnknownException[Schema 'sys' unknown]",
			"settings": `cratedb error (status 401) from http://h:4200: {"error":{"message":"MissingPrivilegeException[Missing 'DQL' privilege for user 'mon']","code":4011}}`}}
	view := NewReplicationModel(240, 40).Refresh(store.StoreSnapshot{Replication: st}).View()
	for _, want := range []string{
		"couldn't read subscriber leases (sys.shards), showing the last poll: SchemaUnknownException",
		"couldn't read replication settings (sys.cluster): MissingPrivilegeException[Missing 'DQL' privilege for user 'mon'] · GRANT DQL ON SCHEMA sys TO mon",
		"No publications or subscriptions owned by mon.",
		"only to the user who created them, superusers included: obsi connects as mon",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("view lacks %q:\n%s", want, view)
		}
	}
}

// CrateDB filters pg_subscription and pg_publication by owner for
// superusers too (checked on 6.4.5): crate doesn't see admin's.
func TestReplicationOwnerNote(t *testing.T) {
	st := store.ReplicationState{UpdatedAt: time.Now(), User: "crate", Superuser: true}
	view := NewReplicationModel(240, 40).Refresh(store.StoreSnapshot{Replication: st}).View()
	if !strings.Contains(strings.Join(strings.Fields(view), " "), "superusers included: obsi connects as crate, so replication set up by another user (e.g. admin on CrateDB Cloud) doesn't show here") {
		t.Errorf("no owner note for a superuser:\n%s", view)
	}
	st.Subscribers = []store.LeaseHolder{{Subscriber: "sub", Tables: 2, Unpublished: 1, LastSeen: time.Now()}}
	view = NewReplicationModel(240, 40).Refresh(store.StoreSnapshot{Replication: st}).View()
	flat := strings.Join(strings.Fields(view), " ")
	for _, want := range []string{"none owned by crate (others' don't show)", "* pulls tables no publication visible to crate lists: one another user owns",
		"Shows what crate owns: CrateDB hides other users' publications and subscriptions, also from superusers."} {
		if !strings.Contains(flat, want) {
			t.Errorf("view lacks %q:\n%s", want, view)
		}
	}
}

// A subscription another user owns shows only while its initial copy
// runs, as a restore from _logical_replication_<name> (seen on 6.4.5).
func TestReplicationHiddenCopy(t *testing.T) {
	st := store.ReplicationState{UpdatedAt: time.Now(), User: "crate", Superuser: true,
		Subscriptions: []cratedb.Subscription{{Name: "mine"}}}
	snap := store.StoreSnapshot{Replication: st, Snapshots: store.SnapshotsState{Restores: []cratedb.SnapshotRestore{
		{Repository: "_logical_replication_s_owner2", Name: "_latest_", State: "STARTED", Shards: 60, Done: 52},
		{Repository: "_logical_replication_mine", Name: "_latest_", State: "STARTED", Shards: 4},
		{Repository: "backups", Name: "nightly", State: "STARTED", Shards: 10},
	}}}
	flat := strings.Join(strings.Fields(NewReplicationModel(200, 40).Refresh(snap).View()), " ")
	if !strings.Contains(flat, "Subscription s_owner2 is copying its tables now (52/60 shards, from sys.snapshot_restore). It isn't listed because a user other than crate owns it") {
		t.Errorf("hidden copy missing:\n%s", flat)
	}
	if strings.Contains(flat, "Subscription mine is copying") || strings.Contains(flat, "nightly") {
		t.Errorf("listed subscription or a real restore flagged:\n%s", flat)
	}
	ov := NewOverviewModel(200, 40, true)
	ov.snap = snap
	if out := ov.renderReplication(); !strings.Contains(out, "s_owner2 copying (another user's subscription)") || strings.Contains(out, "mine copying") {
		t.Errorf("overview:\n%s", out)
	}
	sm := NewSnapshotsModel(200, 40).Refresh(store.StoreSnapshot{Snapshots: store.SnapshotsState{UpdatedAt: time.Now(), Restores: snap.Snapshots.Restores}})
	out := sm.View()
	for _, want := range []string{"Restores running", "backups.nightly", "Replication copies running", "subscription s_owner2  STARTED  52/60 shards"} {
		if !strings.Contains(out, want) {
			t.Errorf("snapshots tab missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "_logical_replication_") {
		t.Errorf("internal repository shown as a restore:\n%s", out)
	}
	ov.snap.Snapshots.UpdatedAt, ov.snap.Snapshots.ListedAt = time.Now(), time.Now()
	ov.snap.Snapshots.Repositories = []cratedb.Repository{{Name: "backups"}}
	if line := ov.renderSnapshots(); !strings.Contains(line, "· 1 restore") {
		t.Errorf("restore count on the Overview counts copies:\n%s", line)
	}
}
