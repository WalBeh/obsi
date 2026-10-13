package tui

import (
	"reflect"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func settingsModel() ReplicationModel {
	now := time.Now()
	snap := store.StoreSnapshot{
		Nodes: []store.NodeSnapshot{
			{NodeInfo: cratedb.NodeInfo{ID: "a", Name: "lab1", ThreadPools: []cratedb.ThreadPoolStats{{Name: "logical_replication", Threads: 7, Active: 1}}}},
			{NodeInfo: cratedb.NodeInfo{ID: "b", Name: "lab2", ThreadPools: []cratedb.ThreadPoolStats{{Name: "logical_replication", Threads: 7, Queue: 3, Rejected: 2}}}},
		},
		Replication: store.ReplicationState{
			UpdatedAt:     now,
			Subscriptions: []cratedb.Subscription{{Name: "s", Tables: []cratedb.SubscribedTable{{Schema: "doc", Table: "x", State: "r"}}}},
			Publications:  []cratedb.Publication{{Name: "p", Tables: []string{"doc.a", "doc.b", "doc.c", "doc.d"}}},
			Subscribers: []store.LeaseHolder{
				{Subscriber: "dr", Tables: 2, On: []string{"doc.a", "doc.b"}, Unknown: 1, LastSeen: now, Expires: now.Add(time.Hour), Period: 12 * time.Hour,
					MaxSilence: 40 * time.Minute},
				// Pulls another publication's table: not this form's business.
				{Subscriber: "elsewhere", Tables: 1, On: []string{"doc.zzz"}, LastSeen: now, Expires: now.Add(time.Hour), Period: 12 * time.Hour,
					MaxSilence: 3 * time.Hour},
			},
			Settings: map[string]string{
				"replication.logical.ops_batch_size":                           "1000",
				"replication.logical.reads_poll_duration":                      "50ms",
				"replication.logical.recovery.chunk_size":                      "1mb",
				"replication.logical.recovery.max_concurrent_file_chunks":      "2",
				"cluster.routing.allocation.node_initial_primaries_recoveries": "4",
			},
			LeasePeriods: map[string]time.Duration{"doc.a": 12 * time.Hour, "doc.b": 2 * time.Minute, "doc.c": 20 * time.Millisecond},
		},
	}
	m := NewReplicationModel(140, 60).Refresh(snap)
	m.persistent = true
	return m
}

func TestReplicationSettingsView(t *testing.T) {
	view := settingsModel().View()
	for _, want := range []string{
		"Subscriber settings", "ops_batch_size 1000 (default 50000)", "reads_poll_duration 50ms",
		"doc.b 2m ▲ under 1h", "doc.c 20ms ▲ under reads_poll_duration", "others 12h",
		"logical_replication thread pool: 2 nodes, 14 threads, 1 active, queue 3, rejected 2",
		"lab2 queue 3, rejected 2",
		"0?", "got no change since their initial copy",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("view lacks %q:\n%s", want, view)
		}
	}
	if strings.Contains(view, "chunk_size 1mb (default") {
		t.Error("a default value is flagged")
	}
}

func TestReplicationSettingsForm(t *testing.T) {
	m := settingsModel()
	m.readOnly = true
	if m, _ = keys(m, "c"); settingsOf(m) != nil {
		t.Fatal("read-only opened the settings")
	}
	m.readOnly = false
	m, _ = keys(m, "c", "enter")
	if settingsOf(m) == nil || settingsOf(m).buf != "1000" {
		t.Fatalf("edit didn't start from the value: %+v", settingsOf(m))
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("2000")})
	m, cmd := keys(m, "enter")
	if req, ok := cmd().(ReplSettingRequest); !ok || req != (ReplSettingRequest{Path: "replication.logical.ops_batch_size", Value: "12000", Persistent: true}) {
		t.Errorf("request = %+v", req)
	}
	m, cmd = keys(m, "c", "down", "x")
	if apply, ok := cmd().(ApplyReplicationMsg); !ok || settingsOf(m) != nil || apply.Stmt != `RESET GLOBAL "replication.logical.reads_poll_duration"` {
		t.Errorf("reset = %+v, form open %v", apply, settingsOf(m) != nil)
	}
}

func TestLeaseForm(t *testing.T) {
	m := settingsModel()
	m, _ = keys(m, "down", "l") // the publication is after the subscription
	f := leaseOf(m)
	if f == nil || f.pub != "p" {
		t.Fatalf("lease form: %+v", f)
	}
	m, _ = keys(m, "down", "enter")
	if f.buf != "2m" {
		t.Fatalf("buf = %q", f.buf)
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("soon")})
	m, _ = keys(m, "enter")
	if !strings.Contains(f.err, "isn't a time value") {
		t.Fatalf("bad value accepted: %q", f.err)
	}
	f.buf = "30m"
	m, _ = keys(m, "enter")
	view := m.View()
	for _, want := range []string{`ALTER TABLE "doc"."b" SET ("soft_deletes.retention_lease.period" = '30m');`, "Under an hour", "dr already went 40m0s"} {
		if !strings.Contains(view, want) {
			t.Errorf("confirm lacks %q:\n%s", want, view)
		}
	}
	m, cmd := keys(m, "y")
	apply := cmd().(ApplyReplicationMsg)
	if apply.Stmt != `ALTER TABLE "doc"."b" SET ("soft_deletes.retention_lease.period" = ?)` || !reflect.DeepEqual(apply.Args, []interface{}{"30m"}) || !apply.Periods {
		t.Errorf("apply = %+v", apply)
	}

	m, _ = keys(m, "l", "down", "x")
	_, cmd = keys(m, "y")
	if apply := cmd().(ApplyReplicationMsg); apply.Stmt != `ALTER TABLE "doc"."b" RESET ("soft_deletes.retention_lease.period")` {
		t.Errorf("reset = %+v", apply)
	}
}
