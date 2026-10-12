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

// As ticket() writes it, copied from the shardlab.
const labTicket = `-- Subscribe to publication lab_pub2 of cluster shardlab (copied by obsi)
-- Tables: doc.repl, doc.repl_p
-- Run on the subscriber; none of these tables may exist there.
--   lab1  6e538939fdcd  psql 5432  transport 4300
CREATE SUBSCRIPTION "lab_pub2" CONNECTION 'crate://<host>:5432?user=<user>&password=<password>&mode=pg_tunnel' PUBLICATION "lab_pub2";
`

func TestParseTicket(t *testing.T) {
	pubs, tables, ok := parseTicket(labTicket)
	if !ok || !reflect.DeepEqual(pubs, []string{"lab_pub2"}) || !reflect.DeepEqual(tables, []string{"doc.repl", "doc.repl_p"}) {
		t.Errorf("got %v %v %v", pubs, tables, ok)
	}
	pubs, tables, _ = parseTicket("-- Tables: all tables (2 now): doc.a, doc.b\nCREATE SUBSCRIPTION s CONNECTION 'x' PUBLICATION \"we\"\"ird\", p2;")
	if !reflect.DeepEqual(pubs, []string{`we"ird`, "p2"}) || !reflect.DeepEqual(tables, []string{"doc.a", "doc.b"}) {
		t.Errorf("all tables: got %v %v", pubs, tables)
	}
	if _, _, ok := parseTicket("just some text"); ok {
		t.Error("plain text parsed as a ticket")
	}
}

func subModel() ReplicationModel {
	snap := store.StoreSnapshot{
		Tables: []cratedb.TableInfo{{SchemaName: "doc", TableName: "repl_p"}, {SchemaName: "doc", TableName: "mine"}},
		Replication: store.ReplicationState{UpdatedAt: time.Now(), Subscriptions: []cratedb.Subscription{
			{Name: "old", Tables: []cratedb.SubscribedTable{{Schema: "doc", Table: "mine", State: "r"}}},
		}},
	}
	return NewReplicationModel(120, 40).Refresh(snap)
}

func typeRepl(m ReplicationModel, s string) ReplicationModel {
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)})
	return m
}

func TestSubscriptionForm(t *testing.T) {
	m := subModel()
	m, _ = keys(m, "s")
	if m.subForm == nil {
		t.Fatal("s didn't open the form")
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(labTicket), Paste: true})
	f := m.subForm
	if f.vals[subName] != "lab_pub2" || f.vals[subPubs] != "lab_pub2" || f.field != subHost {
		t.Fatalf("ticket not applied: %+v", f.vals)
	}
	m = typeRepl(m, "pub.example.com")
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyTab})   // port
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyTab})   // mode
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRight}) // sniff
	if f.vals[subMode] != "sniff" || f.vals[subPort] != "4300" {
		t.Fatalf("mode switch: %+v", f.vals)
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyLeft}) // back to pg_tunnel, 5432
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyTab})  // ssl
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRight})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyTab}) // user
	m = typeRepl(m, "repl")
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyTab}) // password
	m = typeRepl(m, "p&ss w")
	if strings.Contains(m.View(), "p&ss") {
		t.Error("password shown in the form")
	}
	m, _ = keys(m, "enter")
	if !f.review {
		t.Fatalf("no review: %q", f.err)
	}
	if got := f.shown("***"); got != `CREATE SUBSCRIPTION "lab_pub2" CONNECTION 'crate://pub.example.com:5432?user=repl&password=***&mode=pg_tunnel&sslmode=require' PUBLICATION "lab_pub2"` {
		t.Errorf("shown = %s", got)
	}
	view := m.View()
	for _, want := range []string{"password=***", "bind parameter", "These tables exist here", "doc.repl_p"} {
		if !strings.Contains(view, want) {
			t.Errorf("review lacks %q:\n%s", want, view)
		}
	}
	if strings.Contains(view, "p&ss") {
		t.Error("password in the review")
	}
	// doc.repl_p exists here: not run.
	m, cmd := keys(m, "y")
	if cmd != nil || m.subForm == nil || !strings.Contains(f.err, "not run") {
		t.Fatalf("ran despite an existing table: %q", f.err)
	}
	f.tables = []string{"doc.repl"}
	m, cmd = keys(m, "y")
	apply, ok := cmd().(ApplySubscriptionMsg)
	if !ok || apply.Stmt != `CREATE SUBSCRIPTION "lab_pub2" CONNECTION ? PUBLICATION "lab_pub2"` ||
		apply.Args[0] != "crate://pub.example.com:5432?user=repl&password=p%26ss+w&mode=pg_tunnel&sslmode=require" {
		t.Errorf("apply = %+v", apply)
	}
}

// A table failed as "Relation already exists" blocks any new subscription.
func TestSubscriptionBlockedByRelationExists(t *testing.T) {
	m := subModel()
	m.repl.Subscriptions[0].Tables[0] = cratedb.SubscribedTable{Schema: "doc", Table: "mine", State: "e", Reason: "Relation already exists"}
	m, _ = keys(m, "s")
	f := m.subForm
	f.vals[subName], f.vals[subHost], f.vals[subUser], f.vals[subPubs] = "new", "h", "u", "p"
	m, _ = keys(m, "enter")
	if b := f.blockers(); len(b) != 1 || !strings.Contains(b[0], "Drop old first") {
		t.Errorf("blockers = %q", b)
	}
	if !strings.Contains(m.View(), "try to delete the local doc.mine") {
		t.Errorf("review doesn't warn:\n%s", m.View())
	}
}

func TestSubscriptionDropTyped(t *testing.T) {
	m := subModel()
	m.readOnly = true
	m, _ = keys(m, "d")
	if m.subDrop != nil {
		t.Fatal("read-only opened the drop")
	}
	m.readOnly = false
	m, _ = keys(m, "e")
	if !strings.Contains(m.noticeText, "can't be changed") {
		t.Errorf("e on a subscription: %q", m.noticeText)
	}
	m, _ = keys(m, "d")
	if m.subDrop == nil || !strings.Contains(m.View(), "doc.mine") {
		t.Fatalf("drop confirm: %+v", m.subDrop)
	}
	m = typeRepl(m, "ol")
	if _, cmd := keys(m, "enter"); cmd != nil {
		t.Error("ran without the full name")
	}
	m = typeRepl(m, "d")
	_, cmd := keys(m, "enter")
	if apply, ok := cmd().(ApplySubscriptionMsg); !ok || apply.Stmt != `DROP SUBSCRIPTION "old"` {
		t.Errorf("apply = %+v", apply)
	}
}
