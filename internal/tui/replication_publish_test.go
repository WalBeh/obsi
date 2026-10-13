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

func pubModel() ReplicationModel {
	snap := store.StoreSnapshot{
		Tables: []cratedb.TableInfo{{SchemaName: "doc", TableName: "b"}, {SchemaName: "doc", TableName: "a"}, {SchemaName: "doc", TableName: "c"}},
		Replication: store.ReplicationState{UpdatedAt: time.Now(), Publications: []cratedb.Publication{
			{Name: "p1", Tables: []string{"doc.a"}},
			{Name: "everything", AllTables: true, Tables: []string{"doc.a", "doc.b", "doc.c"}},
		}},
	}
	return NewReplicationModel(120, 40).Refresh(snap)
}

func keys(m ReplicationModel, ks ...string) (ReplicationModel, tea.Cmd) {
	var cmd tea.Cmd
	for _, k := range ks {
		msg := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(k)}
		switch k {
		case "enter":
			msg = tea.KeyMsg{Type: tea.KeyEnter}
		case "esc":
			msg = tea.KeyMsg{Type: tea.KeyEsc}
		case "down":
			msg = tea.KeyMsg{Type: tea.KeyDown}
		case " ":
			msg = tea.KeyMsg{Type: tea.KeySpace, Runes: []rune(" ")}
		}
		m, cmd = m.HandleKey(msg)
	}
	return m, cmd
}

func TestPublicationStatements(t *testing.T) {
	m := pubModel()
	cases := []struct {
		name string
		edit *cratedb.Publication
		set  func(f *pubForm)
		want []string
	}{
		{"new tables", nil, func(f *pubForm) { f.name, f.checked["doc.b"], f.checked["doc.c"] = "n", true, true },
			[]string{`CREATE PUBLICATION "n" FOR TABLE "doc"."b", "doc"."c"`}},
		// doc.a is in p1 already: CREATE ... FOR TABLE would refuse it.
		{"new elsewhere", nil, func(f *pubForm) { f.name, f.checked["doc.a"] = "n", true },
			[]string{`CREATE PUBLICATION "n"`, `ALTER PUBLICATION "n" ADD TABLE "doc"."a"`}},
		{"new all", nil, func(f *pubForm) { f.name, f.all = "n", true }, []string{`CREATE PUBLICATION "n" FOR ALL TABLES`}},
		{"new empty", nil, func(f *pubForm) { f.name = "n" }, []string{`CREATE PUBLICATION "n"`}},
		{"edit", &m.repl.Publications[0], func(f *pubForm) { f.checked["doc.c"] = true },
			[]string{`ALTER PUBLICATION "p1" SET TABLE "doc"."a", "doc"."c"`}},
		{"edit to none", &m.repl.Publications[0], func(f *pubForm) { f.checked["doc.a"] = false },
			[]string{`ALTER PUBLICATION "p1" DROP TABLE "doc"."a"`}},
	}
	for _, c := range cases {
		f := m.newPubForm(c.edit)
		c.set(f)
		if got := f.stmts(); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestPublicationFormKeys(t *testing.T) {
	m := pubModel()
	m.readOnly = true
	m, _ = keys(m, "n")
	if pubFormOf(m) != nil || !strings.Contains(m.noticeText, "read-only") {
		t.Fatalf("read-only opened the form: %q", m.noticeText)
	}
	m.readOnly = false

	m, _ = keys(m, "n", "p", "1", "enter")
	if pubFormOf(m) == nil || pubFormOf(m).phase != pubName || !strings.Contains(pubFormOf(m).err, "exists") {
		t.Fatalf("taken name accepted: %+v", pubFormOf(m))
	}
	m, _ = keys(m, "x", "enter", "down", " ", "enter")
	if pubFormOf(m).phase != pubReview {
		t.Fatalf("phase = %d, err %q", pubFormOf(m).phase, pubFormOf(m).err)
	}
	if got := pubFormOf(m).stmts(); !reflect.DeepEqual(got, []string{`CREATE PUBLICATION "p1x" FOR TABLE "doc"."b"`}) {
		t.Errorf("stmts = %q", got)
	}
	view := m.View()
	if !strings.Contains(view, "checking privileges") {
		t.Errorf("review without the pending checks:\n%s", view)
	}
	m = m.setChecks(PublicationChecksMsg{Readers: []string{"stale"}, form: pubFormOf(m), seq: pubFormOf(m).seq - 1})
	m = m.setChecks(PublicationChecksMsg{Readers: []string{"other form"}, form: &pubForm{}, seq: pubFormOf(m).seq})
	if pubFormOf(m).checks != nil {
		t.Fatalf("stale checks applied: %+v", pubFormOf(m).checks)
	}
	m = m.setChecks(PublicationChecksMsg{Readers: []string{"crate", "repl"}, Periods: map[string]time.Duration{"doc.b": 2 * time.Minute}, Checked: 1, form: pubFormOf(m), seq: pubFormOf(m).seq})
	view = m.View()
	for _, want := range []string{"crate, repl", "doc.b 2m0s"} {
		if !strings.Contains(view, want) {
			t.Errorf("review lacks %q:\n%s", want, view)
		}
	}
	m, cmd := keys(m, "y")
	if pubFormOf(m) != nil || cmd == nil {
		t.Fatal("y didn't run")
	}
	if apply, ok := cmd().(ApplyPublicationMsg); !ok || apply.Stmts[0] != `CREATE PUBLICATION "p1x" FOR TABLE "doc"."b"` {
		t.Errorf("apply = %+v", apply)
	}

	// FOR ALL TABLES can't change tables; d asks first.
	m, _ = keys(m, "down", "e")
	if pubFormOf(m) != nil || !strings.Contains(m.noticeText, "FOR ALL TABLES") {
		t.Errorf("edit of an ALL TABLES publication: %q", m.noticeText)
	}
	m, _ = keys(m, "d")
	if pubDropOf(m) == nil || pubDropOf(m).name != "everything" {
		t.Fatalf("drop = %+v", pubDropOf(m))
	}
	m, cmd = keys(m, "y")
	if apply, ok := cmd().(ApplyPublicationMsg); !ok || apply.Stmts[0] != `DROP PUBLICATION "everything"` {
		t.Errorf("drop apply = %+v", apply)
	}
}

func TestUnpublishedLeaseMarked(t *testing.T) {
	now := time.Now()
	snap := store.StoreSnapshot{Replication: store.ReplicationState{UpdatedAt: now, Subscribers: []store.LeaseHolder{
		{Subscriber: "dr", Tables: 1, Unpublished: 1, LastSeen: now, Expires: now.Add(12 * time.Hour), Period: 12 * time.Hour},
	}}}
	view := NewReplicationModel(120, 30).Refresh(snap).View()
	if !strings.Contains(view, "1*") || !strings.Contains(view, "DROP SUBSCRIPTION on its side does") {
		t.Errorf("unpublished lease not marked:\n%s", view)
	}
}

// Review findings on #74.
func TestPublicationReviewEdges(t *testing.T) {
	m := pubModel()
	// doc.new is published but the shards poll hasn't listed it yet.
	pub := cratedb.Publication{Name: "p1", Tables: []string{"doc.a", "doc.new"}}
	f := m.newPubForm(&pub)
	if f.changed() {
		t.Error("an untouched edit counts as changed")
	}
	f.checked["doc.c"] = true
	if got := f.stmts(); !reflect.DeepEqual(got, []string{`ALTER PUBLICATION "p1" SET TABLE "doc"."a", "doc"."c", "doc"."new"`}) {
		t.Errorf("stmts = %q", got)
	}

	f = m.newPubForm(nil)
	f.name, f.all, f.phase = "n", true, pubReview
	if strings.Contains(f.render(120, 40), "created first") {
		t.Error("FOR ALL TABLES review describes CREATE then ADD")
	}
}

func TestReplicationArrowsScrollPastItems(t *testing.T) {
	now := time.Now()
	snap := store.StoreSnapshot{Replication: store.ReplicationState{UpdatedAt: now,
		Publications: []cratedb.Publication{{Name: "p", Tables: []string{"doc.a"}}},
		Subscribers:  []store.LeaseHolder{{Subscriber: "dr", Tables: 1, LastSeen: now, Expires: now.Add(time.Hour), Period: 12 * time.Hour}}}}
	m := NewReplicationModel(120, 6).Refresh(snap)
	if m.maxScroll == 0 {
		t.Fatal("test needs a page taller than the view")
	}
	for range m.maxScroll {
		m, _ = keys(m, "down")
	}
	if m.scroll != m.maxScroll {
		t.Errorf("down past the last item scrolled to %d of %d", m.scroll, m.maxScroll)
	}
}

// Typed views of the open dialog, nil when another (or none) is open.
func pubFormOf(m ReplicationModel) *pubForm           { f, _ := m.modal.(*pubForm); return f }
func pubDropOf(m ReplicationModel) *pubDrop           { f, _ := m.modal.(*pubDrop); return f }
func subFormOf(m ReplicationModel) *subForm           { f, _ := m.modal.(*subForm); return f }
func subDropOf(m ReplicationModel) *subDrop           { f, _ := m.modal.(*subDrop); return f }
func settingsOf(m ReplicationModel) *replSettingsForm { f, _ := m.modal.(*replSettingsForm); return f }
func leaseOf(m ReplicationModel) *leaseForm           { f, _ := m.modal.(*leaseForm); return f }
