package tui

import (
	"reflect"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// Pins the list behaviour Nodes, Tables and Shards share (search, sort
// cycling, selection/scroll clamping) so it can be consolidated without
// drifting. Each tab is driven through a probe.

var listNames = []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima"}

type listProbe interface {
	key(tea.KeyMsg) listProbe
	refresh(n int) listProbe
	rows() []string
	sel() (selected, scroll, listH int)
	searchState() (searching bool, search string)
	sortState() (field int, desc bool)
	sortFields() int
}

func nodesSnap(n int) store.StoreSnapshot {
	var nodes []store.NodeSnapshot
	for i, name := range listNames[:n] {
		ns := store.NodeSnapshot{}
		ns.Name = name
		ns.CPUPercent = int16(i)
		nodes = append(nodes, ns)
	}
	return store.StoreSnapshot{Nodes: nodes}
}

func tablesSnap(n int) store.StoreSnapshot {
	var tables []cratedb.TableInfo
	for i, name := range listNames[:n] {
		tables = append(tables, cratedb.TableInfo{SchemaName: "doc", TableName: name, TotalSize: int64(i)})
	}
	return store.StoreSnapshot{Tables: tables}
}

func shardsSnap(n int) store.StoreSnapshot {
	shards := []cratedb.ShardInfo{{SchemaName: "doc", TableName: "started_t", RoutingState: "STARTED"}}
	for _, name := range listNames[:n] {
		shards = append(shards, cratedb.ShardInfo{SchemaName: "doc", TableName: name, RoutingState: "UNASSIGNED"})
	}
	return store.StoreSnapshot{Shards: shards}
}

type nodesProbe struct{ m NodesModel }

func (p nodesProbe) key(k tea.KeyMsg) listProbe { p.m, _ = p.m.HandleKey(k); return p }
func (p nodesProbe) refresh(n int) listProbe    { p.m = p.m.Refresh(nodesSnap(n)); return p }
func (p nodesProbe) rows() []string {
	var out []string
	for _, i := range p.m.sorted {
		out = append(out, p.m.snap.Nodes[i].Name)
	}
	return out
}
func (p nodesProbe) sel() (int, int, int)        { return p.m.selected, p.m.scroll, p.m.listHeight() }
func (p nodesProbe) searchState() (bool, string) { return p.m.searching, p.m.search }
func (p nodesProbe) sortState() (int, bool)      { return int(p.m.sortField), p.m.sortDesc }
func (p nodesProbe) sortFields() int             { return int(nodeSortFieldCount) }

type tablesProbe struct{ m TablesModel }

func (p tablesProbe) key(k tea.KeyMsg) listProbe { p.m, _ = p.m.HandleKey(k); return p }
func (p tablesProbe) refresh(n int) listProbe    { p.m = p.m.Refresh(tablesSnap(n)); return p }
func (p tablesProbe) rows() []string {
	var out []string
	for _, i := range p.m.sorted {
		out = append(out, p.m.snap.Tables[i].TableName)
	}
	return out
}
func (p tablesProbe) sel() (int, int, int)        { return p.m.selected, p.m.scroll, p.m.listHeight() }
func (p tablesProbe) searchState() (bool, string) { return p.m.searching, p.m.search }
func (p tablesProbe) sortState() (int, bool)      { return int(p.m.sortField), p.m.sortDesc }
func (p tablesProbe) sortFields() int             { return int(sortFieldCount) }

type shardsProbe struct{ m ShardsModel }

func (p shardsProbe) key(k tea.KeyMsg) listProbe { p.m, _ = p.m.HandleKey(k); return p }
func (p shardsProbe) refresh(n int) listProbe    { p.m = p.m.Refresh(shardsSnap(n)); return p }
func (p shardsProbe) rows() []string {
	var out []string
	for _, i := range p.m.sorted {
		out = append(out, p.m.problemShards[i].TableName)
	}
	return out
}
func (p shardsProbe) sel() (int, int, int)        { return p.m.selected, p.m.scroll, p.m.listHeight() }
func (p shardsProbe) searchState() (bool, string) { return p.m.searching, p.m.search }
func (p shardsProbe) sortState() (int, bool)      { return int(p.m.sortField), p.m.sortDesc }
func (p shardsProbe) sortFields() int             { return int(shardSortFieldCount) }

// height 24 keeps the list shorter than 12 rows so scrolling kicks in.
func listProbes(n int) map[string]listProbe {
	return map[string]listProbe{
		"nodes":  nodesProbe{NewNodesModel(120, 24)}.refresh(n),
		"tables": tablesProbe{NewTablesModel(120, 24)}.refresh(n),
		"shards": shardsProbe{NewShardsModel(120, 24)}.refresh(n),
	}
}

func keyType(t tea.KeyType) tea.KeyMsg { return tea.KeyMsg{Type: t} }

func typeText(p listProbe, s string) listProbe {
	for _, r := range s {
		p = p.key(keyRune(r))
	}
	return p
}

func reversed(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}

// s cycles through every field: the first (name) ascending, the rest
// descending, then wraps back to name ascending.
func TestListTabs_SortCycle(t *testing.T) {
	for name, p := range listProbes(len(listNames)) {
		if f, desc := p.sortState(); f != 0 || desc {
			t.Fatalf("%s: initial sort = %d/%v, want 0/asc", name, f, desc)
		}
		if got := p.rows(); !reflect.DeepEqual(got, listNames) {
			t.Fatalf("%s: initial order = %v", name, got)
		}
		for want := 1; want < p.sortFields(); want++ {
			p = p.key(keyRune('s'))
			if f, desc := p.sortState(); f != want || !desc {
				t.Errorf("%s: after %d x s: sort = %d/%v, want %d/desc", name, want, f, desc, want)
			}
			if want == 1 && !reflect.DeepEqual(p.rows(), reversed(listNames)) {
				t.Errorf("%s: field 1 desc order = %v", name, p.rows())
			}
		}
		p = p.key(keyRune('s'))
		if f, desc := p.sortState(); f != 0 || desc {
			t.Errorf("%s: sort did not wrap to 0/asc: %d/%v", name, f, desc)
		}
	}
}

func TestListTabs_Search(t *testing.T) {
	for name, p := range listProbes(len(listNames)) {
		p = p.key(keyRune('j')).key(keyRune('j'))
		p = p.key(keyRune('/'))
		if searching, s := p.searchState(); !searching || s != "" {
			t.Fatalf("%s: / should start an empty search", name)
		}
		if sel, scroll, _ := p.sel(); sel != 0 || scroll != 0 {
			t.Errorf("%s: / should reset selection, got %d/%d", name, sel, scroll)
		}

		// Case-insensitive, and keys that are commands elsewhere are text here.
		p = typeText(p, "LI")
		if got := p.rows(); !reflect.DeepEqual(got, []string{"charlie", "juliet", "lima"}) {
			t.Errorf("%s: search LI = %v", name, got)
		}
		// Space arrives as KeySpace, not runes, and is ignored by search.
		p = p.key(keyType(tea.KeySpace))
		if _, s := p.searchState(); s != "LI" {
			t.Errorf("%s: space changed search to %q", name, s)
		}
		p = p.key(keyType(tea.KeyBackspace))
		if _, s := p.searchState(); s != "L" {
			t.Errorf("%s: backspace -> %q, want L", name, s)
		}

		// enter keeps the filter, esc (outside search) clears it.
		p = p.key(keyType(tea.KeyEnter))
		if searching, s := p.searchState(); searching || s != "L" {
			t.Errorf("%s: enter -> searching=%v search=%q, want false/L", name, searching, s)
		}
		p = p.key(keyType(tea.KeyEsc))
		if _, s := p.searchState(); s != "" || len(p.rows()) != len(listNames) {
			t.Errorf("%s: esc should clear the filter, got %q with %d rows", name, s, len(p.rows()))
		}

		// esc while typing clears and leaves search mode.
		p = typeText(p.key(keyRune('/')), "zz")
		p = p.key(keyType(tea.KeyEsc))
		if searching, s := p.searchState(); searching || s != "" || len(p.rows()) != len(listNames) {
			t.Errorf("%s: esc in search -> searching=%v search=%q rows=%d", name, searching, s, len(p.rows()))
		}
	}
}

func TestListTabs_NavigateAndClamp(t *testing.T) {
	for name, p := range listProbes(len(listNames)) {
		for i := 0; i < len(listNames)+3; i++ {
			p = p.key(keyRune('j'))
			sel, scroll, listH := p.sel()
			if sel < scroll || sel >= scroll+listH {
				t.Fatalf("%s: selected %d outside visible window [%d,%d)", name, sel, scroll, scroll+listH)
			}
		}
		if sel, scroll, listH := p.sel(); sel != len(listNames)-1 || scroll != len(listNames)-listH {
			t.Errorf("%s: bottom: selected=%d scroll=%d listH=%d", name, sel, scroll, listH)
		}
		for i := 0; i < len(listNames)+3; i++ {
			p = p.key(keyType(tea.KeyUp))
		}
		if sel, scroll, _ := p.sel(); sel != 0 || scroll != 0 {
			t.Errorf("%s: top: selected=%d scroll=%d", name, sel, scroll)
		}

		// Fewer rows on refresh pulls the selection onto the last one.
		for i := 0; i < 8; i++ {
			p = p.key(keyRune('j'))
		}
		p = p.refresh(3)
		if sel, scroll, _ := p.sel(); sel != 2 || scroll != 0 {
			t.Errorf("%s: after shrinking to 3 rows: selected=%d scroll=%d", name, sel, scroll)
		}
	}
}

func TestNodesTab_DetailScroll(t *testing.T) {
	m := NewNodesModel(120, 24).Refresh(nodesSnap(3))
	m, _ = m.HandleKey(keyType(tea.KeyPgDown))
	if m.detailScroll != detailScrollStep {
		t.Fatalf("pgdn: detailScroll=%d, want %d", m.detailScroll, detailScrollStep)
	}
	m, _ = m.HandleKey(keyRune('j'))
	if m.detailScroll != 0 {
		t.Errorf("moving to another node should reset detailScroll, got %d", m.detailScroll)
	}
	m, _ = m.HandleKey(keyType(tea.KeyPgUp))
	if m.detailScroll != 0 {
		t.Errorf("pgup below 0: detailScroll=%d", m.detailScroll)
	}
}

func TestShardsTab_ProblemShardsOnly(t *testing.T) {
	m := NewShardsModel(120, 24).Refresh(shardsSnap(3))
	if m.countStarted != 1 || m.buckets[bucketPrimaryUnassigned] != 3 || len(m.sorted) != 3 {
		t.Fatalf("started=%d unassigned=%d listed=%d", m.countStarted, m.buckets[bucketPrimaryUnassigned], len(m.sorted))
	}
	m.selected = 2
	m = m.Refresh(shardsSnap(0))
	if m.selected != 0 {
		t.Errorf("empty list should reset selection, got %d", m.selected)
	}
}

func TestTablesTab_UnhealthyFilter(t *testing.T) {
	snap := tablesSnap(3)
	snap.TableHealth = []cratedb.TableHealth{
		{TableSchema: "doc", TableName: "alpha", Health: "GREEN"},
		{TableSchema: "doc", TableName: "bravo", Health: "GREEN"},
		{TableSchema: "doc", TableName: "bravo", Health: "YELLOW"}, // worst partition wins
	}
	p := tablesProbe{NewTablesModel(120, 24).Refresh(snap)}
	p.m, _ = p.m.HandleKey(keyRune('f'))
	if got := p.rows(); !reflect.DeepEqual(got, []string{"bravo"}) {
		t.Errorf("unhealthy filter = %v, want [bravo]", got)
	}
	p.m, _ = p.m.HandleKey(keyRune('f'))
	if got := p.rows(); len(got) != 3 {
		t.Errorf("second f should show all, got %v", got)
	}
}

// Views must render for empty and populated lists at a narrow width without
// panicking; guards the render paths the refactor moves around.
func TestListTabs_ViewSmoke(t *testing.T) {
	for _, n := range []int{0, len(listNames)} {
		views := map[string]string{
			"nodes":  NewNodesModel(60, 24).Refresh(nodesSnap(n)).View(),
			"tables": NewTablesModel(60, 24).Refresh(tablesSnap(n)).View(),
			"shards": NewShardsModel(60, 24).Refresh(shardsSnap(n)).View(),
		}
		for name, v := range views {
			if strings.TrimSpace(v) == "" {
				t.Errorf("%s with %d rows rendered nothing", name, n)
			}
		}
	}
}
