package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func snapshotsSnap(now time.Time) store.StoreSnapshot {
	started := now.Add(-2 * time.Hour)
	return store.StoreSnapshot{
		Nodes: []store.NodeSnapshot{{NodeInfo: cratedb.NodeInfo{ID: "n2id", Name: "bak2"}}},
		Snapshots: store.SnapshotsState{
			UpdatedAt: now, ListedAt: now.Add(-3 * time.Minute), ListTook: 400 * time.Millisecond, ListEvery: 15 * time.Minute,
			Repositories: []cratedb.Repository{
				{Name: "lab_fs", Type: "fs", Settings: map[string]string{"location": "/repo/lab_fs", "readonly": "true"}},
				{Name: "lab_s3", Type: "s3", Settings: map[string]string{"bucket": "b", "access_key": "[xxxxx]", "secret_key": "[xxxxx]"}},
				{Name: "lab_empty", Type: "fs", Settings: map[string]string{"location": "/repo/e"}},
				{Name: "lab_new", Type: "fs"},
			},
			Restores: []cratedb.SnapshotRestore{{Repository: "lab_fs", Name: "nightly", State: "STARTED", Shards: 10, Done: 3, Failed: 1}},
			Snapshots: []cratedb.SnapshotInfo{
				{Repository: "lab_s3", Name: "running", State: "IN_PROGRESS", Started: now.Add(-time.Minute), TotalShards: 4},
				{Repository: "lab_new", Name: "first", State: "IN_PROGRESS", Started: now.Add(-time.Minute)},
				{Repository: "lab_fs", Name: "nightly", State: "PARTIAL", Started: started, Finished: started.Add(5 * time.Minute),
					Version: "6.4.5", GlobalState: true, TotalShards: 10, Tables: []string{"doc.t1", "doc.parted"},
					Partitions: []cratedb.SnapshotPartition{{Table: "doc.parted", Values: []string{"d1"}}},
					Failures: []string{
						"SnapshotShardFailure{shardId=[t1/W][3], reason='node shutdown', nodeId='n2id'}",
						"SnapshotShardFailure{shardId=[.partitioned.parted.041m8c8/I][1], reason='node shutdown', nodeId='gone'}",
					}},
				{Repository: "lab_s3", Name: "t1_only", State: "SUCCESS", Started: started, Finished: started.Add(time.Second), TotalShards: 4, Tables: []string{"doc.t1"}},
				{Repository: "lab_fs", Name: "lost", State: "FAILED", Reason: "can't read"},
			},
		},
	}
}

func snapshotsModel(t *testing.T) SnapshotsModel {
	t.Helper()
	m := NewSnapshotsModel(160, 80)
	return m.Refresh(snapshotsSnap(time.Now()))
}

func pressSnap(m SnapshotsModel, keys ...string) SnapshotsModel {
	for _, k := range keys {
		var msg tea.KeyMsg
		switch k {
		case "down":
			msg = tea.KeyMsg{Type: tea.KeyDown}
		case "up":
			msg = tea.KeyMsg{Type: tea.KeyUp}
		case "enter":
			msg = tea.KeyMsg{Type: tea.KeyEnter}
		case "esc":
			msg = tea.KeyMsg{Type: tea.KeyEsc}
		case "backspace":
			msg = tea.KeyMsg{Type: tea.KeyBackspace}
		default:
			msg = tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(k)}
		}
		m, _ = m.HandleKey(msg)
	}
	return m
}

// selectRow puts the cursor on a repository's row (snap "") or on one of
// its snapshots.
func selectRow(t *testing.T, m SnapshotsModel, repo, snap string) SnapshotsModel {
	t.Helper()
	for i, r := range m.rows() {
		if r.repo.Name == repo && ((snap == "" && r.snap == nil) || (r.snap != nil && r.snap.Name == snap)) {
			m.selected = i
			return m.clampScroll().followSelection()
		}
	}
	t.Fatalf("no row %s/%s", repo, snap)
	return m
}

func TestSnapshotsView(t *testing.T) {
	m := snapshotsModel(t)
	out := m.View()
	flat := strings.Join(strings.Fields(out), " ")
	for _, want := range []string{
		"4 repositories · newest snapshot", "▲ 1 PARTIAL", "◐ 2 running", "listed 3m ago · R lists now",
		"lab_fs fs · read-only · /repo/lab_fs", "lab_s3 s3 · writable · b",
		"nightly ▲ PARTIAL 2h ago 5m 10 2", "t1_only ● SUCCESS", "running ◐ running",
		"lab_empty fs · writable · /repo/e none listed (empty, or CrateDB couldn't read it)",
		"lab_fs.nightly STARTED 3/10 shards", "1 failed",
		"location /repo/lab_fs · readonly true", // the first repository is selected, its settings below
	} {
		if !strings.Contains(flat, want) {
			t.Errorf("view missing %q:\n%s", want, out)
		}
	}
	// Under each repository newest first; the unreadable one without times last.
	if i, j := strings.Index(out, "nightly"), strings.Index(out, "lost"); i < 0 || j < i {
		t.Errorf("snapshot order:\n%s", out)
	}
	if strings.Contains(out, "── Repositories") || strings.Contains(out, "── Snapshots") {
		t.Errorf("old section rules:\n%s", out)
	}
}

func TestSnapshotsDetails(t *testing.T) {
	m := snapshotsModel(t)
	if out := selectRow(t, m, "lab_s3", "").View(); !strings.Contains(out, "[xxxxx]: CrateDB doesn't show credentials") {
		t.Errorf("s3 repository detail:\n%s", out)
	}
	out := selectRow(t, m, "lab_fs", "nightly").View()
	for _, want := range []string{
		"lab_fs.nightly", "written by CrateDB 6.4.5", "with cluster metadata",
		"tables: doc.t1, doc.parted (1 partition: [d1])",
		"2 of 10 shards failed:",
		"doc.t1 shard 3 on bak2: node shutdown",
		"doc.parted partition 041m8c8 shard 1 on gone: node shutdown",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("snapshot detail missing %q:\n%s", want, out)
		}
	}
	if out := selectRow(t, m, "lab_fs", "lost").View(); !strings.Contains(out, "couldn't read this snapshot's details") || !strings.Contains(out, "reason: can't read") {
		t.Errorf("unreadable snapshot detail:\n%s", out)
	}
}

func TestSnapshotsFilter(t *testing.T) {
	m := snapshotsModel(t)
	m = pressSnap(m, "/", "t", "1", "enter")
	if n := len(m.shown()); n != 1 || m.inputMode() {
		t.Fatalf("filter t1: %d shown, input mode %v", n, m.inputMode())
	}
	if rows := m.rows(); len(rows) != 2 || rows[0].repo.Name != "lab_s3" {
		t.Errorf("filter keeps repositories with matches only: %+v", rows)
	}
	if m = pressSnap(m, "esc"); len(m.shown()) != 5 {
		t.Errorf("esc should clear the filter, %d shown", len(m.shown()))
	}
}

func TestSnapshotsNoRepository(t *testing.T) {
	m := NewSnapshotsModel(120, 30).Refresh(store.StoreSnapshot{Snapshots: store.SnapshotsState{UpdatedAt: time.Now()}})
	if out := m.View(); !strings.Contains(out, "No snapshot repository on this cluster") {
		t.Errorf("view:\n%s", out)
	}
	m = m.Refresh(store.StoreSnapshot{Snapshots: store.SnapshotsState{UpdatedAt: time.Now(),
		Repositories: []cratedb.Repository{{Name: "r", Type: "fs"}}, ListErr: `{"error":{"message":"RepositoryException[boom]"}}`}})
	if out := m.View(); !strings.Contains(out, "couldn't read sys.snapshots: RepositoryException[boom]") || !strings.Contains(out, "not listed yet") {
		t.Errorf("view:\n%s", out)
	}
}

// On a short screen the selected row stays in the list and its details
// in the pane below.
func TestSnapshotsFollowSelection(t *testing.T) {
	m := selectRow(t, NewSnapshotsModel(160, 16).Refresh(snapshotsSnap(time.Now())), "lab_fs", "nightly")
	if out := m.View(); !strings.Contains(out, "node shutdown") || !strings.Contains(out, "▸ ") {
		t.Errorf("selected snapshot or its failures off screen:\n%s", out)
	}
}

func TestShortDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		400 * time.Millisecond:         "400ms",
		10 * time.Second:               "10s",
		30 * time.Second:               "30s",
		70 * time.Second:               "1m10s",
		5*time.Minute + 45*time.Second: "5m45s",
		15 * time.Minute:               "15m",
		time.Hour:                      "1h",
		2*time.Hour + 5*time.Minute + 3*time.Second: "2h5m",
	} {
		if got := snapshotTook(d); got != want {
			t.Errorf("snapshotTook(%v) = %q, want %q", d, got, want)
		}
	}
}

// enter folds a repository's snapshots into its row and opens them again;
// on a snapshot it folds the repository it's in. A filter shows matches in
// folded repositories too.
func TestSnapshotsFold(t *testing.T) {
	m := selectRow(t, snapshotsModel(t), "lab_s3", "")
	m = pressSnap(m, "enter")
	flat := strings.Join(strings.Fields(m.View()), " ")
	if !strings.Contains(flat, "lab_s3 s3 · writable · b +2 snapshots folded") || strings.Contains(flat, "t1_only") {
		t.Errorf("lab_s3 not folded:\n%s", m.View())
	}
	if r := m.rows()[m.selected]; r.snap != nil || r.repo.Name != "lab_s3" {
		t.Errorf("cursor left the folded repository: %+v", r)
	}
	m = pressSnap(m, "enter")
	if out := m.View(); !strings.Contains(out, "t1_only") || strings.Contains(out, "folded") {
		t.Errorf("lab_s3 not opened again:\n%s", out)
	}
	// From a snapshot row: fold its repository, cursor on the repository.
	m = pressSnap(selectRow(t, m, "lab_fs", "nightly"), "enter")
	for _, r := range m.rows() {
		if r.snap != nil && r.repo.Name == "lab_fs" {
			t.Errorf("lab_fs still lists %s after enter on nightly", r.snap.Name)
		}
	}
	if r := m.rows()[m.selected]; r.snap != nil || r.repo.Name != "lab_fs" {
		t.Errorf("enter on nightly didn't fold lab_fs:\n%s", m.View())
	}
	// A filter finds snapshots in folded repositories.
	m = pressSnap(m, "/", "nightly", "enter")
	if !strings.Contains(m.View(), "nightly") {
		t.Errorf("filter hides a folded repository's match:\n%s", m.View())
	}
	if strings.Contains(m.View(), `""`) {
		t.Errorf("empty quotes on the filter line:\n%s", m.View())
	}
}

func TestSnapshotsFilterBackspaceRune(t *testing.T) {
	m := pressSnap(snapshotsModel(t), "/", "lab", "ä", "backspace")
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	if m.search != "la" {
		t.Errorf("search = %q, want la", m.search)
	}
}

// Walking down a long list keeps the selected row on screen, also with
// "more" markers above and below, and its details in the pane.
func TestSnapshotsFollowSelectionKeepsDetails(t *testing.T) {
	now := time.Now()
	st := store.SnapshotsState{UpdatedAt: now, ListedAt: now, Repositories: []cratedb.Repository{{Name: "r"}}}
	for i := range 30 {
		st.Snapshots = append(st.Snapshots, cratedb.SnapshotInfo{Repository: "r", Name: fmt.Sprintf("s%02d", i), State: "SUCCESS",
			Started: now.Add(-time.Duration(i) * time.Hour), Finished: now.Add(-time.Duration(i)*time.Hour + time.Minute),
			Version: "6.4.5", TotalShards: 2, Tables: []string{fmt.Sprintf("doc.last%02d", i)}})
	}
	m := NewSnapshotsModel(160, 12).Refresh(store.StoreSnapshot{Snapshots: st})
	for i := range 30 {
		m = pressSnap(m, "down")
		out := m.View()
		if !strings.Contains(out, fmt.Sprintf("tables: doc.last%02d", i)) || !strings.Contains(out, fmt.Sprintf("s%02d", i)) || !strings.Contains(out, "▸ ") {
			t.Fatalf("after %d downs the selected snapshot or its tables line is off screen:\n%s", i+1, out)
		}
	}
}
