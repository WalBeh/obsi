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

func TestSnapshotsView(t *testing.T) {
	m := snapshotsModel(t)
	out := m.View()
	for _, want := range []string{
		"lab_fs", "read-only", "writable", "nightly  PARTIAL", // the running one isn't the newest finished
		"t1_only  SUCCESS", "none listed", // lab_empty
		"running, none finished", // lab_new
		"lab_fs.nightly  STARTED  3/10 shards", "1 failed",
		"listed 3m ago, took 400ms · every 15m, sooner while one runs",
		"location = /repo/lab_fs", // the first repository is selected and open
	} {
		if !strings.Contains(out, want) {
			t.Errorf("view missing %q:\n%s", want, out)
		}
	}
	// Newest first, the unreadable one without times last.
	if i, j := strings.Index(out, "running"), strings.Index(out, "lost"); i < 0 || j < i {
		t.Errorf("snapshot order:\n%s", out)
	}
}

func TestSnapshotsDetails(t *testing.T) {
	m := snapshotsModel(t)
	out := pressSnap(m, "down").View()
	if !strings.Contains(out, "[xxxxx]: CrateDB doesn't show credentials") {
		t.Errorf("s3 repository detail:\n%s", out)
	}
	if out := pressSnap(m, "down", "down").View(); !strings.Contains(out, "No snapshots listed: the repository is empty, or CrateDB couldn't read it") {
		t.Errorf("empty repository detail:\n%s", out)
	}
	// Repositories, then the two running ones, then nightly.
	out = pressSnap(m, "down", "down", "down", "down", "down", "down").View()
	for _, want := range []string{
		"written by CrateDB 6.4.5", "with cluster metadata",
		"tables: doc.t1, doc.parted (1 partition: [d1])",
		"2 of 10 shards failed:",
		"doc.t1 shard 3 on bak2: node shutdown",
		"doc.parted partition 041m8c8 shard 1 on gone: node shutdown",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("snapshot detail missing %q:\n%s", want, out)
		}
	}
	if out := pressSnap(m, "down", "down", "down", "down", "down", "down", "down", "down").View(); !strings.Contains(out, "couldn't read this snapshot's details") || !strings.Contains(out, "reason: can't read") {
		t.Errorf("unreadable snapshot detail:\n%s", out)
	}
}

func TestSnapshotsFilter(t *testing.T) {
	m := snapshotsModel(t)
	m = pressSnap(m, "/", "t", "1", "enter")
	if n := len(m.shown()); n != 1 || m.inputMode() {
		t.Fatalf("filter t1: %d shown, input mode %v", n, m.inputMode())
	}
	if m = pressSnap(m, "esc"); len(m.shown()) != 5 {
		t.Errorf("esc should clear the filter, %d shown", len(m.shown()))
	}
	// enter on lab_s3 lists its snapshots and selects the first.
	m = pressSnap(m, "down", "enter")
	if n := len(m.shown()); n != 2 || m.selected != len(m.st.Repositories) {
		t.Errorf("enter on lab_s3: %d shown, selected %d", n, m.selected)
	}
	if out := m.View(); !strings.Contains(out, `filter: repository lab_s3`) {
		t.Errorf("filter line:\n%s", out)
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

// Selecting a snapshot low on a short screen scrolls its details in.
func TestSnapshotsFollowSelection(t *testing.T) {
	m := NewSnapshotsModel(160, 16).Refresh(snapshotsSnap(time.Now()))
	m = pressSnap(m, "down", "down", "down", "down", "down", "down")
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

// enter on a repository lists exactly its snapshots, not those of a
// repository whose name contains it.
func TestSnapshotsRepositoryFilterExact(t *testing.T) {
	now := time.Now()
	m := NewSnapshotsModel(160, 60).Refresh(store.StoreSnapshot{Snapshots: store.SnapshotsState{UpdatedAt: now, ListedAt: now,
		Repositories: []cratedb.Repository{{Name: "backup_s3"}, {Name: "s3"}, {Name: "x"}},
		Snapshots: []cratedb.SnapshotInfo{
			{Repository: "backup_s3", Name: "a", Started: now},
			{Repository: "s3", Name: "b", Started: now},
			{Repository: "x", Name: "data.1", Started: now},
		}}})
	m = pressSnap(m, "down", "enter")
	if got := m.shown(); len(got) != 1 || got[0].Name != "b" {
		t.Errorf("enter on s3 shows %+v", got)
	}
	m = pressSnap(m, "esc", "up", "enter") // enter selected the first snapshot; x is just above
	if got := m.shown(); len(got) != 1 || got[0].Name != "data.1" {
		t.Errorf("enter on x shows %+v", got)
	}
}

func TestSnapshotsFilterBackspaceRune(t *testing.T) {
	m := pressSnap(snapshotsModel(t), "/", "lab", "ä", "backspace")
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyBackspace})
	if m.search != "la" {
		t.Errorf("search = %q, want la", m.search)
	}
}

// Walking down the list keeps the selected snapshot's last detail line
// on screen, also with "more" markers above and below.
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
		if out := m.View(); !strings.Contains(out, fmt.Sprintf("tables: doc.last%02d", i)) {
			t.Fatalf("after %d downs the selected snapshot's tables line is off screen:\n%s", i+1, out)
		}
	}
}
