package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestChangesBoard(t *testing.T) {
	started := time.Now().Add(-time.Minute)
	snap := store.StoreSnapshot{ObservedSince: started}
	snap.Changes = store.ChangesState{
		UpdatedAt: time.Now(),
		LogStart:  started.Add(-time.Hour),
		Changes: []store.Change{
			{ID: "j3", Kind: cratedb.ChangeCluster, Ended: started.Add(30 * time.Second), Username: "crate", Obsi: true,
				Stmt:  `SET GLOBAL TRANSIENT "indices.recovery.max_bytes_per_sec" = ?`,
				Diffs: []store.SettingDiff{{Key: "indices.recovery.max_bytes_per_sec", Old: "40mb", New: "200mb"}}},
			{Kind: cratedb.ChangeCluster, Ended: started.Add(20 * time.Second), Unseen: true,
				Diffs: []store.SettingDiff{{Key: "cluster.routing.allocation.enable", Old: "all", New: "none"}}},
			{ID: "j1", Kind: cratedb.ChangeSecurity, Ended: started.Add(-time.Minute), Username: "admin",
				Stmt: "GRANT DQL ON TABLE doc.t TO u"},
		},
	}
	m := NewQueriesModel(160, 50).Refresh(snap)
	m, _ = m.HandleKey(keyRune('c'))
	if m.view() != queriesChanges {
		t.Fatal("c did not open the changes board")
	}
	out := m.View()
	for _, want := range []string{
		"Config Changes",
		"[40mb → 200mb]",
		"(no statement in the log)",
		"── obsi started",
		"run by obsi",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("board lacks %q:\n%s", want, out)
		}
	}
	if strings.Index(out, "obsi started") > strings.Index(out, "GRANT DQL") {
		t.Error("start marker should sit above the backfilled GRANT")
	}

	// A new change on top keeps the cursor on the same one.
	m, _ = m.HandleKey(keyRune('j'))
	snap.Changes.Changes = append([]store.Change{{ID: "j4", Kind: cratedb.ChangeTable, Ended: time.Now()}}, snap.Changes.Changes...)
	m = m.Refresh(snap)
	if m.chgSelected != 2 {
		t.Errorf("cursor at %d, want 2 (the unseen change)", m.chgSelected)
	}

	m, _ = m.HandleKey(keyRune('S'))
	if m.view() != queriesSlowest {
		t.Errorf("S from the changes board went to %v", m.view())
	}
}
