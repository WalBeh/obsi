package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestAlertsModalAndBanner(t *testing.T) {
	a := newTestApp(t)
	a.store.UpdateClusterHealth(nil, []cratedb.TableHealth{{TableSchema: "doc", TableName: "t", Health: "RED", MissingShards: 1}})
	a.alerts = a.store.Snapshot(1, store.SnapshotHint{}).Alerts

	if v := a.View(); !strings.Contains(v, "table doc.t RED") {
		t.Fatalf("banner missing from view:\n%s", v)
	}

	a.Update(keyRune('a'))
	if !a.showAlerts || !strings.Contains(a.View(), "Alerts: 1 firing") {
		t.Fatal("a did not open the alerts modal")
	}
	a.Update(keyRune('2'))
	if a.activeTab != TabOverview {
		t.Error("modal let a tab key through")
	}
	a.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if a.showAlerts {
		t.Error("esc did not close the modal")
	}
}

func TestAlertRowsCleared(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	st := store.AlertsState{History: []store.Alert{
		{Message: "node b left the cluster", Raised: now, Cleared: now.Add(90 * time.Second)},
	}}
	rows := alertRows(st, 100)
	if len(rows) != 1 || !strings.Contains(rows[0], "cleared after 1m30s") {
		t.Errorf("rows = %q", rows)
	}
}

func TestRenderSnapshots(t *testing.T) {
	m := NewOverviewModel(120, 40, true)
	if !strings.Contains(m.renderSnapshots(), "loading") {
		t.Error("want loading before the first poll")
	}
	m.snap.Snapshots = store.SnapshotsState{UpdatedAt: time.Now()}
	if !strings.Contains(m.renderSnapshots(), "no snapshot repository configured") {
		t.Error("want the no-repository line")
	}
	start := time.Date(2026, 9, 24, 3, 0, 0, 0, time.Local)
	m.snap.Snapshots = store.SnapshotsState{UpdatedAt: time.Now(), Repositories: 1, Snapshots: []cratedb.SnapshotInfo{
		{Repository: "backups", Name: "nightly", State: "PARTIAL", Started: start, Finished: start.Add(5 * time.Minute), Failures: 3},
	}}
	out := m.renderSnapshots()
	for _, want := range []string{"2026-09-24 03:00", "backups", "nightly", "PARTIAL", "5m", "3"} {
		if !strings.Contains(out, want) {
			t.Errorf("section missing %q:\n%s", want, out)
		}
	}
}
