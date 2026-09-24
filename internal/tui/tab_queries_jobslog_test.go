package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func jobsLogSnap() store.StoreSnapshot {
	t0 := time.Now().Add(-time.Minute)
	return store.StoreSnapshot{
		ObservedSince:  t0.Add(-time.Hour),
		SampleInterval: 2 * time.Second,
		SlowestQueries: []store.ObservedQuery{
			{ActiveQuery: cratedb.ActiveQuery{ID: "exact", Started: t0}, LastSeen: t0.Add(5381 * time.Millisecond), Done: true, Exact: true},
			{ActiveQuery: cratedb.ActiveQuery{ID: "sampled", Started: t0}, LastSeen: t0.Add(4 * time.Second), Done: true},
		},
		JobsLog: store.JobsLogState{
			Failed: []cratedb.JobLogEntry{
				{ID: "f1", Started: t0, Ended: t0.Add(time.Second), Error: "SQLParseException[line 1:8: mismatched input]", Stmt: "SELEC 1"},
				{ID: "f2", Started: t0, Ended: t0.Add(2 * time.Second), Error: "boom", Stmt: "SELECT 2"},
			},
			Groups: []cratedb.JobLogGroup{
				{Stmt: "SELECT * FROM t WHERE id = ?", Count: 7, Failed: 1, Max: 12 * time.Second, Avg: 3 * time.Second, LastEnded: t0},
			},
			Coverage:  []cratedb.JobLogCoverage{{Node: "n1", Oldest: t0.Add(-7 * time.Minute)}},
			UpdatedAt: time.Now(),
		},
	}
}

func TestQueriesJobsLogModes(t *testing.T) {
	m := NewQueriesModel(160, 60).Refresh(jobsLogSnap())
	m, _ = m.HandleKey(keyRune('S'))

	v := m.View()
	for _, want := range []string{"exact from sys.jobs_log", "log reaches back 8m", "≥4.0s", "5.4s"} {
		if !strings.Contains(v, want) {
			t.Errorf("board missing %q\n%s", want, v)
		}
	}

	m, _ = m.HandleKey(keyRune('j'))
	m, _ = m.HandleKey(keyRune('f'))
	if m.logMode != store.JobsLogFailed || m.slowSelected != 0 {
		t.Fatalf("f: mode=%d selected=%d, want failed/0", m.logMode, m.slowSelected)
	}
	if v := m.View(); !strings.Contains(v, "Failed Queries") || !strings.Contains(v, "mismatched input") {
		t.Errorf("failed view:\n%s", v)
	}
	m, _ = m.HandleKey(keyRune('j'))
	if m.slowSelected != 1 {
		t.Errorf("j in failed view: selected=%d, want 1", m.slowSelected)
	}

	m, _ = m.HandleKey(keyRune('g'))
	if m.logMode != store.JobsLogGrouped {
		t.Fatal("g did not switch to grouped")
	}
	if v := m.View(); !strings.Contains(v, "Slowest Statements") || !strings.Contains(v, "12.0s") {
		t.Errorf("grouped view:\n%s", v)
	}
	if _, cmd := m.HandleKey(keyRune('y')); cmd == nil {
		t.Error("y on a group should yank")
	}

	m, _ = m.HandleKey(keyRune('g'))
	if m.logMode != store.JobsLogSlowest {
		t.Error("second g should return to the board")
	}
}

func TestQueriesJobsLogUnavailable(t *testing.T) {
	snap := jobsLogSnap()
	snap.JobsLog.Err = "stats.enabled = false"
	m := NewQueriesModel(160, 60).Refresh(snap)
	m, _ = m.HandleKey(keyRune('S'))
	if v := m.View(); !strings.Contains(v, "sampled every 2.0s") || !strings.Contains(v, "jobs_log unavailable: stats.enabled = false") {
		t.Errorf("board should say it's sampled and why:\n%s", v)
	}
	m, _ = m.HandleKey(keyRune('f'))
	if v := m.View(); !strings.Contains(v, "jobs_log unavailable") {
		t.Errorf("failed view should explain why it's empty:\n%s", v)
	}
}

func TestFormatJobLogDump(t *testing.T) {
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	d := formatJobLogDump(cratedb.JobLogEntry{ID: "x", Started: t0, Ended: t0.Add(1500 * time.Millisecond), Error: "boom", Stmt: "SELECT 1"})
	for _, want := range []string{"Job ID:   x", "Duration: 1.5s", "Error:    boom", "Statement:\nSELECT 1\n"} {
		if !strings.Contains(d, want) {
			t.Errorf("dump missing %q:\n%s", want, d)
		}
	}
}
