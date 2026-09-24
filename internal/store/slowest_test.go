package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func newTestStore() *Store {
	return New(10, map[string]config.CollectorConfig{
		"queries": {Enabled: true, Interval: config.Duration{Duration: 2 * time.Second}},
	})
}

func job(id string, started time.Time, used int64) cratedb.ActiveQuery {
	return cratedb.ActiveQuery{ID: id, Started: started, Stmt: "SELECT " + id, UsedBytes: used}
}

// A job that vanishes between polls is finished; its duration is frozen at
// the last poll that saw it, and peak memory survives a later drop.
func TestObserveQueries_FinalizeOnDisappearance(t *testing.T) {
	s := newTestStore()
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	a := job("a", t0, 100)

	s.observeQueries([]cratedb.ActiveQuery{a}, t0.Add(2*time.Second))
	a.UsedBytes = 500
	s.observeQueries([]cratedb.ActiveQuery{a}, t0.Add(4*time.Second))
	a.UsedBytes = 50
	s.observeQueries([]cratedb.ActiveQuery{a}, t0.Add(6*time.Second))
	s.observeQueries(nil, t0.Add(8*time.Second))

	if len(s.inflight) != 0 {
		t.Fatalf("inflight = %d, want 0", len(s.inflight))
	}
	if len(s.slowestDone) != 1 {
		t.Fatalf("slowestDone = %d, want 1", len(s.slowestDone))
	}
	got := s.slowestDone[0]
	if !got.Done {
		t.Error("want Done")
	}
	if d := got.Duration(); d != 6*time.Second {
		t.Errorf("Duration = %s, want 6s (frozen at last seen)", d)
	}
	if got.PeakBytes != 500 {
		t.Errorf("PeakBytes = %d, want 500", got.PeakBytes)
	}
}

func TestObserveQueries_CappedAtLimit(t *testing.T) {
	s := newTestStore()
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	now := t0.Add(time.Hour)

	var qs []cratedb.ActiveQuery
	for i := 0; i < SlowestLimit+5; i++ {
		qs = append(qs, job(fmt.Sprintf("j%02d", i), t0.Add(time.Duration(i)*time.Minute), 0))
	}
	s.observeQueries(qs, now)
	s.observeQueries(nil, now.Add(time.Second))

	if len(s.slowestDone) != SlowestLimit {
		t.Fatalf("slowestDone = %d, want %d", len(s.slowestDone), SlowestLimit)
	}
	// j00 started first, so it ran longest; the 5 youngest fall off.
	if s.slowestDone[0].ID != "j00" {
		t.Errorf("top = %s, want j00", s.slowestDone[0].ID)
	}
	if last := s.slowestDone[SlowestLimit-1].ID; last != "j19" {
		t.Errorf("last = %s, want j19", last)
	}
}

// Running jobs compete with finished ones on their current duration.
func TestSlowestSnapshot_MergesRunning(t *testing.T) {
	s := newTestStore()
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

	s.observeQueries([]cratedb.ActiveQuery{job("done", t0, 0), job("run", t0.Add(time.Minute), 0)}, t0.Add(4*time.Minute))
	s.observeQueries([]cratedb.ActiveQuery{job("run", t0.Add(time.Minute), 0)}, t0.Add(5*time.Minute))
	s.observeQueries([]cratedb.ActiveQuery{job("run", t0.Add(time.Minute), 0)}, t0.Add(11*time.Minute))

	// done: 4m. run last seen at t0+11m: 10m.
	got := s.slowestSnapshot(t0.Add(11 * time.Minute))
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].ID != "run" || got[0].Done {
		t.Errorf("top = %s (done=%v), want running job", got[0].ID, got[0].Done)
	}
	if got[1].ID != "done" || !got[1].Done {
		t.Errorf("second = %s (done=%v), want finished job", got[1].ID, got[1].Done)
	}
}

// Regression: running jobs used to count up to now between polls, climb to
// #1, then drop below a slower finished job (or off the board) once the next
// poll found them gone. A job's duration must not change between polls, nor
// when it's finalized.
func TestSlowestSnapshot_NoDropOnFinish(t *testing.T) {
	s := newTestStore()
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	slow := job("slow", t0, 0)
	s.observeQueries([]cratedb.ActiveQuery{slow}, t0.Add(4900*time.Millisecond))
	s.observeQueries(nil, t0.Add(6900*time.Millisecond)) // slow done at 4.9s

	late := job("late", t0.Add(10*time.Second), 0)
	s.observeQueries([]cratedb.ActiveQuery{late}, t0.Add(14*time.Second)) // late seen at 4.0s

	// 2.1s later, before the next poll: late must still read 4.0s and sit
	// below slow, not count up to 6.1s and overtake it.
	mid := s.slowestSnapshot(t0.Add(16100 * time.Millisecond))
	if mid[0].ID != "slow" || mid[1].ID != "late" {
		t.Fatalf("between polls order = %s,%s, want slow,late", mid[0].ID, mid[1].ID)
	}
	if d := mid[1].Duration(); d != 4*time.Second {
		t.Errorf("running late = %s, want 4s (as of last poll)", d)
	}

	s.observeQueries(nil, t0.Add(16*time.Second))
	after := s.slowestSnapshot(t0.Add(16 * time.Second))
	if after[1].ID != "late" || !after[1].Done || after[1].Duration() != 4*time.Second {
		t.Errorf("after finish = %+v, want late done at 4s", after[1])
	}
}

func TestObserveQueries_StuckExcluded(t *testing.T) {
	s := newTestStore()
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	stuck := job("stuck", t0.Add(-48*time.Hour), 0)
	nearly := job("nearly", t0.Add(-StuckThreshold+time.Minute), 0)

	s.observeQueries([]cratedb.ActiveQuery{stuck, nearly}, t0)
	if _, ok := s.inflight["stuck"]; ok {
		t.Error("stuck job must not be tracked")
	}
	if got := s.slowestSnapshot(t0); len(got) != 1 || got[0].ID != "nearly" {
		t.Fatalf("snapshot = %+v, want only nearly", got)
	}

	// nearly crosses the threshold while still running: dropped, not
	// recorded as finished.
	s.observeQueries([]cratedb.ActiveQuery{stuck, nearly}, t0.Add(2*time.Minute))
	if len(s.inflight) != 0 || len(s.slowestDone) != 0 {
		t.Errorf("inflight=%d done=%d, want 0/0", len(s.inflight), len(s.slowestDone))
	}

	// Zero Started (sys.jobs row without a timestamp) reads as stuck.
	s.observeQueries([]cratedb.ActiveQuery{job("nostart", time.Time{}, 0)}, t0)
	if len(s.inflight) != 0 {
		t.Error("job with zero Started must not be tracked")
	}
}

func TestSnapshot_SlowestFields(t *testing.T) {
	s := newTestStore()
	s.UpdateActiveQueries([]cratedb.ActiveQuery{job("a", time.Now().Add(-time.Second), 0)})

	snap := s.Snapshot(2, SnapshotHint{IncludeQueries: true})
	if len(snap.SlowestQueries) != 1 {
		t.Fatalf("SlowestQueries = %d, want 1", len(snap.SlowestQueries))
	}
	if snap.ObservedSince.IsZero() {
		t.Error("ObservedSince unset")
	}
	if snap.SampleInterval != 4*time.Second {
		t.Errorf("SampleInterval = %s, want 4s (2s x throttle 2)", snap.SampleInterval)
	}

	if snap := s.Snapshot(1, SnapshotHint{}); snap.SlowestQueries != nil {
		t.Error("SlowestQueries must be omitted without IncludeQueries")
	}
}
