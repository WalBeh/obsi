package store

import (
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// jobs_log rows replace sampled ones for the same job (exact beats lower
// bound), sampled rows the log no longer holds stay, and memory seen while
// sampling carries over.
func TestSlowestSnapshot_JobsLogMerge(t *testing.T) {
	s := newTestStore()
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

	s.observeQueries([]cratedb.ActiveQuery{job("both", t0, 700), job("evicted", t0, 0)}, t0.Add(4*time.Second))
	s.observeQueries(nil, t0.Add(6*time.Second))
	s.observeQueries([]cratedb.ActiveQuery{job("running", t0, 0)}, t0.Add(3*time.Second))

	s.UpdateJobsLog(JobsLogSlowest, []cratedb.JobLogEntry{
		{ID: "both", Started: t0, Ended: t0.Add(5381 * time.Millisecond)},
		{ID: "short", Started: t0, Ended: t0.Add(100 * time.Millisecond), Error: "boom"},
	}, nil, []cratedb.JobLogCoverage{{Node: "n1", Oldest: t0}})

	got := s.slowestSnapshot(t0.Add(7 * time.Second))
	byID := map[string]ObservedQuery{}
	for _, o := range got {
		if _, dup := byID[o.ID]; dup {
			t.Fatalf("duplicate row for %s", o.ID)
		}
		byID[o.ID] = o
	}
	both := byID["both"]
	if !both.Exact || both.Duration() != 5381*time.Millisecond || both.PeakBytes != 700 {
		t.Errorf("both = exact %v dur %s peak %d, want exact 5.381s peak 700", both.Exact, both.Duration(), both.PeakBytes)
	}
	if e := byID["evicted"]; e.Exact || !e.Done {
		t.Errorf("evicted should stay as a sampled finished row: %+v", e)
	}
	if byID["short"].Error != "boom" {
		t.Error("error not carried onto the board")
	}
	if r, ok := byID["running"]; !ok || r.Done {
		t.Error("running job missing")
	}
	if got[0].ID != "both" {
		t.Errorf("top = %s, want both", got[0].ID)
	}
}

func TestSlowestSnapshot_JobsLogErrorFallsBack(t *testing.T) {
	s := newTestStore()
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	s.UpdateJobsLog(JobsLogSlowest, []cratedb.JobLogEntry{{ID: "x", Started: t0, Ended: t0.Add(time.Second)}}, nil, nil)
	s.SetJobsLogError("stats.enabled = false")

	if got := s.slowestSnapshot(t0); len(got) != 0 {
		t.Errorf("rows from a failed jobs_log must be ignored, got %+v", got)
	}
}

// A poll in one mode must not wipe the rows kept for another.
func TestUpdateJobsLog_PerMode(t *testing.T) {
	s := newTestStore()
	s.UpdateJobsLog(JobsLogSlowest, []cratedb.JobLogEntry{{ID: "a"}}, nil, nil)
	s.UpdateJobsLog(JobsLogGrouped, nil, []cratedb.JobLogGroup{{Stmt: "SELECT 1"}}, nil)
	if len(s.jobsLog.Slowest) != 1 || len(s.jobsLog.Groups) != 1 {
		t.Errorf("slowest=%d groups=%d, want 1/1", len(s.jobsLog.Slowest), len(s.jobsLog.Groups))
	}
}

func TestCoveredSince(t *testing.T) {
	t0 := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	st := JobsLogState{Coverage: []cratedb.JobLogCoverage{{Oldest: t0}, {Oldest: t0.Add(time.Minute)}}}
	if got := st.CoveredSince(); !got.Equal(t0.Add(time.Minute)) {
		t.Errorf("CoveredSince = %s, want the node retaining least", got)
	}
}
