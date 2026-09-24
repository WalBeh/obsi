package store

import (
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// JobsLogMode selects what the jobs_log collector fetches for the slowest
// board.
type JobsLogMode int

const (
	JobsLogSlowest JobsLogMode = iota // slowest finished jobs, merged into the board
	JobsLogFailed                     // jobs with error set, newest first
	JobsLogGrouped                    // per exact stmt: count / max / avg
)

// JobsLogState is what the last sys.jobs_log polls returned. Each mode keeps
// its own rows so flipping between modes doesn't blank the view while the
// next poll is in flight. Err is set when the last poll couldn't use
// sys.jobs_log (query failed, stats disabled); the board then falls back to
// sampling.
type JobsLogState struct {
	Slowest   []cratedb.JobLogEntry
	Failed    []cratedb.JobLogEntry
	Groups    []cratedb.JobLogGroup
	Coverage  []cratedb.JobLogCoverage
	Err       string
	UpdatedAt time.Time
}

// Usable reports whether the last poll succeeded.
func (st JobsLogState) Usable() bool {
	return st.Err == "" && !st.UpdatedAt.IsZero()
}

// CoveredSince is the oldest point every node's log still reaches. The node
// that retains least bounds what can be seen exactly. Zero when no node
// returned entries.
func (st JobsLogState) CoveredSince() time.Time {
	var since time.Time
	for _, c := range st.Coverage {
		if c.Oldest.After(since) {
			since = c.Oldest
		}
	}
	return since
}

// ObservedSince is when the store started watching, i.e. obsi's start. The
// jobs_log collector uses it as the lower bound for ended.
func (s *Store) ObservedSince() time.Time {
	return s.observedSince
}

// UpdateJobsLog records a successful poll. Only mode's rows are replaced;
// coverage is refreshed on every poll.
func (s *Store) UpdateJobsLog(mode JobsLogMode, entries []cratedb.JobLogEntry, groups []cratedb.JobLogGroup, coverage []cratedb.JobLogCoverage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch mode {
	case JobsLogSlowest:
		s.jobsLog.Slowest = entries
	case JobsLogFailed:
		s.jobsLog.Failed = entries
	case JobsLogGrouped:
		s.jobsLog.Groups = groups
	}
	s.jobsLog.Coverage = coverage
	s.jobsLog.Err = ""
	s.jobsLog.UpdatedAt = time.Now()
}

// SetJobsLogError records why sys.jobs_log can't be used right now. Rows
// from earlier polls are kept but ignored until a poll succeeds again.
func (s *Store) SetJobsLogError(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobsLog.Err = reason
	s.jobsLog.UpdatedAt = time.Now()
}

func (st JobsLogState) copy() JobsLogState {
	st.Slowest = copySlice(st.Slowest)
	st.Failed = copySlice(st.Failed)
	st.Groups = copySlice(st.Groups)
	st.Coverage = copySlice(st.Coverage)
	return st
}
