package store

import (
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// ChangesLimit caps the changes board. Oldest entries go first.
const ChangesLimit = 500

// SettingDiff is one setting whose value differs between two reads.
// Empty Old or New means the setting wasn't set on that read.
type SettingDiff struct {
	Key, Old, New string
}

// Change is a configuration statement from sys.jobs_log, or a setting that
// changed without a statement in the log (Unseen, Stmt empty).
type Change struct {
	ID       string
	Node     string
	Username string
	Stmt     string // redacted, ChangeTag stripped
	Error    string
	Kind     cratedb.ChangeKind
	Ended    time.Time
	Obsi     bool // run by obsi (settings editor, shard fix, throttle)
	Unseen   bool
	Diffs    []SettingDiff
}

// ChangeGap is a stretch of one node's sys.jobs_log that rotated away
// between two polls, so changes in it may be missing.
type ChangeGap struct {
	Node     string
	From, To time.Time
}

// ChangesState is the changes board. Changes are newest first. LogStart is
// how far back the log reached on the first poll, i.e. where the backfill
// begins.
type ChangesState struct {
	Changes   []Change
	Gaps      []ChangeGap
	LogStart  time.Time
	Err       string
	UpdatedAt time.Time
}

// Usable reports whether the last poll succeeded.
func (st ChangesState) Usable() bool {
	return st.Err == "" && !st.UpdatedAt.IsZero()
}

// AddChanges records a successful poll. changes may come in any order.
func (s *Store) AddChanges(changes []Change, gaps []ChangeGap, logStart time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changes.LogStart.IsZero() {
		s.changes.LogStart = logStart
	}
	for _, c := range changes {
		i := len(s.changes.Changes)
		for i > 0 && s.changes.Changes[i-1].Ended.Before(c.Ended) {
			i--
		}
		s.changes.Changes = append(s.changes.Changes, Change{})
		copy(s.changes.Changes[i+1:], s.changes.Changes[i:])
		s.changes.Changes[i] = c
	}
	if len(s.changes.Changes) > ChangesLimit {
		s.changes.Changes = s.changes.Changes[:ChangesLimit]
	}
	s.changes.Gaps = append(s.changes.Gaps, gaps...)
	s.changes.Err = ""
	s.changes.UpdatedAt = time.Now()
}

// SetChangesError records why the changes poll failed. Earlier changes stay.
func (s *Store) SetChangesError(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes.Err = reason
	s.changes.UpdatedAt = time.Now()
}

func (st ChangesState) copy() ChangesState {
	st.Changes = copySlice(st.Changes)
	st.Gaps = copySlice(st.Gaps)
	return st
}
