package store

import (
	"sort"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// StuckThreshold is the age beyond which a sys.jobs row is treated as an
// abandoned cursor / runaway query. Shared by the live list (hidden by
// default) and the slowest-queries board (always excluded, a WITH HOLD
// cursor open for days would own every slot).
const StuckThreshold = 24 * time.Hour

// slowestLimit caps the slowest-queries board and the finished entries kept
// in memory to back it.
const slowestLimit = 20

// ObservedQuery is a sys.jobs row as last seen by the queries collector.
// sys.jobs only lists running jobs, so a job that vanishes between polls is
// assumed finished (or killed, indistinguishable) and its duration is
// LastSeen - Started: a lower bound, short by up to one poll interval.
type ObservedQuery struct {
	cratedb.ActiveQuery
	LastSeen  time.Time
	PeakBytes int64 // max UsedBytes across all polls that saw the job
	Done      bool
}

// Duration is the observed runtime: up to LastSeen for finished jobs, up to
// now for running ones.
func (o ObservedQuery) Duration(now time.Time) time.Duration {
	if o.Done {
		return o.LastSeen.Sub(o.Started)
	}
	return now.Sub(o.Started)
}

// observeQueries folds one successful sys.jobs poll into the in-flight set
// and the finished top-N. Caller holds s.mu.
func (s *Store) observeQueries(queries []cratedb.ActiveQuery, now time.Time) {
	present := make(map[string]struct{}, len(queries))
	for _, q := range queries {
		present[q.ID] = struct{}{}
		if q.ID == "" || now.Sub(q.Started) > StuckThreshold {
			continue
		}
		o, ok := s.inflight[q.ID]
		if !ok {
			o = &ObservedQuery{}
			s.inflight[q.ID] = o
		}
		peak := max(o.PeakBytes, q.UsedBytes)
		o.ActiveQuery = q
		o.LastSeen = now
		o.PeakBytes = peak
	}

	for id, o := range s.inflight {
		if _, ok := present[id]; ok {
			// Still running but crossed the stuck threshold since the last
			// poll: drop it, it isn't finished.
			if now.Sub(o.Started) > StuckThreshold {
				delete(s.inflight, id)
			}
			continue
		}
		delete(s.inflight, id)
		o.Done = true
		s.slowestDone = append(s.slowestDone, *o)
	}
	sortObserved(s.slowestDone, now)
	if len(s.slowestDone) > slowestLimit {
		s.slowestDone = s.slowestDone[:slowestLimit]
	}
}

// slowestSnapshot merges finished and still-running jobs into the board.
// A job running for 10m right now outranks one that finished at 4m.
// Caller holds s.mu (read).
func (s *Store) slowestSnapshot(now time.Time) []ObservedQuery {
	out := make([]ObservedQuery, 0, len(s.slowestDone)+len(s.inflight))
	out = append(out, s.slowestDone...)
	for _, o := range s.inflight {
		if now.Sub(o.Started) > StuckThreshold {
			continue
		}
		out = append(out, *o)
	}
	sortObserved(out, now)
	if len(out) > slowestLimit {
		out = out[:slowestLimit]
	}
	return out
}

// sortObserved orders by duration desc; ID breaks ties so map iteration
// order doesn't make rows swap places between ticks.
func sortObserved(qs []ObservedQuery, now time.Time) {
	sort.Slice(qs, func(i, j int) bool {
		di, dj := qs[i].Duration(now), qs[j].Duration(now)
		if di != dj {
			return di > dj
		}
		return qs[i].ID < qs[j].ID
	})
}
