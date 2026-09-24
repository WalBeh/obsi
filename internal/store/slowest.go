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

// SlowestLimit caps the slowest-queries board, the finished entries kept in
// memory to back it, and the rows fetched per sys.jobs_log poll.
const SlowestLimit = 20

// ObservedQuery is a sys.jobs row as last seen by the queries collector.
// sys.jobs only lists running jobs, so a job that vanishes between polls is
// assumed finished (or killed, indistinguishable).
type ObservedQuery struct {
	cratedb.ActiveQuery
	LastSeen  time.Time
	PeakBytes int64 // max UsedBytes across all polls that saw the job
	Done      bool

	// Exact is set for rows taken from sys.jobs_log: LastSeen is the real end
	// time, not the last poll. Error comes with them.
	Exact bool
	Error string
}

// Duration is the runtime as of the last poll that saw the job, running or
// not. A lower bound, short by up to one poll interval. Counting running
// jobs up to now instead made them climb the board between polls, then drop
// (often off the top N) once found finished, and credited them with time
// nobody observed while polling was throttled or failing.
func (o ObservedQuery) Duration() time.Duration {
	return o.LastSeen.Sub(o.Started)
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
	sortObserved(s.slowestDone)
	if len(s.slowestDone) > SlowestLimit {
		s.slowestDone = s.slowestDone[:SlowestLimit]
	}
}

// slowestSnapshot merges finished and still-running jobs into the board.
// A job running for 10m right now outranks one that finished at 4m.
// Finished rows come from sys.jobs_log when the last poll worked (exact
// durations); sampled rows stay in for jobs the log no longer holds, and are
// the whole board when jobs_log is unavailable. Caller holds s.mu (read).
func (s *Store) slowestSnapshot(now time.Time) []ObservedQuery {
	out := make([]ObservedQuery, 0, len(s.jobsLog.Slowest)+len(s.slowestDone)+len(s.inflight))
	exact := make(map[string]struct{}, len(s.jobsLog.Slowest))
	if s.jobsLog.Usable() {
		for _, e := range s.jobsLog.Slowest {
			o := ObservedQuery{
				ActiveQuery: cratedb.ActiveQuery{
					ID: e.ID, Node: e.Node, Started: e.Started, Stmt: e.Stmt, Username: e.Username,
				},
				LastSeen: e.Ended,
				Done:     true,
				Exact:    true,
				Error:    e.Error,
			}
			// Memory is only known if a poll caught the job running.
			if sampled, ok := s.sampledByID(e.ID); ok {
				o.PeakBytes = sampled.PeakBytes
				o.Operations = sampled.Operations
				o.UsedBytes = sampled.UsedBytes
			}
			exact[e.ID] = struct{}{}
			out = append(out, o)
		}
	}
	for _, o := range s.slowestDone {
		if _, ok := exact[o.ID]; !ok {
			out = append(out, o)
		}
	}
	for _, o := range s.inflight {
		if _, ok := exact[o.ID]; ok || now.Sub(o.Started) > StuckThreshold {
			continue
		}
		out = append(out, *o)
	}
	sortObserved(out)
	if len(out) > SlowestLimit {
		out = out[:SlowestLimit]
	}
	return out
}

func (s *Store) sampledByID(id string) (ObservedQuery, bool) {
	if o, ok := s.inflight[id]; ok {
		return *o, true
	}
	for _, o := range s.slowestDone {
		if o.ID == id {
			return o, true
		}
	}
	return ObservedQuery{}, false
}

// sortObserved orders by duration desc; ID breaks ties so map iteration
// order doesn't make rows swap places between ticks.
func sortObserved(qs []ObservedQuery) {
	sort.Slice(qs, func(i, j int) bool {
		di, dj := qs[i].Duration(), qs[j].Duration()
		if di != dj {
			return di > dj
		}
		return qs[i].ID < qs[j].ID
	})
}

// UpdateActiveQueries updates the list of active queries.
func (s *Store) UpdateActiveQueries(queries []cratedb.ActiveQuery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.activeQueries = queries
	s.observeQueries(queries, now)
	s.lastUpdated["queries"] = now
}
