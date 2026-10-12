package store

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// ReplicaShards is what sys.shards on the subscriber says about one
// subscribed table, over its primaries (per partition).
type ReplicaShards struct {
	Shards  int
	Started int
	Docs    int64
	// SeqNo is max_seq_no+1 summed over the started primaries. It rises
	// with every replicated change; the subscriber can't know how far the
	// publisher is ahead.
	SeqNo int64
	Size  int64 // bytes, started primaries
	// CopiedBytes and CopyShardMs sum the started primaries' last recovery,
	// the initial copy until something else recovers them.
	CopiedBytes int64
	CopyShardMs int64
}

// LeaseHolder is a subscriber cluster as the publisher sees it: the
// retention leases it holds on the published primaries.
type LeaseHolder struct {
	Subscriber string // the subscriber's cluster name
	Tables     int
	On         []string // the tables, schema.table
	// Unpublished counts tables it still pulls that no publication lists
	// any more: dropping a publication doesn't stop a subscriber.
	Unpublished int
	Behind      int64 // operations not pulled yet, summed over primaries
	// Unknown counts primaries whose lease still says 0 although the shard
	// has operations: nothing pulled since the initial copy (a file copy),
	// so the lease doesn't tell where the subscriber is. Not in Behind.
	Unknown int
	// Retained sums retaining_seq_no over the Known leases (not at 0); its
	// growth is what the subscriber pulled.
	Retained int64
	Known    int
	LastSeen time.Time // newest lease renewal
	// Expires is the earliest lease expiry, Period the
	// soft_deletes.retention_lease.period of that lease's table.
	Expires time.Time
	Period  time.Duration
	// MaxSilence is the longest it went without renewing while obsi ran.
	MaxSilence time.Duration
	// PulledRate is operations/s pulled between the last two polls, -1
	// while unknown (first poll, lease set changed); Pulled the total
	// since obsi started.
	PulledRate float64
	Pulled     int64
	History    []float64 // Behind per poll, oldest first
}

// ReplicaTable is a subscribed table plus what obsi derived between polls.
type ReplicaTable struct {
	ReplicaShards
	Rate       float64   // changes/s between the last two polls, -1 while unknown
	LastChange time.Time // last poll that saw SeqNo rise
	CopySince  time.Time // first poll that saw the table initializing or copying
	// CopyFromStart is true when obsi saw the table initializing, so
	// CopySince is when the copy began rather than when obsi started.
	CopyFromStart bool
	Copy          *CopyResult // a copy obsi watched finish
	Replicated    int64       // changes since obsi started
	History       []float64   // changes/s per poll, oldest first
}

// CopyResult is a finished initial copy: Bytes over the started primaries,
// Wall from obsi's first sighting (a lower bound unless FromStart), and
// the average per-shard rate from CrateDB's own recovery times.
type CopyResult struct {
	Bytes     int64
	Wall      time.Duration
	FromStart bool
	ShardBps  float64
}

// ReplicationState is the last replication poll. Subscriptions are this
// cluster pulling, Publications and Subscribers this cluster serving.
type ReplicationState struct {
	Subscriptions []cratedb.Subscription
	Publications  []cratedb.Publication
	Replicas      map[string]ReplicaTable // schema.table
	Subscribers   []LeaseHolder
	// Settings are the replication.logical.* cluster settings and
	// node_initial_primaries_recoveries, by dotted name.
	Settings map[string]string
	// LeasePeriods is soft_deletes.retention_lease.period of the published
	// tables read so far (12h when the table doesn't set it).
	LeasePeriods map[string]time.Duration
	// HistorySpan is the time the last HistoryLen polls cover; a History
	// series can be shorter, when its table or subscriber came later.
	HistorySpan time.Duration
	HistoryLen  int
	// Errs has the parts whose query failed on the last poll (see
	// ReplicationPoll); those show what an earlier poll had.
	Errs map[string]string
	// User is who obsi connects as. Unless Superuser, CrateDB lists only
	// the publications and subscriptions User owns.
	User      string
	Superuser bool
	UpdatedAt time.Time
}

// ReplicationPoll is one replication poll. A part whose query failed has
// its error in Errs under "subscriptions", "publications", "shards",
// "leases" or "settings", and keeps what the store had.
type ReplicationPoll struct {
	Subscriptions []cratedb.Subscription
	Publications  []cratedb.Publication
	Shards        map[string]ReplicaShards // schema.table
	Holders       []LeaseHolder
	Settings      map[string]string
	LeasePeriods  map[string]time.Duration
	Errs          map[string]string
	User          string
	Superuser     bool
}

func (r ReplicationState) copy() ReplicationState {
	r.Subscriptions = copySlice(r.Subscriptions)
	r.Publications = copySlice(r.Publications)
	r.Replicas = maps.Clone(r.Replicas) // History slices are rebuilt per poll, never mutated
	r.Subscribers = copySlice(r.Subscribers)
	r.Settings = maps.Clone(r.Settings)
	r.Errs = maps.Clone(r.Errs)
	r.LeasePeriods = maps.Clone(r.LeasePeriods)
	return r
}

// UpdateReplication stores a replication poll.
func (s *Store) UpdateReplication(p ReplicationPoll) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	prev := s.replication
	failed := func(part string) bool { return p.Errs[part] != "" }
	if failed("subscriptions") {
		p.Subscriptions = prev.Subscriptions
	}
	if failed("publications") {
		p.Publications = prev.Publications
	}
	if failed("settings") {
		p.Settings, p.LeasePeriods = prev.Settings, prev.LeasePeriods
	}
	if failed("shards") {
		p.Shards = map[string]ReplicaShards{}
		for fqn, r := range prev.Replicas {
			p.Shards[fqn] = r.ReplicaShards
		}
	}
	subs, pubs, shards, holders := p.Subscriptions, p.Publications, p.Shards, p.Holders
	replicas := make(map[string]ReplicaTable)
	live := map[string]bool{} // series updated this poll; the rest go
	for _, sub := range subs {
		for _, t := range sub.Tables {
			fqn := t.FQN()
			r := ReplicaTable{ReplicaShards: shards[fqn], Rate: -1}
			old, seen := prev.Replicas[fqn]
			if seen {
				r.LastChange, r.CopySince, r.CopyFromStart = old.LastChange, old.CopySince, old.CopyFromStart
				r.Copy, r.Replicated = old.Copy, old.Replicated
			}
			copying := t.State == "i" || t.State == "d"
			switch {
			case copying && r.CopySince.IsZero():
				r.CopySince, r.CopyFromStart = now, t.State == "i"
			case !copying && !r.CopySince.IsZero():
				if t.State == "r" && r.CopiedBytes > 0 {
					r.Copy = &CopyResult{Bytes: r.CopiedBytes, Wall: now.Sub(r.CopySince), FromStart: r.CopyFromStart}
					if r.CopyShardMs > 0 {
						r.Copy.ShardBps = float64(r.CopiedBytes) / (float64(r.CopyShardMs) / 1000)
					}
				}
				r.CopySince, r.CopyFromStart = time.Time{}, false
			}
			complete := r.Shards > 0 && r.Started == r.Shards
			if seen && complete && old.Shards == r.Shards && old.Started == r.Shards && !failed("shards") {
				if d := r.SeqNo - old.SeqNo; d >= 0 {
					r.Rate = float64(d) / now.Sub(prev.UpdatedAt).Seconds()
					r.Replicated += d
					if d > 0 {
						r.LastChange = now
					}
				}
			}
			r.History = s.replHistory(live, "table/"+fqn, max(r.Rate, 0))
			replicas[fqn] = r
		}
	}
	before := map[string]LeaseHolder{}
	for _, h := range prev.Subscribers {
		before[h.Subscriber] = h
	}
	for i := range holders {
		h := &holders[i]
		old, seen := before[h.Subscriber]
		h.MaxSilence = max(old.MaxSilence, now.Sub(h.LastSeen))
		h.Pulled, h.PulledRate = old.Pulled, -1
		// A lease leaving 0 or a shard coming or going isn't pulling.
		if seen && old.Known == h.Known && slices.Equal(old.On, h.On) {
			if d := h.Retained - old.Retained; d >= 0 {
				h.PulledRate = float64(d) / now.Sub(prev.UpdatedAt).Seconds()
				h.Pulled += d
			}
		}
		h.History = s.replHistory(live, "sub/"+h.Subscriber, float64(h.Behind))
	}
	if failed("leases") {
		holders = prev.Subscribers
		for _, h := range holders {
			live["sub/"+h.Subscriber] = true
		}
	}
	if s.replTimes == nil {
		s.replTimes = NewRingBuf[time.Time](max(s.sparklineSize, 1))
	}
	s.replTimes.Push(now)
	for k := range s.replSeries {
		if !live[k] {
			delete(s.replSeries, k)
		}
	}
	times := s.replTimes.Slice()
	s.replication = ReplicationState{Subscriptions: subs, Publications: pubs, Replicas: replicas, Subscribers: holders,
		Settings: p.Settings, LeasePeriods: p.LeasePeriods, HistorySpan: now.Sub(times[0]), HistoryLen: len(times),
		Errs: p.Errs, User: p.User, Superuser: p.Superuser, UpdatedAt: now}
	s.lastUpdated["replication"] = now
	s.alerts.sync("replication", now, replicationAlerts(subs, holders, now))
}

// replHistory pushes v onto the series key, marks it live and returns it,
// oldest first. Caller holds s.mu.
func (s *Store) replHistory(live map[string]bool, key string, v float64) []float64 {
	live[key] = true
	if s.replSeries == nil {
		s.replSeries = map[string]*RingBuf[float64]{}
	}
	rb := s.replSeries[key]
	if rb == nil {
		rb = NewRingBuf[float64](max(s.sparklineSize, 1))
		s.replSeries[key] = rb
	}
	rb.Push(v)
	return rb.Slice()
}

// replicationAlerts raises per failed subscribed table, and per subscriber
// that hasn't renewed its leases for half their period: once a lease
// expires the publisher may discard operations the subscriber still needs.
func replicationAlerts(subs []cratedb.Subscription, holders []LeaseHolder, now time.Time) []Alert {
	var out []Alert
	for _, sub := range subs {
		for _, t := range sub.Tables {
			if t.State != "e" {
				continue
			}
			msg := fmt.Sprintf("replication of %s failed (subscription %s)", t.FQN(), sub.Name)
			if r := firstLine(t.Reason); r != "" {
				msg += ": " + r
			}
			out = append(out, Alert{Key: "failed/" + sub.Name + "/" + t.FQN(), Level: AlertCrit, Message: msg})
		}
	}
	for _, h := range holders {
		if h.Period > 0 && now.After(h.Expires.Add(-h.Period/2)) {
			out = append(out, Alert{Key: "lease/" + h.Subscriber, Level: AlertWarn,
				Message: fmt.Sprintf("subscriber %s hasn't pulled for half its retention lease period (%s)", h.Subscriber, h.Period)})
		}
	}
	sortAlerts(out)
	return out
}
