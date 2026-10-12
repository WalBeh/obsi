package store

import (
	"fmt"
	"maps"
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
}

// LeaseHolder is a subscriber cluster as the publisher sees it: the
// retention leases it holds on the published primaries.
type LeaseHolder struct {
	Subscriber string // the subscriber's cluster name
	Tables     int
	// Unpublished counts tables it still pulls that no publication lists
	// any more: dropping a publication doesn't stop a subscriber.
	Unpublished int
	Behind      int64 // operations not pulled yet, summed over primaries
	// Unknown counts primaries whose lease still says 0 although the shard
	// has operations: nothing pulled since the initial copy (a file copy),
	// so the lease doesn't tell where the subscriber is. Not in Behind.
	Unknown  int
	LastSeen time.Time // newest lease renewal
	// Expires is the earliest lease expiry, Period the
	// soft_deletes.retention_lease.period of that lease's table.
	Expires time.Time
	Period  time.Duration
	// MaxSilence is the longest it went without renewing while obsi ran.
	MaxSilence time.Duration
}

// ReplicaTable is a subscribed table plus what obsi derived between polls.
type ReplicaTable struct {
	ReplicaShards
	Rate       float64   // changes/s between the last two polls, -1 while unknown
	LastChange time.Time // last poll that saw SeqNo rise
	CopySince  time.Time // first poll that saw the table initializing or copying
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
	Err          string
	UpdatedAt    time.Time
}

func (r ReplicationState) copy() ReplicationState {
	r.Subscriptions = copySlice(r.Subscriptions)
	r.Publications = copySlice(r.Publications)
	r.Replicas = maps.Clone(r.Replicas)
	r.Subscribers = copySlice(r.Subscribers)
	r.Settings = maps.Clone(r.Settings)
	r.LeasePeriods = maps.Clone(r.LeasePeriods)
	return r
}

// UpdateReplication stores a replication poll. shards is keyed by
// schema.table.
func (s *Store) UpdateReplication(subs []cratedb.Subscription, pubs []cratedb.Publication, shards map[string]ReplicaShards, holders []LeaseHolder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	prev := s.replication
	replicas := make(map[string]ReplicaTable)
	for _, sub := range subs {
		for _, t := range sub.Tables {
			fqn := t.FQN()
			r := ReplicaTable{ReplicaShards: shards[fqn], Rate: -1}
			old, seen := prev.Replicas[fqn]
			if seen {
				r.LastChange, r.CopySince = old.LastChange, old.CopySince
			}
			if t.State != "i" && t.State != "d" {
				r.CopySince = time.Time{}
			} else if r.CopySince.IsZero() {
				r.CopySince = now
			}
			complete := r.Shards > 0 && r.Started == r.Shards
			if seen && complete && old.Shards == r.Shards && old.Started == r.Shards {
				if d := r.SeqNo - old.SeqNo; d >= 0 {
					r.Rate = float64(d) / now.Sub(prev.UpdatedAt).Seconds()
					if d > 0 {
						r.LastChange = now
					}
				}
			}
			replicas[fqn] = r
		}
	}
	silence := map[string]time.Duration{}
	for _, h := range prev.Subscribers {
		silence[h.Subscriber] = h.MaxSilence
	}
	for i := range holders {
		h := &holders[i]
		h.MaxSilence = max(silence[h.Subscriber], now.Sub(h.LastSeen))
	}
	s.replication = ReplicationState{Subscriptions: subs, Publications: pubs, Replicas: replicas, Subscribers: holders,
		Settings: prev.Settings, LeasePeriods: prev.LeasePeriods, UpdatedAt: now}
	s.lastUpdated["replication"] = now
	s.alerts.sync("replication", now, replicationAlerts(subs, holders, now))
}

// UpdateReplicationSettings stores the replication settings and the
// published tables' lease periods.
func (s *Store) UpdateReplicationSettings(settings map[string]string, periods map[string]time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replication.Settings, s.replication.LeasePeriods = settings, periods
}

// SetReplicationError keeps the last poll and records why this one failed.
func (s *Store) SetReplicationError(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replication.Err = msg
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
