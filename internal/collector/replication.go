package collector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// pg_subscription(_rel) and pg_publication(_tables) are cluster state and
// cheap; sys.shards is only read for subscribed tables, and for leases
// while there are publications or leases, otherwise once a minute.
const (
	replSubscriptionsQuery = `SELECT s.subname, s.subconninfo, s.subpublications, r.srrelid::text, r.srsubstate, r.srsubstate_reason
FROM pg_catalog.pg_subscription s LEFT JOIN pg_catalog.pg_subscription_rel r ON r.srsubid = s.oid
ORDER BY 1, 4`
	replPublicationsQuery = `SELECT p.pubname, p.puballtables, t.schemaname, t.tablename
FROM pg_catalog.pg_publication p LEFT JOIN pg_catalog.pg_publication_tables t ON t.pubname = p.pubname
ORDER BY 1, 3, 4`
	// Shards still being copied are listed without "primary", so the
	// filter is on the STARTED row in Go.
	replicaShardsQuery = `SELECT schema_name || '.' || table_name, partition_ident, id, "primary", state, num_docs, seq_no_stats['max_seq_no'],
  size, recovery['size']['recovered'], recovery['total_time']
FROM sys.shards
WHERE schema_name || '.' || table_name = ANY(?)`
	// Not limited to published tables: a subscriber keeps pulling a table
	// after it left its publication, or the publication was dropped.
	partitionReplicasQuery = `SELECT table_schema || '.' || table_name, number_of_replicas
FROM information_schema.table_partitions
WHERE table_schema || '.' || table_name = ANY(?)`
	leasesQuery = `SELECT now(), schema_name || '.' || table_name, seq_no_stats['max_seq_no'], retention_leases['leases']
FROM sys.shards
WHERE "primary" AND state = 'STARTED'
  AND array_to_string(retention_leases['leases']['source'], ',') LIKE '%` + cratedb.LeaseSourcePrefix + `%'`
)

const leasePeriodRefresh = 10 * time.Minute

// leasesRecheck is how often leases are looked for on a cluster without
// publications or leases.
const leasesRecheck = time.Minute

// replSettingsQuery reads what the Replication tab shows and edits.
const replSettingsQuery = `SELECT settings['replication']['logical'], settings['cluster']['routing']['allocation']['node_initial_primaries_recoveries'] FROM sys.cluster`

// maxPeriodReads caps SHOW CREATE TABLE per poll, so a publication with
// many tables is read over a few polls.
const maxPeriodReads = 20

type ReplicationCollector struct {
	interval time.Duration
	tracker  *QueryTracker

	mu         sync.Mutex
	periods    map[string]leasePeriod // schema.table
	leasesSeen bool
	leasesAt   time.Time
	user       string // who obsi connects as, "" until known
	superuser  bool
	// reset asks the next Collect to forget the above. Reset only sets it:
	// it's called from the TUI, which mustn't wait for a running poll.
	reset atomic.Bool
}

// leasePeriod is a table's period as last read; d is 0 when the read
// failed, so the table waits for the next refresh like the others.
type leasePeriod struct {
	d  time.Duration
	at time.Time
}

func NewReplicationCollector(cfg config.CollectorConfig, tracker *QueryTracker) *ReplicationCollector {
	return &ReplicationCollector{interval: cfg.Interval.Duration, tracker: tracker, periods: map[string]leasePeriod{}}
}

func (c *ReplicationCollector) Name() string            { return "replication" }
func (c *ReplicationCollector) Interval() time.Duration { return c.interval }

func (c *ReplicationCollector) Reset() { c.reset.Store(true) }

func (c *ReplicationCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reset.Swap(false) {
		c.periods = map[string]leasePeriod{}
		c.leasesSeen, c.leasesAt = false, time.Time{}
		c.user = ""
	}
	if c.user == "" {
		c.whoAmI(ctx, reg)
	}
	p := store.ReplicationPoll{Errs: map[string]string{}, User: c.user, Superuser: c.superuser}
	// Each part fails on its own: a user without DQL on sys still sees
	// its subscriptions, a timeout on sys.shards keeps the rest.
	part := func(name, label, stmt string, args ...interface{}) *cratedb.SQLResponse {
		resp, err := trackedQuery(ctx, c.tracker, label, reg, stmt, args...)
		if err != nil {
			p.Errs[name] = err.Error()
			return nil
		}
		return resp
	}
	if resp := part("subscriptions", QueryReplSubscriptions, replSubscriptionsQuery); resp != nil {
		p.Subscriptions = parseSubscriptions(resp.Rows)
	}
	if resp := part("publications", QueryReplPublications, replPublicationsQuery); resp != nil {
		p.Publications = parsePublications(resp.Rows)
	}

	switch tables := subscribedTables(p.Subscriptions); {
	case p.Errs["subscriptions"] != "":
		p.Errs["shards"] = p.Errs["subscriptions"]
	case len(tables) > 0:
		if resp := part("shards", QueryReplicaShards, replicaShardsQuery, tables); resp != nil {
			p.Shards = parseReplicaShards(resp.Rows)
			if resp := part("partitions", QueryReplPartitionReplicas, partitionReplicasQuery, tables); resp != nil {
				for _, row := range resp.Rows {
					fqn := cratedb.ToString(row[0])
					if n, ok := cratedb.MinReplicas(cratedb.ToString(row[1])); ok {
						r := p.Shards[fqn]
						r.PartitionReplicas = max(r.PartitionReplicas, n)
						p.Shards[fqn] = r
					}
				}
			}
		}
	}
	// CrateDB lists publications only to their owner, superusers too: a
	// leased table no visible publication lists may be in another user's.
	var published map[string]bool
	if p.Errs["publications"] == "" {
		published = publishedTables(p.Publications)
	}
	if len(p.Publications) > 0 || c.leasesSeen || time.Since(c.leasesAt) >= leasesRecheck {
		if resp := part("leases", QueryReplLeases, leasesQuery); resp != nil {
			leased := leasedTables(resp.Rows)
			c.refreshPeriods(ctx, reg, append(leased, sortedKeys(publishedTables(p.Publications))...))
			p.Holders = parseLeases(resp.Rows, time.Now(), c.period, published)
			c.leasesSeen, c.leasesAt = len(p.Holders) > 0, time.Now()
		}
	}
	if resp := part("settings", QueryReplSettings, replSettingsQuery); resp != nil {
		p.Settings = parseReplSettings(resp.Rows)
		p.LeasePeriods = map[string]time.Duration{}
		for t := range publishedTables(p.Publications) {
			if lp, ok := c.periods[t]; ok && lp.d > 0 {
				p.LeasePeriods[t] = lp.d
			}
		}
	}
	st.UpdateReplication(p)
	// Missing privileges would warn every poll; only a cluster that
	// answers nothing is an error.
	if len(p.Errs) >= 5 {
		return errors.New(p.Errs["subscriptions"])
	}
	return nil
}

// whoAmI notes the user obsi connects as and whether it's a superuser;
// sys.users is unreadable for most others, which aren't superusers.
func (c *ReplicationCollector) whoAmI(ctx context.Context, reg *cratedb.Registry) {
	resp, err := trackedQuery(ctx, c.tracker, QueryReplUser, reg, `SELECT CURRENT_USER`)
	if err != nil || len(resp.Rows) == 0 {
		return
	}
	c.user = cratedb.ToString(resp.Rows[0][0])
	resp, err = trackedQuery(ctx, c.tracker, QueryReplUser, reg, `SELECT superuser FROM sys.users WHERE name = CURRENT_USER`)
	c.superuser = err == nil && len(resp.Rows) > 0 && cratedb.ToBool(resp.Rows[0][0])
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// parseReplSettings flattens settings['replication']['logical'] into
// dotted names next to node_initial_primaries_recoveries.
func parseReplSettings(rows [][]interface{}) map[string]string {
	out := map[string]string{}
	if len(rows) == 0 {
		return out
	}
	var walk func(prefix string, v interface{})
	walk = func(prefix string, v interface{}) {
		switch v := v.(type) {
		case map[string]interface{}:
			for k, sub := range v {
				walk(prefix+"."+k, sub)
			}
		case float64:
			out[prefix] = strconv.FormatFloat(v, 'f', -1, 64)
		case nil:
		default:
			out[prefix] = fmt.Sprint(v)
		}
	}
	walk("replication.logical", rows[0][0])
	walk("cluster.routing.allocation.node_initial_primaries_recoveries", rows[0][1])
	return out
}

func (c *ReplicationCollector) period(fqn string) time.Duration {
	if p, ok := c.periods[fqn]; ok && p.d > 0 {
		return p.d
	}
	return cratedb.DefaultLeasePeriod
}

// refreshPeriods reads soft_deletes.retention_lease.period of published
// tables and tables subscribers hold leases on, every leasePeriodRefresh
// and at most maxPeriodReads per poll. A failure keeps the default for
// this round.
func (c *ReplicationCollector) refreshPeriods(ctx context.Context, reg *cratedb.Registry, tables []string) {
	reads := 0
	seen := map[string]bool{}
	for _, fqn := range tables {
		if seen[fqn] {
			continue
		}
		seen[fqn] = true
		if p, ok := c.periods[fqn]; ok && time.Since(p.at) < leasePeriodRefresh {
			continue
		}
		if reads == maxPeriodReads {
			return
		}
		reads++
		schema, table, _ := strings.Cut(fqn, ".")
		resp, err := trackedQuery(ctx, c.tracker, QueryReplLeasePeriod, reg, fmt.Sprintf("SHOW CREATE TABLE %s.%s", cratedb.QuoteIdent(schema), cratedb.QuoteIdent(table)))
		if err != nil || len(resp.Rows) == 0 {
			c.periods[fqn] = leasePeriod{at: time.Now()}
			continue
		}
		c.periods[fqn] = leasePeriod{d: parseLeasePeriod(cratedb.ToString(resp.Rows[0][0])), at: time.Now()}
	}
}

func parseLeasePeriod(ddl string) time.Duration {
	if d, ok := cratedb.LeasePeriodFromDDL(ddl); ok {
		return d
	}
	return cratedb.DefaultLeasePeriod
}

func parseSubscriptions(rows [][]interface{}) []cratedb.Subscription {
	var out []cratedb.Subscription
	for _, row := range rows {
		name := cratedb.ToString(row[0])
		if len(out) == 0 || out[len(out)-1].Name != name {
			sub := cratedb.Subscription{Name: name, ConnInfo: cratedb.ToString(row[1])}
			if pubs, ok := row[2].([]interface{}); ok {
				for _, p := range pubs {
					sub.Publications = append(sub.Publications, cratedb.ToString(p))
				}
			}
			out = append(out, sub)
		}
		if rel := cratedb.ToString(row[3]); rel != "" {
			schema, table := cratedb.SplitRegclass(rel)
			sub := &out[len(out)-1]
			sub.Tables = append(sub.Tables, cratedb.SubscribedTable{
				Schema: schema, Table: table,
				State: cratedb.ToString(row[4]), Reason: cratedb.ToString(row[5]),
			})
		}
	}
	return out
}

func parsePublications(rows [][]interface{}) []cratedb.Publication {
	var out []cratedb.Publication
	for _, row := range rows {
		name := cratedb.ToString(row[0])
		if len(out) == 0 || out[len(out)-1].Name != name {
			out = append(out, cratedb.Publication{Name: name, AllTables: cratedb.ToBool(row[1])})
		}
		if schema, table := cratedb.ToString(row[2]), cratedb.ToString(row[3]); table != "" {
			pub := &out[len(out)-1]
			pub.Tables = append(pub.Tables, schema+"."+table)
		}
	}
	return out
}

func subscribedTables(subs []cratedb.Subscription) []string {
	var out []string
	for _, s := range subs {
		for _, t := range s.Tables {
			out = append(out, t.FQN())
		}
	}
	return out
}

func publishedTables(pubs []cratedb.Publication) map[string]bool {
	out := map[string]bool{}
	for _, p := range pubs {
		for _, t := range p.Tables {
			out[t] = true
		}
	}
	return out
}

func parseReplicaShards(rows [][]interface{}) map[string]store.ReplicaShards {
	type shardKey struct {
		fqn, partition string
		id             int64
	}
	started := map[shardKey]bool{}
	out := map[string]store.ReplicaShards{}
	for _, row := range rows {
		k := shardKey{cratedb.ToString(row[0]), cratedb.ToString(row[1]), cratedb.ToInt64(row[2])}
		if _, ok := started[k]; !ok {
			started[k] = false
			r := out[k.fqn]
			r.Shards++
			out[k.fqn] = r
		}
		if !cratedb.ToBool(row[3]) || cratedb.ToString(row[4]) != "STARTED" || started[k] {
			continue
		}
		started[k] = true
		r := out[k.fqn]
		r.Started++
		r.Docs += cratedb.ToInt64(row[5])
		r.SeqNo += cratedb.ToInt64(row[6]) + 1
		r.Size += cratedb.ToInt64(row[7])
		r.CopiedBytes += cratedb.ToInt64(row[8])
		r.CopyShardMs += cratedb.ToInt64(row[9])
		out[k.fqn] = r
	}
	// A replica waiting for a node; shards still being copied are
	// UNASSIGNED without a started primary and don't count.
	for _, row := range rows {
		k := shardKey{cratedb.ToString(row[0]), cratedb.ToString(row[1]), cratedb.ToInt64(row[2])}
		if !cratedb.ToBool(row[3]) && cratedb.ToString(row[4]) == "UNASSIGNED" && started[k] {
			r := out[k.fqn]
			r.UnassignedReplicas++
			out[k.fqn] = r
		}
	}
	return out
}

// leasedTables lists the tables any subscriber holds a lease on.
func leasedTables(rows [][]interface{}) []string {
	seen := map[string]bool{}
	var out []string
	for _, row := range rows {
		fqn := cratedb.ToString(row[1])
		for _, l := range leases(row[3]) {
			if strings.HasPrefix(cratedb.ToString(l["source"]), cratedb.LeaseSourcePrefix) && !seen[fqn] {
				seen[fqn] = true
				out = append(out, fqn)
			}
		}
	}
	return out
}

func leases(v interface{}) []map[string]interface{} {
	list, _ := v.([]interface{})
	out := make([]map[string]interface{}, 0, len(list))
	for _, l := range list {
		if m, ok := l.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

// parseLeases folds the subscribers' retention leases per subscriber.
// published nil means it isn't known which tables are published.
// Lease timestamps are the publisher's clock; they're moved to obsi's by
// the difference to the now() the query returned.
func parseLeases(rows [][]interface{}, now time.Time, period func(string) time.Duration, published map[string]bool) []store.LeaseHolder {
	type agg struct {
		holder store.LeaseHolder
		tables map[string]bool
	}
	bySub := map[string]*agg{}
	for _, row := range rows {
		skew := now.Sub(msTime(row[0]))
		fqn := cratedb.ToString(row[1])
		maxSeq := cratedb.ToInt64(row[2])
		for _, l := range leases(row[3]) {
			name, ok := strings.CutPrefix(cratedb.ToString(l["source"]), cratedb.LeaseSourcePrefix)
			if !ok {
				continue
			}
			a := bySub[name]
			if a == nil {
				a = &agg{holder: store.LeaseHolder{Subscriber: name}, tables: map[string]bool{}}
				bySub[name] = a
			}
			a.tables[fqn] = true
			h := &a.holder
			if retaining := cratedb.ToInt64(l["retaining_seq_no"]); cratedb.LeaseUnpulled(retaining, maxSeq) {
				h.Unknown++
			} else {
				h.Behind += max(maxSeq-retaining, 0)
				h.Retained += retaining
				h.Known++
			}
			seen := msTime(l["timestamp"]).Add(skew)
			if seen.After(h.LastSeen) {
				h.LastSeen = seen
			}
			p := period(fqn)
			if exp := seen.Add(p); h.Expires.IsZero() || exp.Before(h.Expires) {
				h.Expires, h.Period = exp, p
			}
		}
	}
	out := make([]store.LeaseHolder, 0, len(bySub))
	for _, a := range bySub {
		a.holder.Tables = len(a.tables)
		a.holder.On = sortedKeys(a.tables)
		for t := range a.tables {
			if published != nil && !published[t] {
				a.holder.Unpublished++
			}
		}
		out = append(out, a.holder)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Subscriber < out[j].Subscriber })
	return out
}
