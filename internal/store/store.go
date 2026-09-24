package store

import (
	"sync"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/jmx"
)

// stalenessMultiplier defines how many poll intervals without an update before data is considered stale.
const stalenessMultiplier = 3

// Store is the central data store bridging collectors and the TUI.
// All writes come from collectors; all reads come from the TUI via Snapshot().
type Store struct {
	mu sync.RWMutex

	// Latest snapshots
	clusterSettings cratedb.ClusterSettings
	summit          cratedb.Summit
	clusterChecks   []cratedb.ClusterCheck
	tableHealth     []cratedb.TableHealth
	nodes           []NodeSnapshot
	activeQueries   []cratedb.ActiveQuery
	tables          []cratedb.TableInfo
	viewCount       int
	shards          []cratedb.ShardInfo
	allocations     []cratedb.AllocationInfo

	// Slowest-queries board, observed since the store was created.
	observedSince   time.Time
	queriesInterval time.Duration
	inflight        map[string]*ObservedQuery // non-stuck jobs seen on the last poll
	slowestDone     []ObservedQuery           // finished jobs, top SlowestLimit
	jobsLog         JobsLogState

	alerts alertLog

	// JMX snapshot keyed by full pod name (matches NodeInfo.Hostname on Cloud).
	jmxPods    map[string]*jmx.JMXSnapshot
	jmxCluster jmx.ClusterJMX

	// Previous JMX GC counters for delta computation (pod → gc name → reading).
	prevGC map[string]map[string]jmx.GCStat

	// Previous JMX IO counters and current derived rates (pod-keyed).
	prevJMXIO map[string]jmxIOPrev
	jmxRates  map[string]*JMXRates

	// Per-pod history for JMX-derived signals (GC pauses + network rates).
	jmxHistory map[string]*jmxHistory

	// Track known nodes for disappearance detection
	knownNodes map[string]NodeSnapshot // nodeID -> last known snapshot

	// Previous sample for IO rate derivation
	prevIOSample map[string]ioSample
	prevIOTime   time.Time

	// Previous thread pool rejected counters: nodeID -> poolName -> rejected
	prevRejected map[string]map[string]int64

	// Time-series ring buffers (keyed by node ID)
	nodeHistories map[string]*nodeHistory

	sparklineSize int

	// Staleness tracking
	lastUpdated map[string]time.Time
	staleAfter  map[string]time.Duration
}

// StoreSnapshot is a read-only copy of the store for the TUI.
type StoreSnapshot struct {
	ClusterSettings cratedb.ClusterSettings
	Summit          cratedb.Summit
	ClusterChecks   []cratedb.ClusterCheck
	TableHealth     []cratedb.TableHealth
	Nodes           []NodeSnapshot
	ActiveQueries   []cratedb.ActiveQuery
	Tables          []cratedb.TableInfo
	ViewCount       int
	TotalShards     int
	Shards          []cratedb.ShardInfo
	Allocations     []cratedb.AllocationInfo

	// SlowestQueries is the top-N by observed duration since ObservedSince,
	// finished and running jobs mixed. SampleInterval is the effective
	// queries poll interval (throttle applied), i.e. the error bar on
	// finished durations.
	SlowestQueries []ObservedQuery
	ObservedSince  time.Time
	SampleInterval time.Duration
	JobsLog        JobsLogState

	// Alerts is always filled, whatever the hint.
	Alerts AlertsState

	// JMX per-pod snapshots; empty when the JMX collector is disabled or
	// has not yet produced a successful scrape. Pod-name keys match
	// NodeInfo.Hostname on CrateDB Cloud.
	JMX        map[string]*jmx.JMXSnapshot
	JMXCluster jmx.ClusterJMX

	// JMXHistory holds per-pod ring-buffer snapshots derived from JMX
	// (GC pauses + network rates). Key: pod name.
	JMXHistory map[string]JMXHistorySnapshot

	// JMXRates holds per-pod current-cycle rates (network and per-device
	// disk byte rates) computed by the store. Key: pod name.
	JMXRates map[string]*JMXRates

	// NodeHistory maps node ID to its time-series snapshots.
	NodeHistory map[string]NodeHistorySnapshot

	Staleness   map[string]bool      // collector name -> is stale
	LastUpdated map[string]time.Time // collector name -> last success
}

// New creates a new store.
func New(sparklineSize int, collectors map[string]config.CollectorConfig) *Store {
	staleAfter := make(map[string]time.Duration)
	for name, cc := range collectors {
		staleAfter[name] = cc.Interval.Duration * stalenessMultiplier
	}

	return &Store{
		knownNodes:    make(map[string]NodeSnapshot),
		prevIOSample:  make(map[string]ioSample),
		prevRejected:  make(map[string]map[string]int64),
		nodeHistories: make(map[string]*nodeHistory),
		prevGC:        make(map[string]map[string]jmx.GCStat),
		prevJMXIO:     make(map[string]jmxIOPrev),
		jmxRates:      make(map[string]*JMXRates),
		jmxHistory:    make(map[string]*jmxHistory),
		sparklineSize: sparklineSize,
		lastUpdated:   make(map[string]time.Time),
		staleAfter:    staleAfter,

		observedSince:   time.Now(),
		queriesInterval: collectors["queries"].Interval.Duration,
		inflight:        make(map[string]*ObservedQuery),
	}
}

// RegisterCollectorStaleness adds a collector to the staleness-tracking map
// after construction. Used by JMX because it isn't part of the
// cfg.Collectors map (its enablement signal is JMX.Endpoint).
func (s *Store) RegisterCollectorStaleness(name string, interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.staleAfter == nil {
		s.staleAfter = make(map[string]time.Duration)
	}
	s.staleAfter[name] = interval * stalenessMultiplier
}

// UpdateClusterSettings updates cluster-level settings.
func (s *Store) UpdateClusterSettings(settings cratedb.ClusterSettings) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterSettings = settings
	s.lastUpdated["cluster"] = time.Now()
}

// UpdateSummit updates the random summit.
func (s *Store) UpdateSummit(summit cratedb.Summit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.summit = summit
}

// UpdateClusterHealth updates cluster checks and table health.
func (s *Store) UpdateClusterHealth(checks []cratedb.ClusterCheck, health []cratedb.TableHealth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusterChecks = checks
	s.tableHealth = health
	s.lastUpdated["health"] = time.Now()
	s.alerts.sync("health", time.Now(), healthAlerts(checks, health))
}

// ClusterHealth returns the worst table health across the cluster: "RED", "YELLOW", "GREEN", or "" if unknown.
func (s *Store) ClusterHealth() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.tableHealth) == 0 {
		return ""
	}
	worst := "GREEN"
	for _, h := range s.tableHealth {
		if h.Health == "RED" {
			return "RED"
		}
		if h.Health == "YELLOW" {
			worst = "YELLOW"
		}
	}
	return worst
}

// SnapshotHint tells Snapshot which data to include, avoiding expensive copies
// for tabs that don't need them.
type SnapshotHint struct {
	IncludeNodes   bool // node list + history ring buffers
	IncludeTables  bool // table list + shard count (lightweight)
	IncludeShards  bool // full shard list + allocations (expensive on large clusters)
	IncludeQueries bool // active queries
	IncludeHealth  bool // cluster checks + table health
	IncludeCluster bool // cluster settings + summit
	IncludeJMX     bool // per-pod JMX snapshots + cluster summary
}

// Snapshot returns a read-only copy of the store.
// throttleMultiplier adjusts staleness thresholds to match the effective poll interval.
func (s *Store) Snapshot(throttleMultiplier int, hint SnapshotHint) StoreSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := StoreSnapshot{
		Staleness:   make(map[string]bool),
		LastUpdated: make(map[string]time.Time),
		Alerts:      s.alerts.snapshot(),
	}

	if hint.IncludeCluster {
		snap.ClusterSettings = s.clusterSettings
		snap.Summit = s.summit
	}
	if hint.IncludeHealth {
		snap.ClusterChecks = copySlice(s.clusterChecks)
		snap.TableHealth = copySlice(s.tableHealth)
	}
	if hint.IncludeNodes {
		snap.Nodes = copySlice(s.nodes)
		snap.NodeHistory = make(map[string]NodeHistorySnapshot, len(s.nodeHistories))
		for id, h := range s.nodeHistories {
			snap.NodeHistory[id] = h.snapshot()
		}
	}
	if hint.IncludeQueries {
		snap.ActiveQueries = copySlice(s.activeQueries)
		snap.SlowestQueries = s.slowestSnapshot(time.Now())
		snap.ObservedSince = s.observedSince
		snap.SampleInterval = s.queriesInterval * time.Duration(max(throttleMultiplier, 1))
		snap.JobsLog = s.jobsLog.copy()
	}
	if hint.IncludeTables || hint.IncludeShards {
		snap.Tables = copySlice(s.tables)
		snap.ViewCount = s.viewCount
		snap.TotalShards = len(s.shards)
	}
	if hint.IncludeShards {
		snap.Shards = copySlice(s.shards)
		snap.Allocations = copySlice(s.allocations)
	}
	if hint.IncludeJMX && len(s.jmxPods) > 0 {
		snap.JMX = make(map[string]*jmx.JMXSnapshot, len(s.jmxPods))
		for k, v := range s.jmxPods {
			snap.JMX[k] = v
		}
		snap.JMXCluster = s.jmxCluster
		snap.JMXHistory = make(map[string]JMXHistorySnapshot, len(s.jmxHistory))
		for k, h := range s.jmxHistory {
			snap.JMXHistory[k] = h.snapshot()
		}
		snap.JMXRates = make(map[string]*JMXRates, len(s.jmxRates))
		for k, v := range s.jmxRates {
			snap.JMXRates[k] = v
		}
	}

	if throttleMultiplier < 1 {
		throttleMultiplier = 1
	}
	now := time.Now()
	for name, staleAfter := range s.staleAfter {
		effectiveStaleAfter := staleAfter * time.Duration(throttleMultiplier)
		lastUpdate, ok := s.lastUpdated[name]
		snap.Staleness[name] = !ok || lastUpdate.IsZero() || now.Sub(lastUpdate) > effectiveStaleAfter
	}
	for name, t := range s.lastUpdated {
		snap.LastUpdated[name] = t
	}

	return snap
}

func copySlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
