package store

import (
	"sync"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// NodeSnapshot is a point-in-time capture of a node's metrics.
type NodeSnapshot struct {
	cratedb.NodeInfo
	DirectReachable bool          // whether the direct IP heartbeat succeeded
	LastLatency     time.Duration // latency from direct heartbeat (0 if unreachable)
	Gone            bool          // node was previously seen but disappeared from sys.nodes
	LastSeen        time.Time     // when the node was last seen in sys.nodes

	// Derived IO rates (computed from cumulative counter deltas)
	ReadIOPS       float64 // read ops/sec
	WriteIOPS      float64 // write ops/sec
	ReadThroughput float64 // bytes/sec read
	WriteThroughput float64 // bytes/sec written

	// Thread pool deltas (new rejections since last poll)
	ThreadPoolNewRejections int64 // sum of new rejections across write/search/generic
}

type ioSample struct {
	Reads        int64
	Writes       int64
	BytesRead    int64
	BytesWritten int64
}

// Store is the central data store bridging collectors and the TUI.
// All writes come from collectors; all reads come from the TUI via Snapshot().
type Store struct {
	mu sync.RWMutex

	// Latest snapshots
	clusterSettings cratedb.ClusterSettings
	summit          cratedb.Summit
	clusterChecks   []cratedb.ClusterCheck
	tableHealth     []cratedb.TableHealth
	nodes         []NodeSnapshot
	activeQueries []cratedb.ActiveQuery
	tables        []cratedb.TableInfo
	viewCount     int
	shards        []cratedb.ShardInfo
	allocations   []cratedb.AllocationInfo

	// Track known nodes for disappearance detection
	knownNodes map[string]NodeSnapshot // nodeID -> last known snapshot

	// Previous sample for IO rate derivation
	prevIOSample map[string]ioSample
	prevIOTime   time.Time

	// Previous thread pool rejected counters: nodeID -> poolName -> rejected
	prevRejected map[string]map[string]int64

	// Time-series ring buffers (keyed by node ID)
	nodeCPUHistory         map[string]*RingBuf[float64]
	nodeHeapHistory        map[string]*RingBuf[float64]
	nodeLoadHistory        map[string]*RingBuf[float64]
	nodeLoadSatHistory     map[string]*RingBuf[float64]
	nodeReadIOPSHistory    map[string]*RingBuf[float64]
	nodeWriteIOPSHistory   map[string]*RingBuf[float64]
	nodeReadTPHistory      map[string]*RingBuf[float64] // read throughput bytes/s
	nodeWriteTPHistory     map[string]*RingBuf[float64] // write throughput bytes/s

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
	Nodes         []NodeSnapshot
	ActiveQueries []cratedb.ActiveQuery
	Tables        []cratedb.TableInfo
	ViewCount     int
	Shards        []cratedb.ShardInfo
	Allocations   []cratedb.AllocationInfo

	NodeCPUHistory         map[string][]float64
	NodeHeapHistory        map[string][]float64
	NodeLoadHistory        map[string][]float64
	NodeLoadSatHistory     map[string][]float64
	NodeReadIOPSHistory    map[string][]float64
	NodeWriteIOPSHistory   map[string][]float64
	NodeReadTPHistory      map[string][]float64
	NodeWriteTPHistory     map[string][]float64

	Staleness   map[string]bool      // collector name -> is stale
	LastUpdated map[string]time.Time // collector name -> last success
}

// New creates a new store.
func New(sparklineSize int, collectors map[string]config.CollectorConfig) *Store {
	staleAfter := make(map[string]time.Duration)
	for name, cc := range collectors {
		// Consider data stale after 3x the poll interval
		staleAfter[name] = cc.Interval.Duration * 3
	}

	return &Store{
		knownNodes:            make(map[string]NodeSnapshot),
		prevIOSample:          make(map[string]ioSample),
		prevRejected:          make(map[string]map[string]int64),
		nodeCPUHistory:        make(map[string]*RingBuf[float64]),
		nodeHeapHistory:       make(map[string]*RingBuf[float64]),
		nodeLoadHistory:       make(map[string]*RingBuf[float64]),
		nodeLoadSatHistory:    make(map[string]*RingBuf[float64]),
		nodeReadIOPSHistory:   make(map[string]*RingBuf[float64]),
		nodeWriteIOPSHistory:  make(map[string]*RingBuf[float64]),
		nodeReadTPHistory:     make(map[string]*RingBuf[float64]),
		nodeWriteTPHistory:    make(map[string]*RingBuf[float64]),
		sparklineSize:   sparklineSize,
		lastUpdated:     make(map[string]time.Time),
		staleAfter:      staleAfter,
	}
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
}

// UpdateNodes updates node snapshots and pushes to history ring buffers.
func (s *Store) UpdateNodes(nodes []NodeSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(s.prevIOTime).Seconds()

	// Compute IO rates from counter deltas
	for i := range nodes {
		n := &nodes[i]
		if elapsed > 0 {
			if prev, ok := s.prevIOSample[n.ID]; ok {
				n.ReadIOPS = float64(n.FSReads-prev.Reads) / elapsed
				n.WriteIOPS = float64(n.FSWrites-prev.Writes) / elapsed
				n.ReadThroughput = float64(n.FSBytesRead-prev.BytesRead) / elapsed
				n.WriteThroughput = float64(n.FSBytesWritten-prev.BytesWritten) / elapsed
				// Guard against counter resets (node restart)
				if n.ReadIOPS < 0 {
					n.ReadIOPS = 0
				}
				if n.WriteIOPS < 0 {
					n.WriteIOPS = 0
				}
				if n.ReadThroughput < 0 {
					n.ReadThroughput = 0
				}
				if n.WriteThroughput < 0 {
					n.WriteThroughput = 0
				}
			}
		}
		s.prevIOSample[n.ID] = ioSample{
			Reads:        n.FSReads,
			Writes:       n.FSWrites,
			BytesRead:    n.FSBytesRead,
			BytesWritten: n.FSBytesWritten,
		}
	}
	s.prevIOTime = now

	// Compute thread pool rejection deltas
	trackedPools := map[string]bool{"write": true, "search": true, "generic": true}
	for i := range nodes {
		n := &nodes[i]
		var newRej int64
		prev := s.prevRejected[n.ID]
		curr := make(map[string]int64)
		for _, p := range n.ThreadPools {
			if !trackedPools[p.Name] {
				continue
			}
			curr[p.Name] = p.Rejected
			if prev != nil {
				if old, ok := prev[p.Name]; ok {
					delta := p.Rejected - old
					if delta > 0 {
						newRej += delta
					}
				}
			}
		}
		n.ThreadPoolNewRejections = newRej
		s.prevRejected[n.ID] = curr
	}

	// Track node disappearances
	currentIDs := make(map[string]bool)
	for i := range nodes {
		nodes[i].LastSeen = now
		currentIDs[nodes[i].ID] = true
		s.knownNodes[nodes[i].ID] = nodes[i]
	}

	// Append disappeared nodes (previously known but not in current response)
	for id, prev := range s.knownNodes {
		if !currentIDs[id] {
			gone := prev
			gone.Gone = true
			// Keep the LastSeen from when it was last actually seen
			nodes = append(nodes, gone)
		}
	}

	// Remove nodes gone for more than 5 minutes from tracking
	for id, prev := range s.knownNodes {
		if !currentIDs[id] && now.Sub(prev.LastSeen) > 5*time.Minute {
			delete(s.knownNodes, id)
		}
	}

	s.nodes = nodes
	s.lastUpdated["nodes"] = now

	for _, n := range nodes {
		if n.Gone {
			continue // don't push stale metrics into history
		}
		if _, ok := s.nodeCPUHistory[n.ID]; !ok {
			s.nodeCPUHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
			s.nodeHeapHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
			s.nodeLoadHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
			s.nodeLoadSatHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
			s.nodeReadIOPSHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
			s.nodeWriteIOPSHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
			s.nodeReadTPHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
			s.nodeWriteTPHistory[n.ID] = NewRingBuf[float64](s.sparklineSize)
		}
		s.nodeCPUHistory[n.ID].Push(float64(n.CPUPercent))
		if n.HeapMax > 0 {
			s.nodeHeapHistory[n.ID].Push(float64(n.HeapUsed) / float64(n.HeapMax) * 100)
		}
		s.nodeLoadHistory[n.ID].Push(n.Load[0])
		if n.NumCPUs > 0 {
			s.nodeLoadSatHistory[n.ID].Push(n.Load[0] / float64(n.NumCPUs) * 100)
		}
		s.nodeReadIOPSHistory[n.ID].Push(n.ReadIOPS)
		s.nodeWriteIOPSHistory[n.ID].Push(n.WriteIOPS)
		s.nodeReadTPHistory[n.ID].Push(n.ReadThroughput)
		s.nodeWriteTPHistory[n.ID].Push(n.WriteThroughput)
	}
}

// UpdateActiveQueries updates the list of active queries.
func (s *Store) UpdateActiveQueries(queries []cratedb.ActiveQuery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeQueries = queries
	s.lastUpdated["queries"] = time.Now()
}

// UpdateTables updates table and shard info.
func (s *Store) UpdateTables(tables []cratedb.TableInfo, viewCount int, shards []cratedb.ShardInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables = tables
	s.viewCount = viewCount
	s.shards = shards
	s.lastUpdated["shards"] = time.Now()
}

// UpdateAllocations updates allocation info for non-STARTED shards.
func (s *Store) UpdateAllocations(allocs []cratedb.AllocationInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocations = allocs
}

// UpdateShardsPartial replaces only non-STARTED shards in the existing list,
// keeping STARTED shards from the last full collection intact.
func (s *Store) UpdateShardsPartial(nonStarted []cratedb.ShardInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := make([]cratedb.ShardInfo, 0, len(s.shards))
	for _, sh := range s.shards {
		if sh.RoutingState == "STARTED" {
			kept = append(kept, sh)
		}
	}
	s.shards = append(kept, nonStarted...)
	s.lastUpdated["shards"] = time.Now()
}

// AnyNodeHeapAbove returns true if any node has heap usage above the given percentage.
func (s *Store) AnyNodeHeapAbove(pct float64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.nodes {
		if n.Gone || n.HeapMax == 0 {
			continue
		}
		if float64(n.HeapUsed)/float64(n.HeapMax)*100 > pct {
			return true
		}
	}
	return false
}

// Snapshot returns a read-only copy of the store.
// throttleMultiplier adjusts staleness thresholds to match the effective poll interval.
func (s *Store) Snapshot(throttleMultiplier int) StoreSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := StoreSnapshot{
		ClusterSettings: s.clusterSettings,
		Summit:          s.summit,
		ClusterChecks:   copySlice(s.clusterChecks),
		TableHealth:     copySlice(s.tableHealth),
		Nodes:           copySlice(s.nodes),
		ActiveQueries:   copySlice(s.activeQueries),
		Tables:          copySlice(s.tables),
		ViewCount:       s.viewCount,
		Shards:          copySlice(s.shards),
		Allocations:     copySlice(s.allocations),
		NodeCPUHistory:        make(map[string][]float64),
		NodeHeapHistory:       make(map[string][]float64),
		NodeLoadHistory:       make(map[string][]float64),
		NodeLoadSatHistory:    make(map[string][]float64),
		NodeReadIOPSHistory:   make(map[string][]float64),
		NodeWriteIOPSHistory:  make(map[string][]float64),
		NodeReadTPHistory:     make(map[string][]float64),
		NodeWriteTPHistory:    make(map[string][]float64),
		Staleness:             make(map[string]bool),
		LastUpdated:           make(map[string]time.Time),
	}

	for id, buf := range s.nodeCPUHistory {
		snap.NodeCPUHistory[id] = buf.Slice()
	}
	for id, buf := range s.nodeHeapHistory {
		snap.NodeHeapHistory[id] = buf.Slice()
	}
	for id, buf := range s.nodeLoadHistory {
		snap.NodeLoadHistory[id] = buf.Slice()
	}
	for id, buf := range s.nodeLoadSatHistory {
		snap.NodeLoadSatHistory[id] = buf.Slice()
	}
	for id, buf := range s.nodeReadIOPSHistory {
		snap.NodeReadIOPSHistory[id] = buf.Slice()
	}
	for id, buf := range s.nodeWriteIOPSHistory {
		snap.NodeWriteIOPSHistory[id] = buf.Slice()
	}
	for id, buf := range s.nodeReadTPHistory {
		snap.NodeReadTPHistory[id] = buf.Slice()
	}
	for id, buf := range s.nodeWriteTPHistory {
		snap.NodeWriteTPHistory[id] = buf.Slice()
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
