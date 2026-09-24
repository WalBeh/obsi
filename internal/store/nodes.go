package store

import (
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// nodeDisappearanceTimeout is how long a gone node is tracked before being removed.
const nodeDisappearanceTimeout = 5 * time.Minute

// trackedThreadPools are the thread pools monitored for rejection deltas.
var trackedThreadPools = map[string]bool{"write": true, "search": true, "generic": true}

// NodeSnapshot is a point-in-time capture of a node's metrics.
type NodeSnapshot struct {
	cratedb.NodeInfo
	DirectReachable bool          // whether the direct IP heartbeat succeeded
	LastLatency     time.Duration // latency from direct heartbeat (0 if unreachable)
	Gone            bool          // node was previously seen but disappeared from sys.nodes
	LastSeen        time.Time     // when the node was last seen in sys.nodes

	// Derived IO rates (computed from cumulative counter deltas)
	ReadIOPS        float64 // read ops/sec
	WriteIOPS       float64 // write ops/sec
	ReadThroughput  float64 // bytes/sec read
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

// nodeHistory holds all time-series ring buffers for a single node.
type nodeHistory struct {
	CPU       *RingBuf[float64]
	Heap      *RingBuf[float64]
	Load      *RingBuf[float64]
	LoadSat   *RingBuf[float64]
	ReadIOPS  *RingBuf[float64]
	WriteIOPS *RingBuf[float64]
	ReadTP    *RingBuf[float64] // read throughput bytes/s
	WriteTP   *RingBuf[float64] // write throughput bytes/s
}

func newNodeHistory(size int) *nodeHistory {
	return &nodeHistory{
		CPU:       NewRingBuf[float64](size),
		Heap:      NewRingBuf[float64](size),
		Load:      NewRingBuf[float64](size),
		LoadSat:   NewRingBuf[float64](size),
		ReadIOPS:  NewRingBuf[float64](size),
		WriteIOPS: NewRingBuf[float64](size),
		ReadTP:    NewRingBuf[float64](size),
		WriteTP:   NewRingBuf[float64](size),
	}
}

// NodeHistorySnapshot is a read-only copy of a node's time-series data.
type NodeHistorySnapshot struct {
	CPU       []float64
	Heap      []float64
	Load      []float64
	LoadSat   []float64
	ReadIOPS  []float64
	WriteIOPS []float64
	ReadTP    []float64
	WriteTP   []float64
}

func (h *nodeHistory) snapshot() NodeHistorySnapshot {
	return NodeHistorySnapshot{
		CPU:       h.CPU.Slice(),
		Heap:      h.Heap.Slice(),
		Load:      h.Load.Slice(),
		LoadSat:   h.LoadSat.Slice(),
		ReadIOPS:  h.ReadIOPS.Slice(),
		WriteIOPS: h.WriteIOPS.Slice(),
		ReadTP:    h.ReadTP.Slice(),
		WriteTP:   h.WriteTP.Slice(),
	}
}

// UpdateNodes updates node snapshots and pushes to history ring buffers.
func (s *Store) UpdateNodes(nodes []NodeSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.computeIORates(nodes, now)
	s.computeRejectionDeltas(nodes)
	nodes = s.trackDisappearances(nodes, now)

	s.nodes = nodes
	s.lastUpdated["nodes"] = now
	s.alerts.sync("nodes", now, s.alerts.nodeAlerts(nodes, s.clusterSettings))

	s.pushHistory(nodes)
}

// computeIORates derives per-second IO rates from cumulative counter deltas.
// Caller must hold s.mu.
func (s *Store) computeIORates(nodes []NodeSnapshot, now time.Time) {
	elapsed := now.Sub(s.prevIOTime).Seconds()
	for i := range nodes {
		n := &nodes[i]
		if elapsed > 0 {
			if prev, ok := s.prevIOSample[n.ID]; ok {
				n.ReadIOPS = max(float64(n.FSReads-prev.Reads)/elapsed, 0)
				n.WriteIOPS = max(float64(n.FSWrites-prev.Writes)/elapsed, 0)
				n.ReadThroughput = max(float64(n.FSBytesRead-prev.BytesRead)/elapsed, 0)
				n.WriteThroughput = max(float64(n.FSBytesWritten-prev.BytesWritten)/elapsed, 0)
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
}

// computeRejectionDeltas computes new thread pool rejections since the last poll.
// Caller must hold s.mu.
func (s *Store) computeRejectionDeltas(nodes []NodeSnapshot) {
	for i := range nodes {
		n := &nodes[i]
		var newRej int64
		prev := s.prevRejected[n.ID]
		curr := make(map[string]int64)
		for _, p := range n.ThreadPools {
			if !trackedThreadPools[p.Name] {
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
}

// trackDisappearances marks nodes that have left the cluster and appends them
// to the slice. Nodes gone longer than nodeDisappearanceTimeout are removed
// from tracking. Caller must hold s.mu.
func (s *Store) trackDisappearances(nodes []NodeSnapshot, now time.Time) []NodeSnapshot {
	currentIDs := make(map[string]bool, len(nodes))
	for i := range nodes {
		nodes[i].LastSeen = now
		currentIDs[nodes[i].ID] = true
		s.knownNodes[nodes[i].ID] = nodes[i]
	}

	for id, prev := range s.knownNodes {
		if currentIDs[id] {
			continue
		}
		if now.Sub(prev.LastSeen) > nodeDisappearanceTimeout {
			delete(s.knownNodes, id)
			delete(s.nodeHistories, id)
			delete(s.prevIOSample, id)
			delete(s.prevRejected, id)
		} else {
			gone := prev
			gone.Gone = true
			nodes = append(nodes, gone)
		}
	}

	return nodes
}

// pushHistory records current metrics into per-node ring buffers for sparklines.
// Caller must hold s.mu.
func (s *Store) pushHistory(nodes []NodeSnapshot) {
	for _, n := range nodes {
		if n.Gone {
			continue
		}
		h, ok := s.nodeHistories[n.ID]
		if !ok {
			h = newNodeHistory(s.sparklineSize)
			s.nodeHistories[n.ID] = h
		}
		h.CPU.Push(float64(n.CPUPercent))
		if n.HeapMax > 0 {
			h.Heap.Push(float64(n.HeapUsed) / float64(n.HeapMax) * 100)
		}
		h.Load.Push(n.Load[0])
		if n.NumCPUs > 0 {
			h.LoadSat.Push(n.Load[0] / float64(n.NumCPUs) * 100)
		}
		h.ReadIOPS.Push(n.ReadIOPS)
		h.WriteIOPS.Push(n.WriteIOPS)
		h.ReadTP.Push(n.ReadThroughput)
		h.WriteTP.Push(n.WriteThroughput)
	}
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
