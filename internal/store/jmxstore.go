package store

import (
	"time"

	"github.com/waltergrande/cratedb-observer/internal/jmx"
)

// gcSample is one scrape interval's raw delta plus the wall-clock time it
// was recorded. Storing the deltas (rather than a pre-computed pause-per-
// collection) lets snapshot() derive the weighted mean used by our
// Grafana dashboards:
//
//	rate(jvm_gc_collection_seconds_sum) / rate(jvm_gc_collection_seconds_count)
//	  =  Σ Δseconds  /  Σ Δcount     over the window
//
// Storing the timestamp lets snapshot() also compute the count rate —
// rate(jvm_gc_collection_seconds_count) — honestly across croudng
// outages where no sample is pushed for a stretch.
type gcSample struct {
	Count   int64
	Seconds float64
	At      time.Time
}

// jmxHistory holds per-pod time-series buffers derived from JMX scrapes.
// Keyed off the GC collector name because the set is dynamic (G1 today,
// possibly others on differently-tuned JVMs) and we only allocate buffers
// for collectors we actually observe.
type jmxHistory struct {
	GCDeltas  map[string]*RingBuf[gcSample]
	NetRxRate *RingBuf[float64] // bytes/s — cumulative across interfaces
	NetTxRate *RingBuf[float64]
}

func newJMXHistory(sparklineSize int) *jmxHistory {
	return &jmxHistory{
		GCDeltas:  map[string]*RingBuf[gcSample]{},
		NetRxRate: NewRingBuf[float64](sparklineSize),
		NetTxRate: NewRingBuf[float64](sparklineSize),
	}
}

// GCWindowStat is the precomputed read-side summary for the recent-window
// headline next to each collector row. Collections == 0 signals "no GC
// events in the visible window" so the renderer can show "—" instead of
// a meaningless 0ms.
type GCWindowStat struct {
	MeanPauseMs float64 // Σdsec * 1000 / Σdcount (weighted, matches Grafana)
	MaxPauseMs  float64 // max of per-interval averages (worst slot we saw)
	Collections int64   // total collections across the window
	RatePerSec  float64 // Collections / window-duration-seconds; 0 if undefined
}

// JMXHistorySnapshot is the read-only view used by the TUI.
type JMXHistorySnapshot struct {
	// GCPauseMs holds per-interval average pause times in ms, for sparkline
	// rendering. Slots with no GC events are 0.
	GCPauseMs map[string][]float64
	// GCRecent holds precomputed window stats, one per collector.
	GCRecent map[string]GCWindowStat
	// NetRxRate / NetTxRate are sparkline-ready bytes/s series.
	NetRxRate []float64
	NetTxRate []float64
}

// JMXRates holds per-pod, per-scrape-cycle derived rates from JMX. Updated
// in lockstep with the per-pod snapshot — there is one rate value per pod
// per metric, recomputed on every UpdateJMX from the difference against
// the previous scrape.
type JMXRates struct {
	NetRxBytesPerSec float64
	NetTxBytesPerSec float64
	DiskReadPerSec   map[string]float64 // device → bytes/s
	DiskWritePerSec  map[string]float64
}

// jmxIOPrev is the previous-scrape reading needed to compute rates on the
// next UpdateJMX call. Stored separately from JMXRates so callers reading
// the rate map never see the raw cumulative counters.
type jmxIOPrev struct {
	At             time.Time
	NetRxBytes     int64
	NetTxBytes     int64
	DiskReadBytes  map[string]int64
	DiskWriteBytes map[string]int64
}

func (h *jmxHistory) snapshot() JMXHistorySnapshot {
	out := JMXHistorySnapshot{
		GCPauseMs: make(map[string][]float64, len(h.GCDeltas)),
		GCRecent:  make(map[string]GCWindowStat, len(h.GCDeltas)),
		NetRxRate: h.NetRxRate.Slice(),
		NetTxRate: h.NetTxRate.Slice(),
	}
	for name, r := range h.GCDeltas {
		samples := r.Slice()
		pauses := make([]float64, len(samples))
		var sumSec float64
		var sumCnt int64
		var maxMs float64
		var firstAt, lastAt time.Time
		for i, s := range samples {
			if s.Count > 0 {
				ms := s.Seconds * 1000 / float64(s.Count)
				pauses[i] = ms
				if ms > maxMs {
					maxMs = ms
				}
			}
			sumCnt += s.Count
			sumSec += s.Seconds
			if !s.At.IsZero() {
				if firstAt.IsZero() {
					firstAt = s.At
				}
				lastAt = s.At
			}
		}
		out.GCPauseMs[name] = pauses
		stat := GCWindowStat{MaxPauseMs: maxMs, Collections: sumCnt}
		if sumCnt > 0 {
			stat.MeanPauseMs = sumSec * 1000 / float64(sumCnt)
		}
		// Rate is well-defined once at least two samples exist (we need a
		// non-zero window duration). A single sample has window = 0.
		if window := lastAt.Sub(firstAt).Seconds(); window > 0 {
			stat.RatePerSec = float64(sumCnt) / window
		}
		out.GCRecent[name] = stat
	}
	return out
}

// UpdateJMX replaces the per-pod JMX map and cluster summary with the latest
// extracted scrape, and pushes per-collector GC-pause samples into the
// per-pod history rings for sparkline + recent-stats display.
//
// The caller is expected to have already run the cluster-name safety guard;
// the store does no further validation.
func (s *Store) UpdateJMX(ex *jmx.Extracted) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.pushGCHistory(ex, now)
	s.deriveJMXRates(ex, now)
	s.jmxPods = ex.Pods
	s.jmxCluster = ex.Cluster
	s.lastUpdated["jmx"] = now
}

// pushGCHistory diffs cumulative GC counters from the previous scrape and
// records the (Δcount, Δseconds) tuple per scrape interval for each
// collector. Caller must hold s.mu.
//
// Intervals with no GC events push a zero sample. Negative deltas — which
// happen on JVM restarts when counters reset — are dropped (also pushed
// as zero) to avoid polluting the ring with garbage.
func (s *Store) pushGCHistory(ex *jmx.Extracted, now time.Time) {
	for pod, snap := range ex.Pods {
		hist, ok := s.jmxHistory[pod]
		if !ok {
			hist = newJMXHistory(s.sparklineSize)
			s.jmxHistory[pod] = hist
		}
		prevForPod := s.prevGC[pod]
		for gcName, curr := range snap.GC {
			ring, ok := hist.GCDeltas[gcName]
			if !ok {
				ring = NewRingBuf[gcSample](s.sparklineSize)
				hist.GCDeltas[gcName] = ring
			}
			sample := gcSample{At: now}
			if prev, hasPrev := prevForPod[gcName]; hasPrev {
				dCount := curr.Count - prev.Count
				dSec := curr.TotalSeconds - prev.TotalSeconds
				if dCount > 0 && dSec > 0 {
					sample.Count = dCount
					sample.Seconds = dSec
				}
			}
			ring.Push(sample)
		}
		// Replace prev with a copy of curr for the next diff.
		next := make(map[string]jmx.GCStat, len(snap.GC))
		for k, v := range snap.GC {
			next[k] = v
		}
		s.prevGC[pod] = next
	}
}

// deriveJMXRates computes per-pod network and per-device disk byte rates
// from the difference between this and the previous JMX scrape. Pushes
// the network rates into the per-pod history rings for sparkline display.
// Caller must hold s.mu.
//
// Pods that don't have a previous reading (first scrape, or a new pod
// appearing) get zero rates this cycle and a real rate from the next
// scrape onward. Negative deltas (counter resets on pod restart) are
// clamped to zero.
func (s *Store) deriveJMXRates(ex *jmx.Extracted, now time.Time) {
	fresh := make(map[string]*JMXRates, len(ex.Pods))
	for pod, snap := range ex.Pods {
		r := &JMXRates{
			DiskReadPerSec:  map[string]float64{},
			DiskWritePerSec: map[string]float64{},
		}
		if prev, ok := s.prevJMXIO[pod]; ok {
			if elapsed := now.Sub(prev.At).Seconds(); elapsed > 0 {
				r.NetRxBytesPerSec = clampRate(snap.NetRxBytes-prev.NetRxBytes, elapsed)
				r.NetTxBytesPerSec = clampRate(snap.NetTxBytes-prev.NetTxBytes, elapsed)
				for dev, cur := range snap.DiskReadBytes {
					r.DiskReadPerSec[dev] = clampRate(cur-prev.DiskReadBytes[dev], elapsed)
				}
				for dev, cur := range snap.DiskWriteBytes {
					r.DiskWritePerSec[dev] = clampRate(cur-prev.DiskWriteBytes[dev], elapsed)
				}
			}
		}
		fresh[pod] = r

		hist, ok := s.jmxHistory[pod]
		if !ok {
			hist = newJMXHistory(s.sparklineSize)
			s.jmxHistory[pod] = hist
		}
		hist.NetRxRate.Push(r.NetRxBytesPerSec)
		hist.NetTxRate.Push(r.NetTxBytesPerSec)

		s.prevJMXIO[pod] = jmxIOPrev{
			At:             now,
			NetRxBytes:     snap.NetRxBytes,
			NetTxBytes:     snap.NetTxBytes,
			DiskReadBytes:  copyInt64Map(snap.DiskReadBytes),
			DiskWriteBytes: copyInt64Map(snap.DiskWriteBytes),
		}
	}
	s.jmxRates = fresh
}

// clampRate returns delta/elapsed as a non-negative rate, or 0 when the
// delta is negative (cumulative counters were reset, e.g. pod restart).
func clampRate(delta int64, elapsed float64) float64 {
	if delta < 0 || elapsed <= 0 {
		return 0
	}
	return float64(delta) / elapsed
}

func copyInt64Map(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
