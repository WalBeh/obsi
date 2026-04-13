package cratedb

import (
	"sort"
	"time"
)

// LatencyStats holds computed latency percentiles.
type LatencyStats struct {
	Avg time.Duration
	P90 time.Duration
	Max time.Duration
	N   int // number of samples
}

// LatencyTracker records query latencies in a circular buffer and computes percentile stats.
// Not safe for concurrent use — callers must synchronize externally (e.g. via Registry.mu).
type LatencyTracker struct {
	samples []time.Duration
	idx     int
	full    bool
}

// NewLatencyTracker creates a tracker with the given buffer size.
func NewLatencyTracker(size int) *LatencyTracker {
	return &LatencyTracker{
		samples: make([]time.Duration, size),
	}
}

// Record adds a latency sample to the circular buffer.
func (lt *LatencyTracker) Record(d time.Duration) {
	lt.samples[lt.idx] = d
	lt.idx = (lt.idx + 1) % len(lt.samples)
	if lt.idx == 0 {
		lt.full = true
	}
}

// Stats returns avg/p90/max from collected samples.
func (lt *LatencyTracker) Stats() LatencyStats {
	n := lt.idx
	if lt.full {
		n = len(lt.samples)
	}
	if n == 0 {
		return LatencyStats{}
	}

	sorted := make([]time.Duration, n)
	if lt.full {
		copy(sorted, lt.samples)
	} else {
		copy(sorted, lt.samples[:n])
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}

	p90Idx := (n - 1) * 90 / 100
	return LatencyStats{
		Avg: sum / time.Duration(n),
		P90: sorted[p90Idx],
		Max: sorted[n-1],
		N:   n,
	}
}
