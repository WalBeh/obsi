package cratedb

import (
	"testing"
	"time"
)

func TestLatencyTrackerStats(t *testing.T) {
	lt := NewLatencyTracker(10)

	// Empty tracker
	stats := lt.Stats()
	if stats.N != 0 || stats.Avg != 0 || stats.P90 != 0 || stats.Max != 0 {
		t.Errorf("empty tracker should return zero stats, got %+v", stats)
	}

	// Add known samples
	samples := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}
	for _, s := range samples {
		lt.Record(s)
	}

	stats = lt.Stats()
	if stats.N != 5 {
		t.Errorf("N: got %d, want 5", stats.N)
	}
	// Avg: (10+20+30+40+50)/5 = 30ms
	if stats.Avg != 30*time.Millisecond {
		t.Errorf("Avg: got %v, want 30ms", stats.Avg)
	}
	// Max: 50ms
	if stats.Max != 50*time.Millisecond {
		t.Errorf("Max: got %v, want 50ms", stats.Max)
	}
	// P90 index: (5-1)*90/100 = 3 → sorted[3] = 40ms
	if stats.P90 != 40*time.Millisecond {
		t.Errorf("P90: got %v, want 40ms", stats.P90)
	}
}

func TestLatencyTrackerMax(t *testing.T) {
	lt := NewLatencyTracker(5)

	if m := lt.Max(); m != 0 {
		t.Errorf("empty Max: got %v, want 0", m)
	}

	lt.Record(10 * time.Millisecond)
	lt.Record(50 * time.Millisecond)
	lt.Record(30 * time.Millisecond)

	if m := lt.Max(); m != 50*time.Millisecond {
		t.Errorf("Max: got %v, want 50ms", m)
	}
}

func TestLatencyTrackerWraparound(t *testing.T) {
	// Buffer size 3: old high-latency samples should be evicted
	lt := NewLatencyTracker(3)

	lt.Record(100 * time.Millisecond) // will be evicted
	lt.Record(200 * time.Millisecond) // will be evicted
	lt.Record(300 * time.Millisecond) // will be evicted
	lt.Record(1 * time.Millisecond)   // overwrites slot 0
	lt.Record(2 * time.Millisecond)   // overwrites slot 1
	lt.Record(3 * time.Millisecond)   // overwrites slot 2

	stats := lt.Stats()
	if stats.N != 3 {
		t.Errorf("N: got %d, want 3", stats.N)
	}
	// Avg: (1+2+3)/3 = 2ms
	if stats.Avg != 2*time.Millisecond {
		t.Errorf("Avg: got %v, want 2ms (old samples should be evicted)", stats.Avg)
	}
	if stats.Max != 3*time.Millisecond {
		t.Errorf("Max: got %v, want 3ms", stats.Max)
	}
}
