package store

import (
	"math"
	"testing"
	"time"
)

// TestJMXHistorySnapshot_GCWeightedMean pins down the Grafana-aligned math:
//
//	rate(sum) / rate(count)  =  Σ Δseconds / Σ Δcount  over the window
//
// With samples (1 collection × 10ms), (0 collections), (10 collections × 5ms),
// the *weighted* mean is (10ms + 50ms) / (1 + 10) ≈ 5.45ms — not the
// unweighted average of per-interval ratios (7.5ms), which is what an
// earlier version of this package computed.
//
// Also asserts the count rate is derived from sample timestamps (matching
// rate(jvm_gc_collection_seconds_count) on the same window).
func TestJMXHistorySnapshot_GCWeightedMean(t *testing.T) {
	h := newJMXHistory()
	ring := NewRingBuf[gcSample](16)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ring.Push(gcSample{Count: 1, Seconds: 0.010, At: t0})                          // 1 × 10ms
	ring.Push(gcSample{At: t0.Add(30 * time.Second)})                              // quiet
	ring.Push(gcSample{Count: 10, Seconds: 0.050, At: t0.Add(60 * time.Second)})   // 10 × 5ms
	h.GCDeltas["G1 Young Generation"] = ring

	snap := h.snapshot()
	got, ok := snap.GCRecent["G1 Young Generation"]
	if !ok {
		t.Fatal("collector missing from snapshot")
	}

	const wantMean = (10.0 + 50.0) / 11.0 // 5.4545...
	if math.Abs(got.MeanPauseMs-wantMean) > 0.01 {
		t.Errorf("MeanPauseMs = %.4f, want %.4f (weighted by collection count)", got.MeanPauseMs, wantMean)
	}
	if got.MaxPauseMs != 10.0 {
		t.Errorf("MaxPauseMs = %.4f, want 10.0 (worst per-interval avg)", got.MaxPauseMs)
	}
	if got.Collections != 11 {
		t.Errorf("Collections = %d, want 11", got.Collections)
	}
	// Window is 60s, 11 collections → 11/60 ≈ 0.1833/s
	const wantRate = 11.0 / 60.0
	if math.Abs(got.RatePerSec-wantRate) > 0.001 {
		t.Errorf("RatePerSec = %.4f, want %.4f", got.RatePerSec, wantRate)
	}

	pauses := snap.GCPauseMs["G1 Young Generation"]
	wantPauses := []float64{10.0, 0.0, 5.0}
	if len(pauses) != len(wantPauses) {
		t.Fatalf("sparkline length = %d, want %d", len(pauses), len(wantPauses))
	}
	for i, w := range wantPauses {
		if math.Abs(pauses[i]-w) > 0.001 {
			t.Errorf("sparkline[%d] = %.3f, want %.3f", i, pauses[i], w)
		}
	}
}

// TestJMXHistorySnapshot_RateUndefinedOnSingleSample covers the boundary
// where the rate is mathematically undefined (one or zero timestamped
// samples → window duration is 0). The renderer treats RatePerSec == 0
// as "don't show the rate", so this just verifies we don't blow up.
func TestJMXHistorySnapshot_RateUndefinedOnSingleSample(t *testing.T) {
	h := newJMXHistory()
	ring := NewRingBuf[gcSample](4)
	ring.Push(gcSample{Count: 3, Seconds: 0.030, At: time.Now()})
	h.GCDeltas["G1 Young Generation"] = ring

	got := h.snapshot().GCRecent["G1 Young Generation"]
	if got.RatePerSec != 0 {
		t.Errorf("RatePerSec = %.4f, want 0 (window=0 with single sample)", got.RatePerSec)
	}
	if got.Collections != 3 {
		t.Errorf("Collections = %d, want 3", got.Collections)
	}
}

// TestJMXHistorySnapshot_NoEvents covers the "ring exists but is all zeros"
// path — the renderer relies on Collections==0 to show "—" instead of
// a meaningless 0ms.
func TestJMXHistorySnapshot_NoEvents(t *testing.T) {
	h := newJMXHistory()
	ring := NewRingBuf[gcSample](4)
	ring.Push(gcSample{})
	ring.Push(gcSample{})
	h.GCDeltas["G1 Old Generation"] = ring

	snap := h.snapshot()
	got := snap.GCRecent["G1 Old Generation"]
	if got.Collections != 0 {
		t.Errorf("Collections = %d, want 0", got.Collections)
	}
	if got.MeanPauseMs != 0 || got.MaxPauseMs != 0 {
		t.Errorf("expected zeros, got mean=%.2f max=%.2f", got.MeanPauseMs, got.MaxPauseMs)
	}
}
