package jmx

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	fixtureCluster = "devbrain"
	fixturePod0    = "crate-data-hot-5e52d9b3-3b92-4568-a3a4-620ed9d1d445-0"
)

// TestExtract_CroudngFixture is the golden test: a real scrape converted into
// typed snapshots, with assertions chosen to cover every metric family the
// extractor handles. If any of these break, the dispatch is broken.
func TestExtract_CroudngFixture(t *testing.T) {
	scrape := loadScrape(t)

	ex, err := Extract(scrape, fixtureCluster)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	if ex.Cluster.Name != fixtureCluster {
		t.Errorf("cluster.Name = %q, want %q", ex.Cluster.Name, fixtureCluster)
	}
	if ex.Cluster.ID != "5e52d9b3-3b92-4568-a3a4-620ed9d1d445" {
		t.Errorf("cluster.ID = %q", ex.Cluster.ID)
	}
	if ex.Cluster.LastUserActivity.IsZero() {
		t.Error("cluster.LastUserActivity should be set")
	}
	if got, want := len(ex.Pods), 3; got != want {
		t.Errorf("pod count = %d, want %d (filter should exclude non-CrateDB pods)", got, want)
	}
	// Confirm the filter dropped grand-central and backup-metrics.
	for name := range ex.Pods {
		if !strings.HasPrefix(name, "crate-data-hot-") && !strings.HasPrefix(name, "crate-master-") {
			t.Errorf("non-CrateDB pod leaked through filter: %q", name)
		}
	}

	p := ex.Pods[fixturePod0]
	if p == nil {
		t.Fatalf("pod %q missing", fixturePod0)
	}

	// JVM
	if p.HeapMax != 1879048192 {
		t.Errorf("HeapMax = %d, want 1879048192", p.HeapMax)
	}
	if got := p.GC["G1 Young Generation"].Count; got != 640 {
		t.Errorf("GC[Young].Count = %d, want 640", got)
	}
	if got := p.GC["G1 Concurrent GC"].Count; got != 12 {
		t.Errorf("GC[Concurrent].Count = %d, want 12", got)
	}
	if p.Pools["G1 Eden Space"] == 0 {
		t.Error("Pools[G1 Eden Space] = 0")
	}
	if p.BufferPools["direct"] == 0 {
		t.Error("BufferPools[direct] = 0")
	}

	// CrateDB
	if got := p.QueryStats["DDL"].Total; got != 1951 {
		t.Errorf("QueryStats[DDL].Total = %d, want 1951", got)
	}
	parent := p.Breakers["parent"]
	if parent.Limit <= 0 {
		t.Errorf("Breakers[parent].Limit = %d, want > 0", parent.Limit)
	}
	if parent.Tripped != 0 {
		t.Errorf("Breakers[parent].Tripped = %d, want 0", parent.Tripped)
	}
	if fielddata := p.Breakers["fielddata"]; fielddata.Limit != -1 {
		t.Errorf("Breakers[fielddata].Limit = %d, want -1 (unlimited)", fielddata.Limit)
	}
	if p.Connections["transport"].Open == 0 {
		t.Error("Connections[transport].Open = 0")
	}
	if p.ThreadPools["search"].PoolSize == 0 {
		t.Error("ThreadPools[search].PoolSize = 0")
	}

	// cAdvisor
	if p.ContainerMemBytes != 2298281984 {
		t.Errorf("ContainerMemBytes = %d, want 2298281984", p.ContainerMemBytes)
	}
	if p.NetRxBytes == 0 || p.NetTxBytes == 0 {
		t.Errorf("Net Rx/Tx not populated: rx=%d tx=%d", p.NetRxBytes, p.NetTxBytes)
	}
	if len(p.DiskReadBytes) == 0 {
		t.Error("DiskReadBytes empty (expected per-device entries)")
	}

	// Operator
	if p.LastUserActivity.IsZero() {
		t.Error("LastUserActivity not set on pod-0")
	}
}

// TestExtract_ClusterMismatchRejected verifies the safety guard: when the
// scrape is for a different cluster than obsi is connected to, no data is
// returned. This is the load-bearing invariant of the integration.
func TestExtract_ClusterMismatchRejected(t *testing.T) {
	scrape := loadScrape(t)

	ex, err := Extract(scrape, "some-other-cluster")
	if !errors.Is(err, ErrClusterMismatch) {
		t.Fatalf("err = %v, want wrapping ErrClusterMismatch", err)
	}
	if ex != nil {
		t.Errorf("Extract should return nil result on mismatch, got %+v", ex)
	}
}

// TestExtract_NoIdentityRejected verifies that a scrape without
// cloud_clusters_health is rejected — without identity, we cannot run the
// safety check.
func TestExtract_NoIdentityRejected(t *testing.T) {
	// jmx-sample.txt is a raw JMX-exporter scrape and has no cloud_* metrics.
	f, err := os.Open("testdata/jmx-sample.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := Parse(f)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Extract(s, ""); !errors.Is(err, ErrNoClusterIdentity) {
		t.Errorf("err = %v, want ErrNoClusterIdentity", err)
	}
}

// TestExtract_SkipsCheckWhenExpectedEmpty allows fixture-driven tests and
// future direct-scrape modes to opt out of the identity match.
func TestExtract_SkipsCheckWhenExpectedEmpty(t *testing.T) {
	scrape := loadScrape(t)
	if _, err := Extract(scrape, ""); err != nil {
		t.Fatalf("Extract with empty expected: %v", err)
	}
}

func loadScrape(t *testing.T) *Scrape {
	t.Helper()
	f, err := os.Open("testdata/croudng-sample.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s, err := Parse(f)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestUnixSecondsToTime exercises the conversion used for the operator's
// last-activity gauge; bad inputs must not produce garbage times.
func TestUnixSecondsToTime(t *testing.T) {
	cases := []struct {
		in       float64
		wantZero bool
	}{
		{0, true},
		{-1, true},
		{1778668425.5, false},
	}
	for _, c := range cases {
		got := unixSecondsToTime(c.in)
		if got.IsZero() != c.wantZero {
			t.Errorf("unixSecondsToTime(%v) zero=%v, want zero=%v", c.in, got.IsZero(), c.wantZero)
		}
	}
	// Spot-check a known value: 1778668425 → 2026-05-13T10:33:45Z
	got := unixSecondsToTime(1778668425)
	want := time.Date(2026, 5, 13, 10, 33, 45, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("conversion = %v, want %v", got, want)
	}
}
