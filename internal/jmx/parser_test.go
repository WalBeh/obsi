package jmx

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestParse_CroudngFixture exercises the parser against a real scrape captured
// from `croudng clusters metrics --watch`, asserting the metadata header and a
// representative sample.
func TestParse_CroudngFixture(t *testing.T) {
	f, err := os.Open("testdata/croudng-sample.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	s, err := Parse(f)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got, want := s.Meta.ScrapedAt, time.Date(2026, 5, 13, 10, 34, 19, 0, time.UTC); !got.Equal(want) {
		t.Errorf("ScrapedAt = %v, want %v", got, want)
	}
	if s.Meta.Cached {
		t.Errorf("Cached = true, want false (fixture has no 'served from cache')")
	}
	if s.Meta.RateLimited {
		t.Errorf("RateLimited = true, want false")
	}
	if s.Meta.UpstreamAge != 0 {
		t.Errorf("UpstreamAge = %v, want 0", s.Meta.UpstreamAge)
	}

	if len(s.Samples) == 0 {
		t.Fatal("no samples parsed")
	}

	// cloud_clusters_health carries the cluster identity used by the safety
	// check; verify it parsed with all expected labels.
	got := findSample(s.Samples, "cloud_clusters_health", nil)
	if got == nil {
		t.Fatal("cloud_clusters_health not found")
	}
	if got.Labels["cluster_name"] != "devbrain" {
		t.Errorf("cluster_name = %q, want devbrain", got.Labels["cluster_name"])
	}
	if got.Labels["cluster_id"] != "5e52d9b3-3b92-4568-a3a4-620ed9d1d445" {
		t.Errorf("cluster_id = %q", got.Labels["cluster_id"])
	}
	if got.Timestamp == 0 {
		t.Error("timestamp not parsed")
	}
}

// TestParse_JMXSampleFixture exercises the parser against a raw JMX-exporter
// scrape (the upstream format croudng wraps), which includes HELP/TYPE lines,
// scientific notation, trailing commas in label sets, and negative values.
func TestParse_JMXSampleFixture(t *testing.T) {
	f, err := os.Open("testdata/jmx-sample.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	s, err := Parse(f)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !s.Meta.ScrapedAt.IsZero() {
		t.Errorf("ScrapedAt should be zero for raw JMX scrape (no croudng header)")
	}

	// Scientific notation: 5.4411008E7
	got := findSample(s.Samples, "jvm_memory_pool_allocated_bytes_total",
		map[string]string{"pool": "CodeHeap 'profiled nmethods'"})
	if got == nil {
		t.Fatal("jvm_memory_pool_allocated_bytes_total{pool=CodeHeap 'profiled nmethods'} not found")
	}
	if got.Value != 5.4411008e7 {
		t.Errorf("value = %v, want 5.4411008e7", got.Value)
	}

	// Negative value: jvm_memory_bytes_max{area="nonheap"} = -1.0
	got = findSample(s.Samples, "jvm_memory_bytes_max", map[string]string{"area": "nonheap"})
	if got == nil || got.Value != -1.0 {
		t.Errorf("jvm_memory_bytes_max{nonheap} = %v, want -1", got)
	}

	// No labels: process_open_fds = 258
	got = findSample(s.Samples, "process_open_fds", nil)
	if got == nil || got.Value != 258 {
		t.Errorf("process_open_fds = %v, want 258", got)
	}
}

// TestParse_CroudngHeaderVariants covers the optional tokens in the croudng
// metadata line. Real scrapes can appear with any combination of these.
func TestParse_CroudngHeaderVariants(t *testing.T) {
	cases := []struct {
		name string
		line string
		want ScrapeMeta
	}{
		{
			name: "fresh scrape",
			line: "# croudng: scraped at 2026-05-13T10:34:19Z, upstream latency 4.511s",
			want: ScrapeMeta{ScrapedAt: time.Date(2026, 5, 13, 10, 34, 19, 0, time.UTC)},
		},
		{
			name: "cached + age",
			line: "# croudng: served from cache, scraped at 2026-05-13T10:08:09Z, upstream latency 1.322s, age 16s",
			want: ScrapeMeta{
				ScrapedAt:   time.Date(2026, 5, 13, 10, 8, 9, 0, time.UTC),
				UpstreamAge: 16 * time.Second,
				Cached:      true,
			},
		},
		{
			name: "rate limited",
			line: "# croudng: served from cache, scraped at 2026-05-13T10:08:09Z, age 45s (upstream rate limited)",
			want: ScrapeMeta{
				ScrapedAt:   time.Date(2026, 5, 13, 10, 8, 9, 0, time.UTC),
				UpstreamAge: 45 * time.Second,
				Cached:      true,
				RateLimited: true,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(strings.NewReader(c.line + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if s.Meta != c.want {
				t.Errorf("meta = %+v, want %+v", s.Meta, c.want)
			}
		})
	}
}

// TestParse_Errors verifies the parser surfaces a line number and rejects
// malformed input rather than panicking.
func TestParse_Errors(t *testing.T) {
	cases := []string{
		`metric_no_value`,
		`metric{bad_labels 1`,
		`metric{name=missing_quote} 1`,
		`metric 1 not_a_number`,
		`metric notafloat`,
	}
	for _, in := range cases {
		if _, err := Parse(strings.NewReader(in + "\n")); err == nil {
			t.Errorf("expected error for %q", in)
		}
	}
}

func findSample(samples []Sample, name string, matchLabels map[string]string) *Sample {
	for i := range samples {
		s := &samples[i]
		if s.Name != name {
			continue
		}
		ok := true
		for k, v := range matchLabels {
			if s.Labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			return s
		}
	}
	return nil
}
