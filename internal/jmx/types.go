// Package jmx parses Prometheus text-format scrapes served by croudng and
// extracts JVM and CrateDB metrics for obsi.
//
// Only the subset of the Prometheus exposition format that croudng emits is
// supported: HELP/TYPE comments are tolerated and ignored, label values use
// the standard backslash-escape rules, and timestamps are optional.
package jmx

import "time"

// Sample is one Prometheus text-format data point.
type Sample struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp int64 // milliseconds since epoch; 0 if absent
}

// ScrapeMeta is extracted from the croudng-specific header line, if present:
//
//	# croudng: served from cache, scraped at 2026-05-13T10:08:09Z, upstream latency 1.322s, age 16s (upstream rate limited)
//
// Fields are zero-valued when the corresponding token is absent.
type ScrapeMeta struct {
	ScrapedAt   time.Time
	UpstreamAge time.Duration
	Cached      bool
	RateLimited bool
}

// Scrape is the parsed result of a Prometheus exposition.
type Scrape struct {
	Meta    ScrapeMeta
	Samples []Sample
}
