# Changelog

All notable changes to obsi are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added

- **JMX metrics integration** for CrateDB Cloud clusters, via `croudng`'s
  local Prometheus endpoint. Opt-in via a single config knob; disabled by
  default with no change for non-Cloud setups. See `docs/jmx.md` for
  setup, math notes, and the cluster-name safety guard.
  - Overview tab: cluster-level circuit-breaker status (parent, request,
    query, fielddata, in_flight_requests, jobs_log, operations_log) and
    per-query-type activity (Select / Insert / Update / Delete / DDL /
    Management / …) with total count, failure rate, and avg duration.
  - Nodes detail: container memory vs JVM heap (exposes "native" delta),
    network rx/tx with sparklines, per-device disk rate breakdown,
    plus GC (cumulative + Grafana-aligned recent weighted mean pause,
    max, and frequency, with sparkline), memory pools (Eden / Old Gen /
    Survivor as % of heap; non-heap value-only), and buffer pools
    (direct / mapped).
  - Scroll the Nodes detail panel with `pgdn` / `pgup` (or `shift+↓` /
    `shift+↑`) when JMX content exceeds the reserved height.
  - Cluster-name safety guard: scrapes are matched against
    `sys.cluster.name`; a mismatch disables the collector permanently
    rather than risk attributing foreign metrics to obsi's nodes.

### Notes

- GC pause math matches the existing Grafana dashboards
  (`rate(jvm_gc_collection_seconds_sum)/rate(jvm_gc_collection_seconds_count)`
  for pause duration; `rate(jvm_gc_collection_seconds_count)` for
  frequency), pinned by tests in `internal/store/store_test.go`.
- True per-event GC pause percentiles (p90, etc.) are not computable
  from croudng's cumulative counters and are deliberately not claimed.
