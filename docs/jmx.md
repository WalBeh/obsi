# JMX metrics integration (via croudng)

obsi can ingest JVM, cAdvisor and crate-operator metrics for **CrateDB Cloud**
clusters by scraping the Prometheus endpoint served by
[`croudng clusters metrics --watch`](https://github.com/crate/croudng).

The integration is opt-in, single-endpoint, and disabled by default.

---

## What it adds

Surfaced in the TUI when JMX is enabled:

| Tab | Section | What it shows |
|-----|---------|---------------|
| **Overview** | Circuit Breakers | Max used/limit across pods + total trips for each CrateDB breaker (parent / request / query / fielddata / in_flight_requests / jobs_log / operations_log) |
| **Overview** | Query Activity | Per-type lifetime counts, failure count + rate, avg duration ms (Select / Insert / Update / Delete / DDL / Management / …) |
| **Nodes detail** | Container memory | Pod RSS vs JVM heap, with the "native" delta exposed — the off-heap leak signal that heap alone can't show |
| **Nodes detail** | Net rx/tx | Cumulative network rates per pod with sparkline (no `sys.nodes` equivalent) |
| **Nodes detail** | Per-device disk | Per-block-device read/write rate for pods with multiple data volumes (sda/sdb/sdc/…) |
| **Nodes detail** | GC | Per-collector cumulative + recent weighted mean pause, max, and frequency, with sparkline |
| **Nodes detail** | Memory pools | Eden / Old Gen / Survivor as % of heap; Metaspace / Compressed Class / CodeHeap value-only |
| **Nodes detail** | Buffer pools | Direct + mapped — Lucene off-heap + mmap |

The Nodes detail panel can scroll with `pgup` / `pgdn` (or `shift+up` / `shift+down`)
when content exceeds the reserved height.

---

## Setup

### 1. Run `croudng` against your cluster

```bash
croudng clusters metrics -n <cluster> --profile <profile> --watch
```

This serves a Prometheus endpoint on `http://127.0.0.1:9275/metrics` by
default. Leave it running in a separate terminal.

obsi does **not** launch `croudng` itself — keeping them independent
avoids version-coupling and lets you swap clusters or restart the proxy
without touching obsi.

### 2. Point obsi at it

Add to your obsi config (`~/.config/obsi/config.toml` on Linux,
`~/Library/Application Support/obsi/config.toml` on macOS):

```toml
[jmx]
endpoint = "http://127.0.0.1:9275/metrics"
interval = "30s"   # default; configurable
timeout  = "10s"   # default
```

An empty `endpoint` (the default) disables the integration completely —
non-Cloud setups need no further changes and see no new sections in the
TUI.

### 3. Start obsi

```bash
obsi <your-cloud-profile>
```

If `croudng` isn't running yet, obsi will log a one-line reminder and
keep retrying at the configured interval:

```
WARN  JMX endpoint unreachable; start croudng in another terminal:
      hint: croudng clusters metrics -n <cluster> --profile <profile> --watch
```

---

## The cluster-name safety guard

Each scrape carries `cloud_clusters_health{cluster_name=...}`. obsi compares
that label against `sys.cluster.name` on the connection it's bootstrapped
against. If they don't match — for example, croudng is pointed at cluster
`devbrain` while obsi is connected to cluster `xdemo3` — the JMX collector
is **disabled for the rest of the process lifetime** and an error logged:

```
ERROR JMX collector disabled: scrape is for a different cluster than obsi
      is connected to: scrape="devbrain" obsi="xdemo3"
```

This is intentional. Attributing one cluster's metrics to another cluster's
nodes would silently corrupt every panel and is the kind of bug operators
spend hours chasing. Restart obsi after fixing the mismatch to re-enable.

Cluster identity is by name (`cluster_name` label). Across organisations
with the same cluster name this is a blind spot — clusters within the
same control plane don't share names.

---

## Polling cadence

Default is **30 s**. The reason: croudng wraps the CrateDB Cloud REST
API which:

- caches scrapes for at least ~15 s
- may serve up to ~1 min stale
- rate-limits at roughly 1/min

Polling faster than 30 s burns work to see the same numbers. The
`croudng` header on each scrape (`# croudng: scraped at ..., age 16s`)
is parsed and tracked but not currently displayed; the obsi-side
interval is the relevant tuning knob.

---

## Metric math

GC stats are computed to match the existing Grafana dashboards used by
the CrateDB team:

- **Recent mean pause** = `Σ Δseconds / Σ Δcount * 1000`
  ≡ `rate(jvm_gc_collection_seconds_sum) / rate(jvm_gc_collection_seconds_count)`
  over the visible window (the sparkline ring; ~1 hr at defaults).
  This is the **weighted** mean — outlier intervals with a single long
  pause don't skew the average upward the way an unweighted mean would.

- **Recent rate** = `Σ Δcount / window_seconds`
  ≡ `rate(jvm_gc_collection_seconds_count)`.
  Computed from per-sample timestamps so a croudng outage doesn't fake
  the rate by treating gaps as full intervals. Unit auto-scales (`/s`,
  `/min`, `/hr`) so a sparse Old-Gen doesn't show as `0.00/s`.

- **Max pause** = the worst per-interval average observed in the window.
  Note: this is the max of *averages*, not the max of *individual
  pauses* — croudng exposes only cumulative counters, not per-event
  pause data, so a true per-event p90/max is not computable from this
  source. We do not claim percentiles.

Tests in `internal/store/store_test.go` pin the math so future changes
don't silently drift away from Grafana.

---

## What's deliberately NOT integrated

- `crate_is_master`, `crate_roles`, `crate_cluster_state_version`,
  `crate_node{name="shard_*"}` — already in `sys.cluster` / `sys.shards`
- `cratedb_max_shards_per_node` — already managed by the inline settings
  editor on the Overview tab
- `cratedb_num_large_translogs` — obsi already computes translog overflow
  from `sys.shards.translog_stats`
- `cloud_clusters_health` — used only for the cluster-name safety check
- Various JVM metrics (`jvm_classes_*`, `jvm_threads_state`,
  `jvm_threads_deadlocked`) — low operational value, rarely actionable

These are dropped on read in the extractor, not surfaced anywhere.

---

## On-prem / self-hosted clusters

Out of scope for v1. The parser, store, and TUI are source-agnostic —
adding a direct JMX-exporter scrape mode later is mostly a config knob
and a second collector that bypasses the croudng-specific header
parsing. Until then, on-prem users get the existing `sys.nodes` /
`sys.shards` coverage without the JMX additions.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| No JMX sections in TUI | `[jmx]` block absent or `endpoint=""` | Add the block, restart obsi |
| `WARN JMX endpoint unreachable` | croudng not running | Start it, obsi will pick up at the next interval |
| `ERROR JMX collector disabled: ... cluster mismatch` | croudng points at a different cluster than obsi | Restart croudng with the correct `-n <cluster>` then restart obsi |
| `recent — ` next to GC collector | Not enough data yet (need ≥2 scrapes), or collector hasn't fired in the window | Wait for the next scrape; for Old Gen this is expected on healthy clusters |
| Per-device disk section missing | All devices have zero recent activity | Activity will appear once the cluster does real IO |
