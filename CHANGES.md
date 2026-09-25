# Changelog

All notable changes to obsi are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Added

- **Recovery view** on the Shards tab (`v`): running recoveries with
  source and target node, bytes to copy, how long obsi has seen them and
  the shortest time the `indices.recovery.max_bytes_per_sec` throttle
  allows, plus the copies waiting for a slot and each node's incoming /
  outgoing recoveries against `node_concurrent_recoveries`. CrateDB
  doesn't report progress for copies recovering from another node, so
  there's no percentage. `e` there edits `max_bytes_per_sec`,
  `node_concurrent_recoveries` and `cluster_concurrent_rebalance` with an
  old → new confirm, `ctrl+r` resets them. Set TRANSIENT, so a bump made
  during an incident is gone after a full cluster restart, and allowed in
  read-only mode.

- **Shard fixes.** The Shards tab names the common causes under the
  verdict, one line per cause with the shard count: allocation disabled
  cluster-wide, a table filter keeping shards off nodes, more replicas
  than nodes, the retry limit, disk watermarks, a node that left and is
  still waited for, recovery throttling. Where a statement fixes it
  (`RESET GLOBAL "cluster.routing.allocation.enable"`, `ALTER TABLE ...
  RESET (...)`, `ALTER CLUSTER REROUTE RETRY FAILED`, ...) it's shown
  below; `y` copies it, `x` runs it after a confirm. Read-only mode only
  runs RETRY FAILED. While the tab is open it also checks, once a minute,
  for STARTED shards that a table filter sends away from their node with
  nowhere to go, which otherwise looks all green.

- **Shards verdict.** The Shards tab opens with what the non-STARTED
  shards mean for the data: "Data unavailable: 2 primaries unassigned",
  "Fewer copies: ...", "Rebalancing: 12 shards moving, all data
  available". Each shard is bucketed (primary/replica unassigned or
  recovering, restoring, moving) and the detail panel says what that
  bucket means and where a moving shard is going. The list now comes
  from `sys.allocations`: on CrateDB 6.3 `sys.shards` shows a recovering
  copy as UNASSIGNED without a node and a lost primary as a replica.
  `sys.allocations` is polled every 30s (`allocations_interval` under
  `[collectors.shards]`): its per-node texts ran to 1.5MB per poll with
  1200 unassigned copies.

- **Snapshots** section on the Overview: the last 10 snapshots with
  state, duration and failures, or "no snapshot repository configured".
  Polled every 15 minutes (`[collectors.snapshots]`) because listing
  reads the repository bucket. A FAILED or PARTIAL newest snapshot in a
  repository raises an alert.

- **Alerts.** A node leaving, obsi losing the connection, a table turning
  YELLOW or RED (partitions folded into one alert per table), a failing
  `sys.checks` entry, disk past a percentage watermark and heap above 85%
  raise an alert once and clear when the condition goes away. Newest one
  next to the tabs, count in the status bar, `a` for the history.
  `alert_bell = true` under `[tui]` rings the terminal bell.

- **Read-only by default.** The SQL tab only runs `SELECT`, `SHOW`,
  `EXPLAIN` and `WITH` statements, and the settings editor is refused
  with a hint. `K` still kills after its confirm; a restart just to kill
  a runaway query is too slow. `--read-write` lifts it for one run,
  `read_only = false` under `[connection]` for good. Status bar shows
  `read-only` or `read-write`.

- **sys.jobs_log on the slowest board.** Finished jobs now show exact
  durations and errors from `sys.jobs_log`; running jobs still come from
  the `sys.jobs` poll. Jobs the log no longer holds keep their sampled
  duration, marked `≥`, and without `sys.jobs_log` the board stays fully
  sampled with the reason in the header. `f` lists failed queries, `g`
  groups by exact statement (count / max / avg / failed). Polled every
  15s only while the board is open. obsi's own statements are tagged
  `/* obsi */` and left out of the Queries tab. `--doctor` checks
  `sys.jobs_log`.

- **Key help** on every tab. `?` opens a modal listing the keys the active
  tab handles (the Queries list differs between live and slowest views)
  plus the global ones. `?` is plain text in the SQL editor and search
  prompts, so `F1` opens it there too. Status bar now shows `?:help`.

- **Per-query memory accounting** in the Queries tab. The active-queries
  collector now `LEFT JOIN`s `sys.jobs` with `sys.operations` on `job_id`,
  surfacing how much memory each running query holds and which operation
  is responsible.
  - New `MEMORY` column shows total `used_bytes` for the job plus the name
    of the dominant operation (e.g. `42.3MB COLLECT`). Jobs still in their
    planning phase (no `sys.operations` rows yet) survive the join and
    show `—`.
  - `i` on a selected query opens an operation-details modal: per-op
    memory, node, and offset-from-job-start, plus the full untruncated
    statement.
  - `y` yanks the job header + operations table + full statement to the
    system clipboard via OSC 52 (works over SSH). Available both from
    the list (acts on the highlighted row) and from inside the `i` modal.

- **Hide stuck queries** in the Queries tab. Sessions running longer
  than 24h — typically abandoned `DECLARE ... CURSOR WITH HOLD`
  cursors — are hidden by default so they don't drown out queries an
  operator actually cares about. The header surfaces the hidden count
  (`N active queries (M stuck hidden — press h to show)`). Press `h`
  to toggle visibility; selection re-anchors to the same job ID across
  the toggle so the cursor doesn't jump to an unrelated row.

- **Slowest queries** in the Queries tab. `S` flips to a board of the 20
  slowest jobs seen in `sys.jobs` since obsi started, running and
  finished mixed, with peak memory. Built from the existing poll, so
  durations are as of the last poll that saw the job (short by up to one
  interval, running rows step once per poll) and sub-interval queries
  never show up. Stuck
  queries (>24h) are always excluded. `i` and `y` work on finished jobs.

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

### Fixed

- obsi crashed about 5s after opening the Shards tab whenever a shard
  wasn't STARTED: the 5s fast-path query has no translog columns and
  parsing its rows ran past the end.
- The Shards tab never showed why a shard wasn't allocated: it asked
  `sys.allocations` for an `explanations` column that doesn't exist and
  quietly fell back to a query without reasons. It now shows the
  explanation plus each node's refusal, nodes with the same reason on one
  line. Reasons on partitioned tables also no longer mix across
  partitions.
- Non-ASCII statements, errors and table names (e.g. `größe`, `表名`) were
  cut mid-character when truncated or wrapped, leaving broken glyphs, and
  sometimes shortened although they fit. Truncation and wrapping now count
  characters instead of bytes.
- A `[collectors.<name>]` section without `enabled` turned that collector
  off (the README's own example did this), and one without `interval` ran
  it back to back on a 0s timer. Omitted fields now fall back to the
  defaults; an explicit `enabled = false` still disables it.

### Notes

- GC pause math matches the existing Grafana dashboards
  (`rate(jvm_gc_collection_seconds_sum)/rate(jvm_gc_collection_seconds_count)`
  for pause duration; `rate(jvm_gc_collection_seconds_count)` for
  frequency), pinned by tests in `internal/store/store_test.go`.
- True per-event GC pause percentiles (p90, etc.) are not computable
  from croudng's cumulative counters and are deliberately not claimed.
