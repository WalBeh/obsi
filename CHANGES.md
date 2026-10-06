# Changelog

All notable changes to obsi are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Changed

- **Recovery view says whether the throttle is the bottleneck.** RUNNING
  and AT LIMIT are now ELAPSED (since obsi first saw the recovery) and
  FASTEST (the size at the copy's share of `max_bytes_per_sec`), with a
  STATUS: once ELAPSED passes FASTEST it says the copy is slower than the
  throttle, i.e. disk, network or replay limit it and raising the throttle
  won't help. Below the running list, recoveries that finished while the
  Shards tab was open show what the new copy recorded: time taken, bytes
  copied, the actual MB/s against its share, and whether it was rebuilt
  from operations instead of files.

### Fixed

- The recovery view split `max_bytes_per_sec` over the receiving node's
  recoveries only. It caps a node's incoming and outgoing recovery traffic
  together, so a node sending one copy and receiving another gives each
  half (seen on the shardlab); the busier of the two nodes counts now.
- A moving replica was shown copying from the node it leaves. It copies
  from the primary, so FROM and SIZE are the primary's now.

## [0.3.5] - 2026-10-06

### Fixed

- A moving shard's detail on the Shards tab showed a recovery bar such as
  "0.0% stage: DONE". That was the source copy's own, long finished
  recovery; CrateDB doesn't list the receiving copy while a shard moves.
  The detail now says no progress is reported for a move.

## [0.3.4] - 2026-10-06

### Fixed

- Changing a setting in the Overview editor (`e`) could say ✓ and change
  nothing: a TRANSIENT value wins over the PERSISTENT one obsi sets, e.g.
  a `max_bytes_per_sec` left by the recovery throttle form. obsi now
  compares the value in effect before and after; if it didn't move, it
  sets the value TRANSIENT as well and says which transient value was in
  the way.

## [0.3.3] - 2026-10-02

### Added

- **Node checks and cluster health on the Overview.** `sys.node_checks`
  is folded over the nodes: each failing check is listed once with the
  nodes it fails on, e.g. the shard limit (90% of
  `cluster.max_shards_per_node`) or a disk watermark, with a hint for what
  to do. A `Cluster` block shows `sys.cluster_health`: health, pending
  cluster-state tasks, missing and underreplicated shards (`unknown` when
  CrateDB can't count them). A RED cluster and failing node checks raise
  alerts with the check's short name; the disk checks don't, obsi
  already alerts on disk usage. Acknowledged checks are left out.
  `--doctor` checks both tables.

### Fixed

- Severity 1 checks were labelled INFO. CrateDB calls that level LOW, so
  they show as `[LOW]` now; 2 and 3 stay `[WARN]` and `[CRIT]`.

## [0.3.2] - 2026-09-29

### Added

- **Notices a different cluster behind the endpoint**, e.g. a k8s
  port-forward restarted against another cluster. The heartbeat reads
  `sys.cluster`'s id (names like `crate` repeat), and on a mismatch obsi
  stops polling and refuses writes, raises an alert and asks: `enter`
  switches to the new cluster and drops what obsi kept about the old one,
  `q` quits. If the old cluster comes back first, obsi carries on.

## [0.3.1] - 2026-09-25

### Added

- `obsi --version` (or `-v`). The release builds always set the version,
  but nothing read it.

## [0.3.0] - 2026-09-25

### Added

- **Shards tab that explains itself.** A one-line verdict on what the
  non-STARTED shards mean for the data (unavailable, fewer copies,
  rebalancing), the reason each shard isn't allocated, and the common
  causes named with the statement that fixes them: `y` copies it, `x`
  runs it after a confirm. STARTED shards stuck on a node their filter
  excludes are flagged too.
- **Recovery view** (`v` on Shards): running and queued recoveries, slots
  per node and the throttle, which `e` edits in place.
- **Config changes board** (`c` on Queries): who changed cluster or table
  settings, users or privileges and when, with old → new values, read
  from `sys.jobs_log`. Config statements get their own colour in all
  Queries views, and passwords are shown as `***`.
- **Alerts** for a node leaving, a lost connection, tables going
  YELLOW/RED, failing checks, disk and heap pressure and failed snapshots.
  Newest next to the tabs, history on `a`, optional terminal bell.
- **Snapshots** on the Overview: the last 10 with state and failures, or a
  warning when there's no repository.
- **Slowest queries** (`S` on Queries), exact from `sys.jobs_log`, plus
  failed queries (`f`) and statements grouped by text (`g`).
- **Key help** on every tab with `?` or `F1`.
- A local 3-node Docker cluster with scenarios for testing the Shards tab,
  see `docs/shardlab.md`.

### Changed

- **obsi starts read-only.** The SQL tab only runs reads and the settings
  editor is refused; `K`, the recovery throttle and `REROUTE RETRY FAILED`
  still work. `--read-write` or `read_only = false` under `[connection]`
  lifts it.

### Fixed

- The Shards tab crashed a few seconds after opening whenever a shard
  wasn't STARTED, and never showed why a shard wasn't allocated.
- Non-ASCII statements and names were cut mid-character.
- A `[collectors.<name>]` section without `enabled` or `interval` turned
  the collector off or ran it back to back.

## [0.2.0] - 2026-05-20

### Added

- **JMX metrics** for CrateDB Cloud clusters via `croudng`: circuit
  breakers, query-type stats, GC, memory and buffer pools, container
  memory, network and disk rates. Off unless `[jmx] endpoint` is set, see
  `docs/jmx.md`.
- **Per-query memory** in the Queries tab, `i` for the operations behind
  it and `y` to copy a query to the clipboard.
- Queries running longer than 24h (abandoned cursors) are hidden, `h`
  shows them.

### Notes

- GC pause math matches the existing Grafana dashboards
  (`rate(jvm_gc_collection_seconds_sum)/rate(jvm_gc_collection_seconds_count)`
  for pause duration; `rate(jvm_gc_collection_seconds_count)` for
  frequency), pinned by tests in `internal/store/store_test.go`.
- True per-event GC pause percentiles (p90, etc.) are not computable
  from croudng's cumulative counters and are deliberately not claimed.
