# Changelog

All notable changes to obsi are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
