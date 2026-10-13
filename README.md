# obsi — CrateDB Observer

A lightweight TUI monitoring tool for CrateDB clusters. Single binary, zero dependencies.

## What it answers

- **Why isn't this shard started?** The Shards tab says what the non-STARTED shards mean for the data (unavailable, fewer copies, rebalancing), gives the reason from `sys.allocations` per shard and node, and names the common causes with the statement that fixes them (`y` copies, `x` runs it).
- **Why is recovery so slow?** The recovery view (`v`) compares each running copy with the fastest time `max_bytes_per_sec` allows, measures what each node actually moved, names the node whose disk or network is the limit, and marks shards the balancer moves back and forth.
- **Will a restart copy everything?** The Overview shows `unassigned.node_left.delayed_timeout` across tables and partitions next to the longest time a node was away, and sets it for all tables or one.
- **Who changed what?** `c` on the Queries tab lists cluster and table settings, users and privileges, publications and subscriptions changed since obsi started, and before that as far as `sys.jobs_log` reaches, with old → new values.
- **Why is this query slow?** `E` shows its plan, then `EXPLAIN ANALYZE` with the bind values you type, suggested from partition values and `pg_stats`: time per phase and node, the slowest Lucene shard queries, everything copyable.
- **What's going on right now?** Alerts for nodes leaving, RED tables, disk and heap, failed snapshots and failing node checks; running queries with memory per query; the slowest and failed queries from `sys.jobs_log`; per-node metrics with sparklines.
- **Is replication keeping up?** The Replication tab shows, on a subscriber, each subscribed table as preparing, copying, in sync or FAILED with CrateDB's reason, with changes per second; on a publisher, each subscriber cluster with the operations it hasn't pulled yet and when its retention lease expires. Publications are set up there too, with a check of which users can read the chosen tables, and `y` copies a ticket that `s` on the subscriber turns into the subscription.
- **What does this partitioned table look like?** `enter` on the Tables tab lists its partitions with size bars, shard counts, health and delay.

It starts read-only, and it notices when a port-forward suddenly points at a different cluster.

## Install

Download a prebuilt binary from [Releases](https://github.com/WalBeh/obsi/releases) (Linux, macOS, Windows).

Or install via Go:

```bash
go install github.com/WalBeh/obsi@latest
```

Or build from source:

```bash
go build -o obsi .
```

## Usage

```bash
# Connect with URL
obsi https://admin:password@cluster.example.com:4200

# Save as a named profile for future use
obsi https://admin:password@cluster:4200 --profile prod

# Reconnect using profile (password stored in OS keyring)
obsi prod

# Flags can appear anywhere
obsi --skip-verify prod

# Check connectivity and permissions, then exit
obsi doctor prod

# Local dev (empty password auto-detected)
obsi http://localhost:4200

# Allow writes (SQL tab, settings editor) for this run only
obsi prod --read-write

obsi --version
```

Password resolution: `--password` flag > `OBSI_PASSWORD` env var > OS keyring > empty password > interactive prompt. The prompt only comes when the server refuses the login (401/403); a timeout or refused connection is reported as such.

obsi starts read-only: the SQL tab only runs statements starting with `SELECT`, `SHOW`, `EXPLAIN` or `WITH`, and `e` (SET GLOBAL) is refused. `K` still kills the selected query after its confirm, and the recovery throttle can be changed from the Shards recovery view; `KILL` typed in the SQL tab stays blocked. The status bar shows which mode you're in. Turn it off with `--read-write` for one run or `read_only = false` under `[connection]`. The SQL check looks at the first keyword only; a CrateDB user with just DQL privileges is the real guard.

## Tabs

| Key | Tab | What it shows |
|-----|-----|---------------|
| `1` | Overview | Cluster settings (inline editable), node-left delay across tables vs. the longest node absence (settable for all tables), health checks, node/zone topology, CrateDB version, table health, a replication summary when there is replication, last 10 snapshots |
| `2` | Nodes | Per-node metrics with sparklines, disk IO, thread pool pressure, watermark bars |
| `3` | Queries | Active queries with duration, memory + dominant operation, node, username, statement preview; slowest, failed and grouped queries from `sys.jobs_log`; config changes since start (`c`); explain a query (`E`) |
| `4` | Tables | Table list with replication markers (`⇠` pulled, `⇢` published), shard distribution, size stats, translog flush status, partition count, node-left delay (settable per table), health filter; `enter` lists a partitioned table's partitions |
| `5` | Shards | One-line verdict (data unavailable / fewer copies / rebalancing), non-STARTED shards with what each means and why it isn't allocated |
| `6` | SQL | Ad-hoc SQL queries with auto LIMIT, history, scrollable results (reads only, unless `--read-write`) |
| `7` | Replication | Logical replication: subscriptions with per-table state, shards, docs, changes/s and the last change seen; publications with their tables and the subscriber clusters holding leases, how many operations each is behind and when its lease expires; create, change and drop publications (`n`/`e`/`d`), copy a subscribe ticket (`y`); create a subscription from a pasted ticket (`s`) and drop one (`d`, type its name); the replication settings (`c`) and each published table's retention lease period (`l`); the `logical_replication` thread pool; sparklines of changes/s per table and BEHIND per subscriber, operations pulled/s, and what each finished initial copy moved |

## Keys

| Key | Action |
|-----|--------|
| `1-7` | Switch tabs |
| `tab` / `shift+tab` | Next/prev tab |
| `j/k` or `↑/↓` | Navigate |
| `s` | Cycle sort column (Nodes, Tables, Shards) |
| `/` | Search/filter |
| `esc` | Clear search |
| `e` | Edit cluster settings (Overview tab) |
| `f` | Toggle unhealthy table filter (Tables tab) |
| `enter` | Tables tab: partitions of the selected partitioned table — values, shards, records, size bar, health, node-left delay; `s` sorts, `y` copies, `esc` back |
| `e` | Tables tab: set the node-left delay (`unassigned.node_left.delayed_timeout`) of the selected table, partitions included, after a confirm |
| `K` | Kill selected query (Queries tab) |
| `i` | Operation details for selected query — per-op memory, full statement (Queries tab) |
| `y` | Yank selected query + operations to clipboard (Queries list or `i` modal, via OSC 52); on the Tables tab, `SHOW CREATE TABLE` of the selected table |
| `h` | Show/hide stuck queries (Queries tab) — queries running longer than 24h are hidden by default |
| `E` | Explain the selected query (Queries tab, live list and boards): `EXPLAIN` plan right away, `a` for `EXPLAIN ANALYZE` with the bind values you type (CrateDB doesn't keep them; `ctrl+n` takes a suggestion from partition values or `pg_stats`), `↑↓`/`pgup`/`pgdn` scroll the result, `y` copies everything. The analyze run is killed on `esc` or after `explain_timeout` |
| `S` | Toggle between live queries and the 20 slowest seen since obsi started (Queries tab) |
| `f` | Failed queries from `sys.jobs_log`, newest first (Queries tab) |
| `g` | Slowest statements from `sys.jobs_log`, grouped by exact text with count/max/avg (Queries tab) |
| `c` | Config changes from `sys.jobs_log`: cluster and table settings, users and privileges, publications and subscriptions, with old → new values (Queries tab) |
| `t` | Cycle throttle (normal/mild/heavy/paused) |
| `ctrl+r` / `R` / `F5` | Force refresh current tab |
| `r` | Reconnect to cluster |
| `L` | Toggle query log |
| `a` | Alerts raised since obsi started, firing and cleared |
| `v` | Shards tab: recovery view — running recoveries (from → to, size, elapsed vs. the fastest time the throttle allows, and whether something else is slower), recoveries finished while the tab was open with their actual MB/s, per node what it moved while busy and whether the node (not the throttle) is the limit, shards moved back to a node they left (`↩`), queued copies, recovery slots per node; `e` there edits the recovery throttle (set TRANSIENT, allowed in read-only mode) |
| `y` / `x` | Shards tab: copy / run the suggested fix. `x` asks first; in read-only mode it only runs `ALTER CLUSTER REROUTE RETRY FAILED` |
| `pgdn` / `pgup` (or `shift+↓` / `shift+↑`) | Scroll detail panel (Nodes tab) |
| `?` / `F1` | Keys for the active tab (`F1` also works while typing in the SQL editor or a search) |
| `q` | Quit |

## Doctor

Check connectivity and permissions before launching:

```bash
obsi doctor https://admin:pass@cluster:4200
```

## Profiles

Profiles store cluster connection details so you don't retype URLs.

```bash
# Save a profile (password goes to OS keyring)
obsi https://admin:pass@prod-cluster:4200 --profile prod

# List saved profiles
obsi --list-profiles

# Use a different config file (multi-client setups)
obsi --config ~/clients/acme.toml --profile prod
```

With no arguments, `obsi` connects to the last used profile.

## Configuration

Config file is created on first run at `~/.config/obsi/config.toml` (Linux) or `~/Library/Application Support/obsi/config.toml` (macOS).

```toml
last_profile = "prod"

[profiles.prod]
endpoint = "https://prod-cluster:4200"
username = "admin"

[profiles.staging]
endpoint = "https://staging-cluster:4200"
username = "crate"

[connection]
read_only = true     # default; false allows SQL writes and settings edits

[collectors.nodes]
interval = "5s"

[collectors.queries]
interval = "2s"

[collectors.shards]
interval = "30s"
allocations_interval = "30s"   # sys.allocations poll while shards aren't STARTED

[collectors.jobs_log]
interval = "15s"     # only polled while the slowest board is open

[collectors.changes]
interval = "10s"     # sys.jobs_log is a ring per node; longer misses more on busy clusters

[collectors.snapshots]
interval = "15m"     # sys.snapshots lists the repository (S3, Azure) on every read

[collectors.replication]
interval = "10s"     # pg_catalog lookups; sys.shards only for replicated tables

[tui]
alert_bell = false   # ring the terminal bell when an alert is raised
explain_timeout = "2m"  # EXPLAIN ANALYZE from the Queries tab is killed after this

[jmx]
# Set to the croudng endpoint to enable JVM/cAdvisor/operator metrics
# on CrateDB Cloud clusters. Empty disables the integration.
endpoint = ""        # e.g. "http://127.0.0.1:9275/metrics"
interval = "30s"
timeout  = "10s"
```

Collector/TUI/logging settings are global (shared across profiles).

## Features

- Failover-aware connection: works through load balancers, falls back to direct node IPs
- Node disappearance detection with "last seen" tracking
- Node checks and cluster health on the Overview: `sys.node_checks` folded over the nodes (shard limit, disk watermarks, recovery settings) with the nodes each failing check hits, and `sys.cluster_health` with pending tasks and missing/underreplicated shards. A RED cluster or a failing check raises an alert
- Cluster identity check: the heartbeat reads `sys.cluster`'s id, so a port-forward that comes back pointing at another cluster (same name or not) is noticed within one heartbeat. Polling and writes pause behind a notice until you switch (`enter`, clears obsi's history of the old cluster) or quit (`q`); if the original cluster comes back, obsi just continues
- Disk watermark visualization (low/high/flood markers on disk bars)
- Inline cluster settings editor (allocation, rebalance, recovery, watermarks, max shards), plus `unassigned.node_left.delayed_timeout` for all tables at once: a per-table setting (each partition keeps its own copy, blob tables can't change it) that decides whether a restarted node keeps its copies or they get rebuilt elsewhere
- CrateDB version display with mixed-version warning
- Translog flush monitoring: highlights shards exceeding the flush threshold
- Table health color-coding (RED/YELLOW/GREEN) with unhealthy-only filter
- Shard size skew detection
- CrateDB Cloud hostname shortening (`crate-data-hot-<uuid>-0` -> `data-hot-0`)
- Zone-aware topology display
- IO throughput and IOPS derived from cumulative counters
- Thread pool pressure monitoring (write/search/generic) with rejection delta tracking
- Query latency stats (avg/p90/max) in status bar
- Snapshots on the Overview: the last 10 from `sys.snapshots` with state, duration and failure count, or a warning when no repository is configured. Checks `sys.repositories` first and polls every 15 minutes, since listing a repository reads the bucket
- Alerts on transitions: a node leaving, lost connection, a table going YELLOW/RED, a failing `sys.checks` entry, disk past a watermark, heap above 85% (clears below 80%), the newest snapshot in a repository FAILED or PARTIAL. The newest one sits next to the tabs, the count in the status bar, `a` lists them. Each condition raises once until it clears
- Per-query memory accounting: `sys.jobs` joined with `sys.operations` shows the dominant operation and total `used_bytes` per running query; `i` opens a details modal, `y` yanks job + ops + statement to the clipboard. Stuck queries (running longer than 24h, typically abandoned cursors) are hidden by default; press `h` to show them
- Slowest queries since startup: `S` on the Queries tab shows the top 20 jobs by duration. Finished jobs come from `sys.jobs_log` with exact durations and errors; running ones from the `sys.jobs` poll. `sys.jobs_log` is an in-memory ring per node (`stats.jobs_log_size`), so jobs it no longer holds fall back to sampled durations, marked `≥` (short by up to one poll interval). Without `sys.jobs_log` (`stats.enabled = false`, no privileges) the whole board is sampled and the header says why. `f` lists failed queries, `g` groups by statement. `sys.jobs_log` is only polled while the board is open. obsi tags its own statements with `/* obsi */` and leaves them out of the Queries tab. Stuck queries are always left out
- Config changes flight recorder: `c` on the Queries tab lists `SET/RESET GLOBAL`, `ALTER CLUSTER`, `ALTER TABLE ... SET/RESET/OPEN/CLOSE/REROUTE`, `CREATE/ALTER/DROP USER/ROLE`, `GRANT`, `REVOKE`, `DENY` and `CREATE/ALTER/DROP PUBLICATION/SUBSCRIPTION` from `sys.jobs_log`, including failed attempts and whatever the log held before obsi started. Cluster settings are compared on every poll, tables when an `ALTER TABLE` names them, for old → new values; a cluster setting that changed with no statement in the log is listed as such. Partition-level `ALTER TABLE` gets no diff, and an unqualified table name is assumed to be in `doc`. The log is a ring per node (`stats.jobs_log_size`), so on a busy cluster statements can rotate out between two 10s polls; the board shows those stretches as possible gaps. Session `SET`s from drivers, schema DDL and `REFRESH`/`OPTIMIZE` are left out. Passwords and keys in statements, including the password in a `CREATE SUBSCRIPTION` connection URL, are replaced by `***` before obsi keeps them
- Logical replication (Replication tab): reads `pg_subscription`, `pg_subscription_rel`, `pg_publication` and `pg_publication_tables`, and `sys.shards` only for the replicated tables. A subscriber can't know how far behind the publisher it is, so it shows changes per second and when obsi last saw a change arrive; the lag lives on the publisher, in the retention leases each subscriber holds on the published primaries (`max_seq_no` minus the lease's `retaining_seq_no`). Subscribers are named by their cluster name. A failed table and a subscriber that hasn't renewed its leases for half of `soft_deletes.retention_lease.period` (12h by default, read from `SHOW CREATE TABLE`) raise an alert; once a lease expires the publisher may drop operations the subscriber still needs. Subscribed tables are marked read-only on the Tables tab. A subscribed table whose replicas this cluster can't place is flagged with both fixes: `number_of_replicas` isn't replicated, so an `ALTER TABLE` here holds for what exists, but each new partition arrives with the publisher's count; a range such as `'0-1'` on the publisher fits both clusters. A subscriber that pulls tables no publication lists any more is marked: in 6.4.5, after `DROP PUBLICATION` its subscribers keep replicating the tables they have until `DROP SUBSCRIPTION`, but new partitions and new columns stop arriving, so rows using a column added later are skipped there without an error and the subscriber diverges, while `ALTER PUBLICATION ... DROP TABLE` stops that table and makes it writable on the subscriber.
- Replication throughput (Replication tab): the settings give no rate, only ceilings that depend on the round trip, so obsi measures. On a subscriber: changes/s per table with a sparkline over the history (each character the busiest of its samples, from 0), the changes replicated since obsi started with ≈ bytes from the tables' average document size, and for an initial copy obsi watched finish the bytes copied, the time, and the per-shard rate from CrateDB's recovery record; when that rate means one round trip of 30ms or more for `max_concurrent_file_chunks` × `chunk_size`, a hint that a WAN link may be the limit. On a publisher: operations pulled/s per subscriber from how fast its leases advance, a BEHIND sparkline, and the total pulled. Copy progress while it runs isn't visible, and network or disk IO can't be told apart from other load. Terminals narrower than 120 columns drop the sparklines
- Replication settings (Replication tab): `replication.logical.ops_batch_size`, `reads_poll_duration`, `recovery.chunk_size`, `recovery.max_concurrent_file_chunks` and `cluster.routing.allocation.node_initial_primaries_recoveries` (which caps how many shards a node copies at once) on one line, non-defaults highlighted; `c` sets them (`SET GLOBAL` as `set_global_mode` says, with the same TRANSIENT check as the Overview) or resets them. Each publication lists its tables' `soft_deletes.retention_lease.period`, flagged under an hour and under `reads_poll_duration`; `l` changes it per table (`ALTER TABLE` without `ONLY`, so partitions too), with the longest a subscriber went without pulling as a guide. The `logical_replication` thread pool shows its threads, queue and rejections over the nodes. On the publisher, a table copied by files keeps its subscriber's lease at 0 until the first change, so BEHIND shows `?` for it instead of the whole table
- Subscriber setup (Replication tab, `--read-write`): `s` opens a form for host, port, mode (`pg_tunnel` on 5432 by default, or `sniff` on 4300), SSL mode, user, password and publications; pasting a subscribe ticket fills in the publication and the tables to expect. The review refuses when one of those tables exists here, and while another subscription has a table failed with "Relation already exists" (CrateDB 6.4.5 then tries to delete that local table and every subscription fails). The connection string goes to CrateDB as a bind parameter, so `sys.jobs_log` records `CONNECTION ?` and never the password. Two things seen on 6.4.5 are pointed out in the review: `sniff` doesn't check the password, and with `sslmode` `prefer` or `require` against a publisher certificate the subscriber doesn't trust, `CREATE SUBSCRIPTION` never answers (obsi gives up after a minute and says why). A refused connection or an unknown user fails right away; an unreachable host after CrateDB's 30s connect timeout. `d` on a subscription asks for its name before `DROP SUBSCRIPTION`, which turns its tables into regular writable tables for good
- Publisher setup (Replication tab, `--read-write`): `n` creates a publication from a name and a table list (or all tables), `e` changes its tables, `d` drops it. The review shows the SQL and checks which users may read every chosen table, resolved from `sys.privileges`, `sys.users` and `sys.roles` the way CrateDB does (own table, schema, cluster entry first, then roles, where a DENY wins), since the subscriber's user silently skips tables it can't read; and it flags a `soft_deletes.retention_lease.period` under an hour. A table already in another publication is added with `ALTER PUBLICATION ... ADD TABLE`, because `CREATE PUBLICATION ... FOR TABLE` refuses it. `y` copies a subscribe ticket: the `CREATE SUBSCRIPTION` to fill in (`pg_tunnel`, which only needs the publisher's PostgreSQL port), the tables, and the publisher's nodes with their ports, without a password Copy progress isn't shown in bytes: CrateDB lists shards being copied as UNASSIGNED without progress, so the tab counts started shards and the time since the copy began. A table that failed with "Relation already exists" gets a warning not to create another subscription first: CrateDB 6.4.5 then tries to delete the local table and every subscription on the cluster fails. A table that stays initializing for 5 minutes gets a hint: after a restart, CrateDB 6.4.5's metadata tracker can die and leave new tables there until the subscription is dropped and created again. Pausing a subscription isn't possible (`ALTER SUBSCRIPTION` isn't implemented in CrateDB). CrateDB lists publications and subscriptions only to their owner and to superusers, so obsi connected as another user says so and shows only that user's; reading `sys.shards` and `sys.cluster` needs DQL on schema `sys`. Each part (subscriptions, publications, shards, leases, settings) is read on its own: one that fails shows its error and keeps the last data
- Local 3-node test cluster with scenarios for the Shards tab, see [docs/shardlab.md](docs/shardlab.md)
- Optional JMX metrics for CrateDB Cloud (GC, memory pools, buffer pools, circuit breakers, per-query-type stats, network IO, per-device disk, container memory) via [`croudng`](https://github.com/crate/croudng) — see [docs/jmx.md](docs/jmx.md)

## JMX metrics (CrateDB Cloud)

obsi can surface JVM, container, and CrateDB-internal metrics by scraping
the local Prometheus endpoint served by `croudng clusters metrics --watch`.
Quick setup:

```bash
# 1. Run croudng in a separate terminal
croudng clusters metrics -n <cluster> --profile <profile> --watch

# 2. Add to your obsi config
[jmx]
endpoint = "http://127.0.0.1:9275/metrics"

# 3. Start obsi as usual — new sections appear on Overview and Nodes detail
```

See [docs/jmx.md](docs/jmx.md) for the full integration guide, including the
cluster-name safety guard, the Grafana-aligned GC math, and what's included
vs. deliberately skipped.

## License

MIT
