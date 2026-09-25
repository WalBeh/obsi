# obsi — CrateDB Observer

A lightweight TUI monitoring tool for CrateDB clusters. Single binary, zero dependencies.

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
obsi prod --doctor --skip-verify

# Local dev (empty password auto-detected)
obsi http://localhost:4200

# Allow writes (SQL tab, settings editor) for this run only
obsi prod --read-write
```

Password resolution: `--password` flag > `OBSI_PASSWORD` env var > OS keyring > empty password > interactive prompt.

obsi starts read-only: the SQL tab only runs statements starting with `SELECT`, `SHOW`, `EXPLAIN` or `WITH`, and `e` (SET GLOBAL) is refused. `K` still kills the selected query after its confirm; `KILL` typed in the SQL tab stays blocked. The status bar shows which mode you're in. Turn it off with `--read-write` for one run or `read_only = false` under `[connection]`. The SQL check looks at the first keyword only; a CrateDB user with just DQL privileges is the real guard.

## Tabs

| Key | Tab | What it shows |
|-----|-----|---------------|
| `1` | Overview | Cluster settings (inline editable), health checks, node/zone topology, CrateDB version, table health, last 10 snapshots |
| `2` | Nodes | Per-node metrics with sparklines, disk IO, thread pool pressure, watermark bars |
| `3` | Queries | Active queries with duration, memory + dominant operation, node, username, statement preview |
| `4` | Tables | Table list with shard distribution, size stats, translog flush status, health filter |
| `5` | Shards | One-line verdict (data unavailable / fewer copies / rebalancing), non-STARTED shards with what each means and why it isn't allocated |
| `6` | SQL | Ad-hoc SQL queries with auto LIMIT, history, scrollable results (reads only, unless `--read-write`) |

## Keys

| Key | Action |
|-----|--------|
| `1-6` | Switch tabs |
| `tab` / `shift+tab` | Next/prev tab |
| `j/k` or `↑/↓` | Navigate |
| `s` | Cycle sort column (Nodes, Tables, Shards) |
| `/` | Search/filter |
| `esc` | Clear search |
| `e` | Edit cluster settings (Overview tab) |
| `f` | Toggle unhealthy table filter (Tables tab) |
| `K` | Kill selected query (Queries tab) |
| `i` | Operation details for selected query — per-op memory, full statement (Queries tab) |
| `y` | Yank selected query + operations to clipboard (Queries list or `i` modal, via OSC 52) |
| `h` | Show/hide stuck queries (Queries tab) — queries running longer than 24h are hidden by default |
| `S` | Toggle between live queries and the 20 slowest seen since obsi started (Queries tab) |
| `f` | Failed queries from `sys.jobs_log`, newest first (Queries tab) |
| `g` | Slowest statements from `sys.jobs_log`, grouped by exact text with count/max/avg (Queries tab) |
| `t` | Cycle throttle (normal/mild/heavy/paused) |
| `ctrl+r` / `R` / `F5` | Force refresh current tab |
| `r` | Reconnect to cluster |
| `L` | Toggle query log |
| `a` | Alerts raised since obsi started, firing and cleared |
| `v` | Shards tab: recovery view — running recoveries (from → to, size, time at the throttle limit), queued copies, recovery slots per node |
| `y` / `x` | Shards tab: copy / run the suggested fix. `x` asks first; in read-only mode it only runs `ALTER CLUSTER REROUTE RETRY FAILED` |
| `pgdn` / `pgup` (or `shift+↓` / `shift+↑`) | Scroll detail panel (Nodes tab) |
| `?` / `F1` | Keys for the active tab (`F1` also works while typing in the SQL editor or a search) |
| `q` | Quit |

## Doctor

Check connectivity and permissions before launching:

```bash
obsi https://admin:pass@cluster:4200 --doctor
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

[collectors.snapshots]
interval = "15m"     # sys.snapshots lists the repository (S3, Azure) on every read

[tui]
alert_bell = false   # ring the terminal bell when an alert is raised

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
- Disk watermark visualization (low/high/flood markers on disk bars)
- Inline cluster settings editor (allocation, rebalance, recovery, watermarks, max shards)
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
