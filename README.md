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
```

Password resolution: `--password` flag > `OBSI_PASSWORD` env var > OS keyring > empty password > interactive prompt.

## Tabs

| Key | Tab | What it shows |
|-----|-----|---------------|
| `1` | Overview | Cluster settings (inline editable), health checks, node/zone topology, CrateDB version, table health |
| `2` | Nodes | Per-node metrics with sparklines, disk IO, thread pool pressure, watermark bars |
| `3` | Queries | Active queries with duration, node, username, statement preview |
| `4` | Tables | Table list with shard distribution, size stats, translog flush status, health filter |
| `5` | Shards | Shard allocation problems, recovery progress, relocations |
| `6` | SQL | Ad-hoc SQL queries with auto LIMIT, history, scrollable results |

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
| `t` | Cycle throttle (normal/mild/heavy/paused) |
| `ctrl+r` / `R` / `F5` | Force refresh current tab |
| `r` | Reconnect to cluster |
| `L` | Toggle query log |
| `?` | Help |
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

[collectors.nodes]
interval = "5s"

[collectors.queries]
interval = "2s"

[collectors.shards]
interval = "30s"
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

## License

MIT
