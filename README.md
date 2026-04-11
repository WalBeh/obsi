# obsi — CrateDB Observer

A lightweight TUI monitoring tool for CrateDB clusters. Single binary, zero dependencies.

## Install

```bash
go install github.com/WalBeh/obsi@latest
```

Or build from source:

```bash
go build -o obsi .
```

## Usage

```bash
# Connect with URL (credentials embedded)
obsi https://admin:password@cluster.example.com:4200

# Local dev (empty password auto-detected)
obsi http://localhost:4200

# With explicit flags
obsi --endpoint http://localhost:4200 --username crate --password secret
```

Password resolution order: `--password` flag > `OBSI_PASSWORD` env var > OS keyring > empty password > interactive prompt.

## Tabs

| Key | Tab | What it shows |
|-----|-----|---------------|
| `1` | Overview | Cluster settings, health checks, node summary, table health |
| `2` | Nodes | Per-node metrics with sparklines, disk IO, watermark bars |
| `3` | Queries | Active queries with duration and statement preview |
| `4` | Tables | Table list with shard distribution, settings, size stats |

## Keys

| Key | Action |
|-----|--------|
| `1-4` | Switch tabs |
| `tab` / `shift+tab` | Next/prev tab |
| `j/k` or `↑/↓` | Navigate |
| `s` | Cycle sort column |
| `/` | Search/filter |
| `esc` | Clear search |
| `ctrl+r` | Force refresh current tab |
| `r` | Reconnect to cluster |
| `q` | Quit |

## Configuration

First run creates `~/.config/obsi/config.toml` with defaults:

```toml
[connection]
endpoint = "http://localhost:4200"
username = "crate"
timeout = "3s"
query_timeout = "10s"
heartbeat_interval = "5s"
node_refresh_interval = "30s"

[collectors.nodes]
enabled = true
interval = "5s"

[collectors.queries]
enabled = true
interval = "2s"

[collectors.shards]
enabled = true
interval = "1m0s"
```

## Features

- Failover-aware connection: works through load balancers, falls back to direct node IPs
- Node disappearance detection with "last seen" tracking
- Disk watermark visualization (low/high/flood markers on disk bars)
- Shard size skew detection
- CrateDB Cloud hostname shortening (`crate-data-hot-<uuid>-0` -> `data-hot-0`)
- Zone-aware topology display
- IO throughput and IOPS derived from cumulative counters

## License

MIT
