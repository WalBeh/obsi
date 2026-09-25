package config

import "time"

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Connection: ConnectionConfig{
			Endpoint:            "http://localhost:4200",
			Username:            "crate",
			Timeout:             Duration{3 * time.Second},
			QueryTimeout:        Duration{10 * time.Second},
			HeartbeatInterval:   Duration{5 * time.Second},
			NodeRefreshInterval: Duration{30 * time.Second},
			ReadOnly:            true,
		},
		Collectors: map[string]CollectorConfig{
			"cluster": {
				Enabled:  true,
				Interval: Duration{60 * time.Second},
			},
			"health": {
				Enabled:  true,
				Interval: Duration{10 * time.Second},
			},
			"nodes": {
				Enabled:  true,
				Interval: Duration{10 * time.Second},
			},
			"queries": {
				Enabled:  true,
				Interval: Duration{2 * time.Second},
			},
			// sys.allocations carries every node's decision text per shard,
			// ~1.5MB for 1200 unassigned copies, so it's polled less often
			// than the 5s fast path.
			"shards": {
				Enabled:             true,
				Interval:            Duration{1 * time.Minute},
				AllocationsInterval: Duration{30 * time.Second},
			},
			// sys.snapshots lists the repository (S3, Azure) on every read.
			"snapshots": {
				Enabled:  true,
				Interval: Duration{15 * time.Minute},
			},
			// sys.jobs_log is a ring per node; a longer interval misses
			// more changes on busy clusters.
			"changes": {
				Enabled:  true,
				Interval: Duration{10 * time.Second},
			},
			// Only polls while the slowest board is open.
			"jobs_log": {
				Enabled:  true,
				Interval: Duration{15 * time.Second},
			},
		},
		JMX: JMXConfig{
			Endpoint: "", // empty = disabled until user opts in
			Interval: Duration{30 * time.Second},
			Timeout:  Duration{10 * time.Second},
		},
		TUI: TUIConfig{
			RefreshRate:      Duration{500 * time.Millisecond},
			SparklineHistory: 120,
			SetGlobalMode:    "persistent",
		},
		Logging: LoggingConfig{
			Level: "warn",
		},
	}
}
