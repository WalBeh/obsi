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
		},
		Collectors: map[string]CollectorConfig{
			"cluster": {
				Enabled:  true,
				Interval: Duration{10 * time.Second},
			},
			"health": {
				Enabled:  true,
				Interval: Duration{10 * time.Second},
			},
			"nodes": {
				Enabled:  true,
				Interval: Duration{5 * time.Second},
			},
			"queries": {
				Enabled:  true,
				Interval: Duration{2 * time.Second},
			},
			"shards": {
				Enabled:  true,
				Interval: Duration{1 * time.Minute},
			},
		},
		TUI: TUIConfig{
			RefreshRate:      Duration{500 * time.Millisecond},
			SparklineHistory: 120,
		},
		Logging: LoggingConfig{
			Level: "warn",
		},
	}
}
