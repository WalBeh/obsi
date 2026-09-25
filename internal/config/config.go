package config

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

// Config is the top-level configuration.
type Config struct {
	LastProfile string                     `toml:"last_profile,omitempty"`
	Connection  ConnectionConfig           `toml:"connection"`
	Profiles    map[string]ProfileConfig   `toml:"profiles,omitempty"`
	Collectors  map[string]CollectorConfig `toml:"collectors"`
	JMX         JMXConfig                  `toml:"jmx"`
	TUI         TUIConfig                  `toml:"tui"`
	Logging     LoggingConfig              `toml:"logging"`
}

// ProfileConfig holds per-cluster connection details.
type ProfileConfig struct {
	Endpoint string `toml:"endpoint"`
	Username string `toml:"username"`
}

// ConnectionConfig holds CrateDB connection settings.
type ConnectionConfig struct {
	Endpoint            string   `toml:"endpoint"`
	Username            string   `toml:"username"`
	PasswordEncrypted   string   `toml:"password_encrypted,omitempty"`
	Timeout             Duration `toml:"timeout"`       // heartbeat/ping timeout
	QueryTimeout        Duration `toml:"query_timeout"` // data query timeout (collectors)
	HeartbeatInterval   Duration `toml:"heartbeat_interval"`
	NodeRefreshInterval Duration `toml:"node_refresh_interval"`
	// ReadOnly blocks the SQL editor's writes and SET GLOBAL. K still kills:
	// it's one confirmed job, and restarting obsi mid-incident to get it back
	// is worse. On when omitted.
	ReadOnly bool `toml:"read_only"`
}

// CollectorConfig holds settings for a single collector.
type CollectorConfig struct {
	Enabled  bool     `toml:"enabled"`
	Interval Duration `toml:"interval"`
	// AllocationsInterval (shards only) spaces out the sys.allocations
	// polls the Shards tab makes while shards aren't STARTED.
	AllocationsInterval Duration `toml:"allocations_interval,omitempty"`
}

// JMXConfig configures ingestion of JVM/cAdvisor/operator metrics served by
// croudng's localhost Prometheus endpoint. An empty Endpoint disables the
// integration entirely — there is no separate Enabled flag.
type JMXConfig struct {
	Endpoint string   `toml:"endpoint"`
	Interval Duration `toml:"interval"`
	Timeout  Duration `toml:"timeout"`
}

// TUIConfig holds TUI display settings.
type TUIConfig struct {
	RefreshRate      Duration `toml:"refresh_rate"`
	SparklineHistory int      `toml:"sparkline_history"`
	SetGlobalMode    string   `toml:"set_global_mode"` // "persistent" (default) or "transient"
	AlertBell        bool     `toml:"alert_bell"`      // ring the terminal bell when an alert is raised
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	Level string `toml:"level"`
	File  string `toml:"file"`
}

// Duration wraps time.Duration for TOML unmarshaling.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// Load reads a TOML config file. If it doesn't exist, creates one with defaults.
func Load(path string) (*Config, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		cfg := DefaultConfig()
		if err := Save(path, cfg); err != nil {
			return nil, fmt.Errorf("create default config: %w", err)
		}
		return cfg, nil
	}

	var cfg Config
	md, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}

	applyDefaults(&cfg, md)
	return &cfg, nil
}

// Save writes the config to a TOML file.
func Save(path string, cfg *Config) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create config file %s: %w", path, err)
	}
	defer f.Close()

	enc := toml.NewEncoder(f)
	return enc.Encode(cfg)
}

// applyDefaults fills in zero-value fields with sensible defaults. md tells
// an omitted key from one set to its zero value.
func applyDefaults(cfg *Config, md toml.MetaData) {
	defaults := DefaultConfig()

	if cfg.Connection.Endpoint == "" {
		cfg.Connection.Endpoint = defaults.Connection.Endpoint
	}
	if cfg.Connection.Username == "" {
		cfg.Connection.Username = defaults.Connection.Username
	}
	if cfg.Connection.Timeout.Duration == 0 {
		cfg.Connection.Timeout = defaults.Connection.Timeout
	}
	if cfg.Connection.QueryTimeout.Duration == 0 {
		cfg.Connection.QueryTimeout = defaults.Connection.QueryTimeout
	}
	if cfg.Connection.HeartbeatInterval.Duration == 0 {
		cfg.Connection.HeartbeatInterval = defaults.Connection.HeartbeatInterval
	}
	if cfg.Connection.NodeRefreshInterval.Duration == 0 {
		cfg.Connection.NodeRefreshInterval = defaults.Connection.NodeRefreshInterval
	}
	if !md.IsDefined("connection", "read_only") {
		cfg.Connection.ReadOnly = defaults.Connection.ReadOnly
	}
	if cfg.TUI.RefreshRate.Duration == 0 {
		cfg.TUI.RefreshRate = defaults.TUI.RefreshRate
	}
	if cfg.TUI.SparklineHistory == 0 {
		cfg.TUI.SparklineHistory = defaults.TUI.SparklineHistory
	}
	if cfg.TUI.SetGlobalMode == "" {
		cfg.TUI.SetGlobalMode = defaults.TUI.SetGlobalMode
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = defaults.Logging.Level
	}

	if cfg.Collectors == nil {
		cfg.Collectors = defaults.Collectors
	} else {
		// A section like `[collectors.queries] interval = "2s"` used to decode
		// as enabled = false, silently turning the collector off, and one
		// without interval ran it with a 0s timer, back to back. Fill the
		// omitted fields per collector instead.
		for name, dc := range defaults.Collectors {
			cc, ok := cfg.Collectors[name]
			if !ok {
				cfg.Collectors[name] = dc
				continue
			}
			if !md.IsDefined("collectors", name, "enabled") {
				cc.Enabled = dc.Enabled
			}
			if cc.Interval.Duration <= 0 {
				cc.Interval = dc.Interval
			}
			if cc.AllocationsInterval.Duration <= 0 {
				cc.AllocationsInterval = dc.AllocationsInterval
			}
			cfg.Collectors[name] = cc
		}
	}

	if cfg.JMX.Interval.Duration == 0 {
		cfg.JMX.Interval = defaults.JMX.Interval
	}
	if cfg.JMX.Timeout.Duration == 0 {
		cfg.JMX.Timeout = defaults.JMX.Timeout
	}
}
