package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func loadString(t *testing.T, body string) *Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The README's example config sets only intervals. That must not turn the
// collectors off.
func TestLoad_CollectorWithoutEnabled(t *testing.T) {
	cfg := loadString(t, `
[collectors.nodes]
interval = "5s"

[collectors.queries]
interval = "2s"
`)
	for _, name := range []string{"nodes", "queries"} {
		if !cfg.Collectors[name].Enabled {
			t.Errorf("%s: interval-only section disabled the collector", name)
		}
	}
	if got := cfg.Collectors["nodes"].Interval.Duration; got != 5*time.Second {
		t.Errorf("nodes interval = %s, want the configured 5s", got)
	}
	if !cfg.Collectors["shards"].Enabled {
		t.Error("collector missing from the file must keep its default")
	}
}

func TestLoad_ExplicitDisableKept(t *testing.T) {
	cfg := loadString(t, `
[collectors.shards]
enabled = false
interval = "1m"
`)
	if cfg.Collectors["shards"].Enabled {
		t.Error("explicit enabled = false must be respected")
	}
}

// Without an interval the collector ran on a 0s timer, back to back.
func TestLoad_CollectorWithoutInterval(t *testing.T) {
	cfg := loadString(t, `
[collectors.queries]
enabled = true
`)
	want := DefaultConfig().Collectors["queries"].Interval.Duration
	if got := cfg.Collectors["queries"].Interval.Duration; got != want {
		t.Errorf("queries interval = %s, want default %s", got, want)
	}
}

func TestLoad_ReadOnly(t *testing.T) {
	if cfg := loadString(t, "[connection]\nendpoint = \"http://localhost:4200\"\n"); !cfg.Connection.ReadOnly {
		t.Error("omitted read_only must default to true")
	}
	if cfg := loadString(t, "[connection]\nread_only = false\n"); cfg.Connection.ReadOnly {
		t.Error("explicit read_only = false was overridden")
	}
}
