package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// Manager starts and stops collector goroutines.
type Manager struct {
	collectors []Collector
	registry   *cratedb.Registry
	store      *store.Store
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewManager creates a new collector manager.
func NewManager(reg *cratedb.Registry, st *store.Store, collectors ...Collector) *Manager {
	return &Manager{
		collectors: collectors,
		registry:   reg,
		store:      st,
	}
}

// Start launches one goroutine per collector.
func (m *Manager) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	for _, c := range m.collectors {
		m.wg.Add(1)
		go m.runCollector(ctx, c)
	}
}

// TriggerCollector runs a named collector once immediately in the background.
func (m *Manager) TriggerCollector(ctx context.Context, name string) {
	for _, c := range m.collectors {
		if c.Name() == name {
			go func(c Collector) {
				slog.Info("manual refresh triggered", "collector", c.Name())
				if err := c.Collect(ctx, m.registry, m.store); err != nil {
					slog.Warn("manual refresh failed", "collector", c.Name(), "error", err)
				}
			}(c)
			return
		}
	}
}

// Stop cancels all collector goroutines and waits for them to finish.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *Manager) runCollector(ctx context.Context, c Collector) {
	defer m.wg.Done()

	// Collect once immediately
	if err := c.Collect(ctx, m.registry, m.store); err != nil {
		slog.Warn("collector initial run failed", "collector", c.Name(), "error", err)
		m.store.MarkStale(c.Name())
	}

	ticker := time.NewTicker(c.Interval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Collect(ctx, m.registry, m.store); err != nil {
				slog.Warn("collector failed", "collector", c.Name(), "error", err)
				m.store.MarkStale(c.Name())
			}
		}
	}
}

// DefaultCollectors returns all enabled collectors based on configuration.
func DefaultCollectors(cfg map[string]config.CollectorConfig) []Collector {
	all := map[string]Collector{
		"cluster": NewClusterCollector(cfg["cluster"]),
		"health":  NewHealthCollector(cfg["health"]),
		"nodes":   NewNodesCollector(cfg["nodes"]),
		"queries": NewQueriesCollector(cfg["queries"]),
		"shards":  NewShardsCollector(cfg["shards"]),
	}

	var enabled []Collector
	for name, c := range all {
		if cc, ok := cfg[name]; ok && cc.Enabled {
			enabled = append(enabled, c)
		}
	}
	return enabled
}
