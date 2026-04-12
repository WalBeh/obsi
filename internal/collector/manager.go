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

// ThrottleLevel controls how aggressively obsi polls the cluster.
type ThrottleLevel int

const (
	ThrottleNone   ThrottleLevel = iota // normal intervals
	ThrottleMild                        // 2x intervals
	ThrottleHeavy                       // 5x intervals
)

var throttleNames = [3]string{"normal", "mild (2x)", "heavy (5x)"}
var throttleMultipliers = [3]int{1, 2, 5}

// Manager starts and stops collector goroutines.
type Manager struct {
	collectors      []Collector
	registry        *cratedb.Registry
	store           *store.Store
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	throttle        ThrottleLevel
	throttleMu      sync.RWMutex
	fastPathMu      sync.RWMutex
	fastPathEnabled map[string]bool
}

// NewManager creates a new collector manager.
func NewManager(reg *cratedb.Registry, st *store.Store, collectors ...Collector) *Manager {
	return &Manager{
		collectors:      collectors,
		registry:        reg,
		store:           st,
		fastPathEnabled: make(map[string]bool),
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

// SetThrottle changes the throttle level for all collectors.
func (m *Manager) SetThrottle(level ThrottleLevel) {
	m.throttleMu.Lock()
	defer m.throttleMu.Unlock()
	m.throttle = level
	slog.Info("throttle changed", "level", throttleNames[level])
}

// CycleThrottle cycles through throttle levels.
func (m *Manager) CycleThrottle() ThrottleLevel {
	m.throttleMu.Lock()
	defer m.throttleMu.Unlock()
	m.throttle = (m.throttle + 1) % 3
	slog.Info("throttle changed", "level", throttleNames[m.throttle])
	return m.throttle
}

// Throttle returns the current throttle level.
func (m *Manager) Throttle() ThrottleLevel {
	m.throttleMu.RLock()
	defer m.throttleMu.RUnlock()
	return m.throttle
}

// ThrottleName returns the display name for a throttle level.
func ThrottleName(level ThrottleLevel) string {
	return throttleNames[level]
}

// ThrottleMultiplier returns the interval multiplier for a throttle level.
func ThrottleMultiplier(level ThrottleLevel) int {
	return throttleMultipliers[level]
}

// SuggestThrottle checks node heap pressure and suggests throttling.
// Returns true if any node has heap > 85%.
func (m *Manager) SuggestThrottle() bool {
	return m.store.AnyNodeHeapAbove(85)
}

// SetFastPath enables or disables the fast-path ticker for a named collector.
// Only collectors implementing FastPathCollector respond to this.
func (m *Manager) SetFastPath(collectorName string, enabled bool) {
	m.fastPathMu.Lock()
	defer m.fastPathMu.Unlock()
	if m.fastPathEnabled[collectorName] != enabled {
		m.fastPathEnabled[collectorName] = enabled
		slog.Debug("fast-path toggled", "collector", collectorName, "enabled", enabled)
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
	}

	baseInterval := c.Interval()
	ticker := time.NewTicker(baseInterval)
	defer ticker.Stop()

	// Fast-path support: second ticker for high-frequency lightweight collection
	fpc, hasFastPath := c.(FastPathCollector)
	var fastCh <-chan time.Time
	var fastTicker *time.Ticker
	if hasFastPath {
		fastTicker = time.NewTicker(5 * time.Second)
		fastTicker.Stop() // start stopped
		defer fastTicker.Stop()
	}
	fastPathActive := false

	lastMultiplier := 1

	for {
		// Check fast-path toggle
		if hasFastPath {
			m.fastPathMu.RLock()
			shouldBeActive := m.fastPathEnabled[c.Name()]
			m.fastPathMu.RUnlock()
			if shouldBeActive && !fastPathActive {
				fastTicker.Reset(5 * time.Second)
				fastCh = fastTicker.C
				fastPathActive = true
			} else if !shouldBeActive && fastPathActive {
				fastTicker.Stop()
				fastCh = nil
				fastPathActive = false
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Adjust ticker if throttle level changed
			m.throttleMu.RLock()
			mult := throttleMultipliers[m.throttle]
			m.throttleMu.RUnlock()
			if mult != lastMultiplier {
				lastMultiplier = mult
				ticker.Reset(baseInterval * time.Duration(mult))
			}

			if err := c.Collect(ctx, m.registry, m.store); err != nil {
				slog.Warn("collector failed", "collector", c.Name(), "error", err)
				// Don't mark stale — keep showing last successful data.
			// Time-based staleness in Snapshot() handles aging out.
			}
		case <-fastCh:
			if err := fpc.CollectFastPath(ctx, m.registry, m.store); err != nil {
				slog.Warn("fast-path collect failed", "collector", c.Name(), "error", err)
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
