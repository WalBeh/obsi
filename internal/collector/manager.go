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
	ThrottleMax                         // heartbeat only — all collectors paused
	throttleLevels                      // sentinel: total number of levels
)

const (
	// FastPathInterval is the tick rate for high-frequency lightweight collection.
	FastPathInterval = 5 * time.Second

	// ThrottleMaxPollInterval is how often paused collectors re-check throttle state.
	ThrottleMaxPollInterval = 2 * time.Second

	// heapPressureThreshold is the heap usage percentage above which throttling is suggested.
	heapPressureThreshold = 85.0

	// SummitRefreshInterval is how often the random summit is fetched (ORDER BY random() is expensive).
	SummitRefreshInterval = 5 * time.Minute
)

var throttleNames = []string{"normal", "mild (2x)", "heavy (5x)", "max (paused)"}
var throttleMultipliers = []int{1, 2, 5, 0} // 0 = paused

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
	tracker         *QueryTracker
}

// NewManager creates a new collector manager.
func NewManager(reg *cratedb.Registry, st *store.Store, tracker *QueryTracker, collectors ...Collector) *Manager {
	return &Manager{
		collectors:      collectors,
		registry:        reg,
		store:           st,
		tracker:         tracker,
		fastPathEnabled: make(map[string]bool),
	}
}

// QueryTracker returns the query execution stats tracker.
func (m *Manager) QueryTracker() *QueryTracker {
	return m.tracker
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
	m.throttle = (m.throttle + 1) % throttleLevels
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
// Returns true if any node has heap above the threshold.
func (m *Manager) SuggestThrottle() bool {
	return m.store.AnyNodeHeapAbove(heapPressureThreshold)
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
// Blocked in ThrottleMax mode — all collectors are paused.
func (m *Manager) TriggerCollector(ctx context.Context, name string) {
	if m.Throttle() == ThrottleMax {
		return
	}
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

// TriggerAll runs every collector once in the background with a short stagger
// between each to avoid slamming a recovering cluster with concurrent queries.
func (m *Manager) TriggerAll(ctx context.Context) {
	if m.Throttle() == ThrottleMax {
		return
	}
	go func() {
		for i, c := range m.collectors {
			if i > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
			slog.Info("recovery refresh triggered", "collector", c.Name())
			if err := c.Collect(ctx, m.registry, m.store); err != nil {
				slog.Warn("recovery refresh failed", "collector", c.Name(), "error", err)
			}
		}
	}()
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

	// Fast-path support: second ticker for high-frequency lightweight collection
	fpc, hasFastPath := c.(FastPathCollector)
	var fastCh <-chan time.Time
	var fastTicker *time.Ticker
	if hasFastPath {
		fastTicker = time.NewTicker(FastPathInterval)
		fastTicker.Stop() // start stopped
		defer fastTicker.Stop()
	}
	fastPathActive := false

	for {
		// Compute effective interval from current throttle level
		m.throttleMu.RLock()
		level := m.throttle
		mult := throttleMultipliers[level]
		m.throttleMu.RUnlock()

		// ThrottleMax: all collectors paused — sleep and re-check.
		if level == ThrottleMax {
			if hasFastPath && fastPathActive {
				fastTicker.Stop()
				fastCh = nil
				fastPathActive = false
			}
			timer := time.NewTimer(ThrottleMaxPollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				continue
			}
		}

		effectiveInterval := baseInterval * time.Duration(mult)

		// Check fast-path toggle — force off when throttle >= Heavy
		if hasFastPath {
			m.fastPathMu.RLock()
			shouldBeActive := m.fastPathEnabled[c.Name()] && level < ThrottleHeavy
			m.fastPathMu.RUnlock()
			if shouldBeActive && !fastPathActive {
				fastTicker.Reset(FastPathInterval)
				fastCh = fastTicker.C
				fastPathActive = true
			} else if !shouldBeActive && fastPathActive {
				fastTicker.Stop()
				fastCh = nil
				fastPathActive = false
			}
		}

		// Use time.NewTimer instead of a ticker to guarantee the full interval
		// elapses between collections, including after slow queries.
		timer := time.NewTimer(effectiveInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := c.Collect(ctx, m.registry, m.store); err != nil {
				slog.Warn("collector failed", "collector", c.Name(), "error", err)
			}
		case <-fastCh:
			timer.Stop()
			if err := fpc.CollectFastPath(ctx, m.registry, m.store); err != nil {
				slog.Warn("fast-path collect failed", "collector", c.Name(), "error", err)
			}
		}
	}
}

// DefaultCollectors returns all enabled collectors in a deterministic order.
// The JMX collector is appended only when jmxCfg.Endpoint is non-empty —
// empty endpoint is the off-switch, there is no separate Enabled flag.
func DefaultCollectors(cfg map[string]config.CollectorConfig, jmxCfg config.JMXConfig, tracker *QueryTracker) []Collector {
	type entry struct {
		name string
		make func() Collector
	}
	ordered := []entry{
		{"cluster", func() Collector { return NewClusterCollector(cfg["cluster"], tracker) }},
		{"health", func() Collector { return NewHealthCollector(cfg["health"], tracker) }},
		{"nodes", func() Collector { return NewNodesCollector(cfg["nodes"], tracker) }},
		{"queries", func() Collector { return NewQueriesCollector(cfg["queries"], tracker) }},
		{"shards", func() Collector { return NewShardsCollector(cfg["shards"], tracker) }},
	}

	var enabled []Collector
	for _, e := range ordered {
		if cc, ok := cfg[e.name]; ok && cc.Enabled {
			enabled = append(enabled, e.make())
		}
	}
	if jmxCfg.Endpoint != "" {
		enabled = append(enabled, NewJMXCollector(jmxCfg))
	}
	return enabled
}
