package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/jmx"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// JMXCollector polls croudng's localhost Prometheus endpoint, runs the
// cluster-name safety guard, and pushes the result into the store.
//
// It implements the Collector interface so the manager handles lifecycle,
// throttling and context cancellation like any other collector.
type JMXCollector struct {
	interval time.Duration
	scraper  *jmx.Scraper

	mu                  sync.Mutex
	consecutiveFailures int
	mismatchDisabled    bool // permanent once tripped — protects against foreign data
}

// NewJMXCollector wires a JMX collector with sensible HTTP defaults.
func NewJMXCollector(cfg config.JMXConfig) *JMXCollector {
	slog.Info("JMX collector configured", "endpoint", cfg.Endpoint, "interval", cfg.Interval.Duration)
	return &JMXCollector{
		interval: cfg.Interval.Duration,
		scraper:  jmx.NewScraper(cfg.Endpoint, cfg.Timeout.Duration),
	}
}

func (c *JMXCollector) Name() string            { return "jmx" }
func (c *JMXCollector) Interval() time.Duration { return c.interval }

func (c *JMXCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	c.mu.Lock()
	disabled := c.mismatchDisabled
	c.mu.Unlock()
	if disabled {
		return nil
	}

	// We need a cluster name from sys.cluster to run the safety guard.
	// Before bootstrap completes the name is empty; skip silently rather
	// than fetch unverified data.
	expected := reg.Status().ClusterName
	if expected == "" {
		slog.Debug("JMX collect skipped: cluster name not yet known")
		return nil
	}
	slog.Debug("JMX collect", "expected_cluster", expected, "url", c.scraper.URL)

	ex, err := c.scraper.Fetch(ctx, expected)
	if err != nil {
		return c.handleErr(err)
	}

	c.mu.Lock()
	wasFailing := c.consecutiveFailures > 0
	c.consecutiveFailures = 0
	c.mu.Unlock()

	if wasFailing {
		slog.Info("JMX endpoint reachable again", "pods", len(ex.Pods))
	} else {
		slog.Debug("JMX collect ok", "pods", len(ex.Pods), "scraped_at", ex.Cluster.Meta.ScrapedAt)
	}

	st.UpdateJMX(ex)
	return nil
}

// handleErr classifies fetch errors and produces appropriate logging.
// Cluster-name mismatch disables the collector permanently; transient
// failures get a friendly reminder on the first occurrence and a quieter
// nudge every five attempts thereafter.
func (c *JMXCollector) handleErr(err error) error {
	if errors.Is(err, jmx.ErrClusterMismatch) {
		c.mu.Lock()
		c.mismatchDisabled = true
		c.mu.Unlock()
		slog.Error("JMX collector disabled: scrape is for a different cluster than obsi is connected to",
			"error", err, "url", c.scraper.URL)
		return err
	}

	c.mu.Lock()
	c.consecutiveFailures++
	n := c.consecutiveFailures
	c.mu.Unlock()

	if n == 1 {
		slog.Warn("JMX endpoint unreachable; start croudng in another terminal:",
			"hint", "croudng clusters metrics -n <cluster> --profile <profile> --watch",
			"url", c.scraper.URL, "error", err)
	} else if n%5 == 0 {
		slog.Warn("JMX endpoint still unreachable", "attempts", n, "url", c.scraper.URL, "error", err)
	}
	return err
}
