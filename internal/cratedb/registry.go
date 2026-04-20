package cratedb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// RegistryStatus represents the current connection state for display.
type RegistryStatus struct {
	Connected       bool
	PrimaryOK       bool   // whether the primary/LB endpoint is reachable
	ActiveNode      string // name of the node that last answered a query
	ClusterName     string
	TotalNodes      int
	HealthyNodes    int    // direct nodes reachable (may be 0 for cloud clusters)
	DirectReachable bool   // whether any direct node is reachable
	Reconnecting    bool   // whether a reconnect attempt is in progress
	Nodes           []NodeHealth
	Latency         LatencyStats // query latency stats for the active endpoint
}

type nodeEntry struct {
	Info   NodeInfo
	Health NodeHealth
	Client *Client
}

// Query labels for registry-internal queries. Defined here (not in collector)
// to avoid import cycles. Referenced by collector.queryDefs for the tracker.
const (
	QueryLabelHeartbeat     = "registry.heartbeat"
	QueryLabelBootstrap     = "registry.bootstrap"
	QueryLabelNodeDiscovery = "registry.node_discovery"
)

const (
	// latencyBufferSize is the number of recent query latency samples kept for percentile computation.
	latencyBufferSize = 100

	// heartbeatBackoffThreshold is the number of consecutive failures before backing off pings.
	heartbeatBackoffThreshold = 3

	// heartbeatBackoffModulo controls how often a backed-off node is pinged (every Nth cycle).
	heartbeatBackoffModulo = 6
)

// QueryRecorder is an optional interface for recording query execution stats.
// Implemented by collector.QueryTracker; kept as an interface to avoid import cycles.
type QueryRecorder interface {
	Record(label string, dur time.Duration, rows int64)
	RecordError(label string, err error)
}

// Registry maintains the set of known nodes and their health.
// It provides failover-aware query execution.
type Registry struct {
	mu          sync.RWMutex
	primary     *Client
	nodes       map[string]*nodeEntry
	clusterName string
	lastActive  string // name of node that last answered successfully

	primaryOK    bool // whether primary/LB is reachable
	reconnecting bool // whether a reconnect is in progress

	username     string
	password     string
	pingTimeout  time.Duration // short timeout for heartbeat/ping
	queryTimeout time.Duration // longer timeout for data queries
	skipVerify   bool          // skip TLS certificate verification

	heartbeatInterval   time.Duration
	nodeRefreshInterval time.Duration

	latency *LatencyTracker

	cancel     context.CancelFunc
	recorder   QueryRecorder // optional query stats recorder
	onRecovery func()        // called when connection recovers after being down
}

// NewRegistry creates a new node registry.
func NewRegistry(endpoint, username, password string, pingTimeout, queryTimeout, heartbeatInterval, nodeRefreshInterval time.Duration, skipVerify bool) *Registry {
	return &Registry{
		primary:             NewClient(endpoint, username, password, queryTimeout, skipVerify),
		nodes:               make(map[string]*nodeEntry),
		username:            username,
		password:            password,
		pingTimeout:         pingTimeout,
		queryTimeout:        queryTimeout,
		skipVerify:          skipVerify,
		heartbeatInterval:   heartbeatInterval,
		nodeRefreshInterval: nodeRefreshInterval,
		latency:             NewLatencyTracker(latencyBufferSize),
	}
}

// SetRecorder attaches a query stats recorder to the registry.
func (r *Registry) SetRecorder(rec QueryRecorder) {
	r.recorder = rec
}

// OnRecovery registers a callback invoked when the primary endpoint recovers
// after being down. Used to trigger collector refreshes on reconnect.
func (r *Registry) OnRecovery(fn func()) {
	r.onRecovery = fn
}

// Bootstrap connects to CrateDB and discovers all nodes.
func (r *Registry) Bootstrap(ctx context.Context) error {
	// Get cluster name
	start := time.Now()
	resp, err := r.primary.Query(ctx, "SELECT name FROM sys.cluster")
	if err != nil {
		r.recordQuery(QueryLabelBootstrap, time.Since(start), 0, err)
		return fmt.Errorf("bootstrap cluster name: %w", err)
	}
	r.recordQuery(QueryLabelBootstrap, time.Since(start), int64(len(resp.Rows)), nil)
	if len(resp.Rows) > 0 {
		if name, ok := resp.Rows[0][0].(string); ok {
			r.mu.Lock()
			r.clusterName = name
			r.primaryOK = true
			r.mu.Unlock()
		}
	}

	// Discover nodes
	return r.Refresh(ctx)
}

// Reconnect forces a re-bootstrap of the primary endpoint.
// Safe to call from any goroutine.
func (r *Registry) Reconnect(ctx context.Context) {
	r.mu.Lock()
	if r.reconnecting {
		r.mu.Unlock()
		return
	}
	r.reconnecting = true
	r.mu.Unlock()

	go func() {
		defer func() {
			r.mu.Lock()
			r.reconnecting = false
			r.mu.Unlock()
		}()

		slog.Info("reconnecting to primary endpoint")

		pingCtx, cancel := context.WithTimeout(ctx, r.pingTimeout)
		defer cancel()

		_, err := r.primary.Ping(pingCtx)
		r.mu.Lock()
		r.primaryOK = err == nil
		r.mu.Unlock()

		if err == nil {
			slog.Info("primary endpoint reconnected")
			_ = r.Refresh(ctx)
			if r.onRecovery != nil {
				r.onRecovery()
			}
		} else {
			slog.Warn("reconnect failed", "error", err)
		}
	}()
}

// Refresh re-discovers nodes via sys.nodes from any reachable endpoint.
// Only fetches the columns needed for node discovery and failover —
// full node metrics are collected by the nodes collector.
func (r *Registry) Refresh(ctx context.Context) error {
	start := time.Now()
	resp, err := r.queryAny(ctx, `SELECT id, name, hostname, rest_url FROM sys.nodes`)
	dur := time.Since(start)
	if err != nil {
		r.recordQuery(QueryLabelNodeDiscovery, dur, 0, err)
		return fmt.Errorf("discover nodes: %w", err)
	}
	r.recordQuery(QueryLabelNodeDiscovery, dur, int64(len(resp.Rows)), nil)

	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make(map[string]bool)
	for _, row := range resp.Rows {
		id := ToString(row[0])
		name := ToString(row[1])
		hostname := ToString(row[2])
		restURL := ToString(row[3])
		seen[id] = true

		if entry, ok := r.nodes[id]; ok {
			// Update name/hostname in case they changed (unlikely but safe)
			entry.Info.ID = id
			entry.Info.Name = name
			entry.Info.Hostname = hostname
			entry.Info.RestURL = restURL
		} else {
			if restURL == "" {
				restURL = hostname + ":4200"
			}
			client := NewClient("http://"+restURL, r.username, r.password, r.queryTimeout, r.skipVerify)
			r.nodes[id] = &nodeEntry{
				Info:   NodeInfo{ID: id, Name: name, Hostname: hostname, RestURL: restURL},
				Health: NodeHealth{NodeID: id, Reachable: true, LastSeen: time.Now()},
				Client: client,
			}
		}
	}

	// Remove nodes no longer in sys.nodes
	for id := range r.nodes {
		if !seen[id] {
			delete(r.nodes, id)
		}
	}

	return nil
}

// Start begins the heartbeat and node refresh loops.
func (r *Registry) Start(ctx context.Context) {
	ctx, r.cancel = context.WithCancel(ctx)
	go r.heartbeatLoop(ctx)
	go r.refreshLoop(ctx)
}

// Stop stops the background loops.
func (r *Registry) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
}

// Query tries the primary endpoint first; on timeout, fans out to healthy direct nodes.
// CrateDB application errors (4xx/5xx) are returned immediately without failover,
// since they indicate a query-level problem that won't resolve on a different node.
func (r *Registry) Query(ctx context.Context, stmt string, args ...interface{}) (*SQLResponse, error) {
	// Try primary first
	resp, err := r.primary.Query(ctx, stmt, args...)
	if err == nil {
		r.mu.Lock()
		r.lastActive = "loadbalancer"
		r.recordLatency(r.primary.lastLatency)
		r.mu.Unlock()
		return resp, nil
	}

	// Don't failover for CrateDB application errors — the server responded,
	// another node will return the same error.
	var crateErr *CrateDBError
	if errors.As(err, &crateErr) {
		return nil, err
	}

	slog.Debug("primary endpoint failed, trying direct nodes", "error", err)

	// Failover to healthy direct nodes
	r.mu.RLock()
	healthy := make([]*nodeEntry, 0)
	for _, e := range r.nodes {
		if e.Health.Reachable {
			healthy = append(healthy, e)
		}
	}
	r.mu.RUnlock()

	if len(healthy) == 0 {
		return nil, fmt.Errorf("all nodes unreachable, primary error: %w", err)
	}

	// Shuffle to distribute load
	rand.Shuffle(len(healthy), func(i, j int) {
		healthy[i], healthy[j] = healthy[j], healthy[i]
	})

	var lastErr error
	for _, entry := range healthy {
		resp, err := entry.Client.Query(ctx, stmt, args...)
		if err == nil {
			r.mu.Lock()
			r.lastActive = entry.Info.Name
			r.recordLatency(entry.Client.lastLatency)
			r.mu.Unlock()
			return resp, nil
		}
		lastErr = err
		slog.Debug("direct node query failed", "node", entry.Info.Name, "error", err)
	}

	return nil, fmt.Errorf("all nodes failed, last error: %w", lastErr)
}

// recordLatency adds a sample to the circular buffer and adjusts the
// primary client's timeout if latency is high. Caller must hold r.mu.
func (r *Registry) recordLatency(d time.Duration) {
	r.latency.Record(d)

	// Adaptive timeout: base timeout + observed max latency, capped at 2x base.
	// On a stressed cluster with 1s+ latency, a 10s base timeout leaves
	// only 9s for actual query execution, which is often not enough.
	// The cap prevents unbounded growth that would make the app unresponsive
	// when the cluster is struggling.
	maxLatency := r.latency.Max()
	if maxLatency > 0 {
		adjusted := r.primary.baseTimeout + maxLatency
		cap := r.primary.baseTimeout * 2
		if adjusted > cap {
			adjusted = cap
		}
		if adjusted != r.primary.httpClient.Timeout {
			r.primary.httpClient.Timeout = adjusted
			slog.Debug("adaptive timeout adjusted", "base", r.primary.baseTimeout, "max_latency", maxLatency, "effective", adjusted, "cap", cap)
		}
	}
}

// Status returns the current connection summary.
func (r *Registry) Status() RegistryStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	status := RegistryStatus{
		ClusterName:  r.clusterName,
		TotalNodes:   len(r.nodes),
		ActiveNode:   r.lastActive,
		PrimaryOK:    r.primaryOK,
		Reconnecting: r.reconnecting,
	}

	for _, e := range r.nodes {
		if e.Health.Reachable {
			status.HealthyNodes++
		}
		status.Nodes = append(status.Nodes, e.Health)
	}

	status.DirectReachable = status.HealthyNodes > 0
	status.Connected = r.primaryOK || status.DirectReachable
	status.Latency = r.latency.Stats()
	return status
}

func (r *Registry) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runHeartbeat(ctx)
		}
	}
}

func (r *Registry) runHeartbeat(ctx context.Context) {
	var wg sync.WaitGroup

	// Heartbeat the primary/LB endpoint
	wg.Add(1)
	go func() {
		defer wg.Done()
		pingCtx, cancel := context.WithTimeout(ctx, r.pingTimeout)
		defer cancel()

		latency, err := r.primary.Ping(pingCtx)
		if err != nil {
			r.recordQuery(QueryLabelHeartbeat, latency, 0, err)
		} else {
			r.recordQuery(QueryLabelHeartbeat, latency, 1, nil)
		}
		r.mu.Lock()
		wasPrimaryOK := r.primaryOK
		r.primaryOK = err == nil
		r.mu.Unlock()

		if err != nil {
			slog.Debug("primary heartbeat failed", "error", err)
		} else if !wasPrimaryOK {
			slog.Info("primary endpoint recovered")
			// Re-discover nodes on recovery
			_ = r.Refresh(ctx)
			if r.onRecovery != nil {
				r.onRecovery()
			}
		}
	}()

	// Heartbeat direct nodes
	r.mu.RLock()
	entries := make([]*nodeEntry, 0, len(r.nodes))
	for _, e := range r.nodes {
		entries = append(entries, e)
	}
	r.mu.RUnlock()

	for _, entry := range entries {
		// Back off on consistently unreachable nodes:
		// after 3 fails, only ping every 6th heartbeat cycle (~30s at 5s interval)
		if entry.Health.ConsecutiveFails >= heartbeatBackoffThreshold {
			entry.Health.BackoffCounter++
			if entry.Health.BackoffCounter%heartbeatBackoffModulo != 0 {
				continue
			}
		}

		wg.Add(1)
		go func(e *nodeEntry) {
			defer wg.Done()

			pingCtx, cancel := context.WithTimeout(ctx, r.pingTimeout)
			defer cancel()

			latency, err := e.Client.Ping(pingCtx)

			r.mu.Lock()
			defer r.mu.Unlock()

			if err != nil {
				e.Health.ConsecutiveFails++
				e.Health.Reachable = false
				slog.Debug("heartbeat failed", "node", e.Info.Name, "fails", e.Health.ConsecutiveFails, "error", err)
			} else {
				e.Health.Reachable = true
				e.Health.LastSeen = time.Now()
				e.Health.LastLatency = latency
				e.Health.ConsecutiveFails = 0
				e.Health.BackoffCounter = 0
			}
		}(entry)
	}
	wg.Wait()
}

func (r *Registry) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(r.nodeRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Refresh(ctx); err != nil {
				slog.Warn("node refresh failed", "error", err)
			}
		}
	}
}

// recordQuery delegates to the optional QueryRecorder if set.
func (r *Registry) recordQuery(label string, dur time.Duration, rows int64, err error) {
	if r.recorder == nil {
		return
	}
	if err != nil {
		r.recorder.RecordError(label, err)
	} else {
		r.recorder.Record(label, dur, rows)
	}
}

// queryAny tries the primary, then any node, to execute a query.
func (r *Registry) queryAny(ctx context.Context, stmt string, args ...interface{}) (*SQLResponse, error) {
	resp, err := r.primary.Query(ctx, stmt, args...)
	if err == nil {
		return resp, nil
	}

	// Snapshot node clients under lock, then release before making HTTP calls.
	// Holding RLock during slow HTTP calls causes priority inversion: a waiting
	// writer (heartbeat) blocks new readers (TUI's Status()), freezing the UI.
	r.mu.RLock()
	clients := make([]*Client, 0, len(r.nodes))
	for _, e := range r.nodes {
		clients = append(clients, e.Client)
	}
	r.mu.RUnlock()

	for _, c := range clients {
		resp, err := c.Query(ctx, stmt, args...)
		if err == nil {
			return resp, nil
		}
	}

	return nil, fmt.Errorf("no reachable endpoint for query: %w", err)
}


