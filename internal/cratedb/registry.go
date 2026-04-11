package cratedb

import (
	"context"
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
}

type nodeEntry struct {
	Info   NodeInfo
	Health NodeHealth
	Client *Client
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

	heartbeatInterval   time.Duration
	nodeRefreshInterval time.Duration

	cancel context.CancelFunc
}

// NewRegistry creates a new node registry.
func NewRegistry(endpoint, username, password string, pingTimeout, queryTimeout, heartbeatInterval, nodeRefreshInterval time.Duration) *Registry {
	return &Registry{
		primary:             NewClient(endpoint, username, password, queryTimeout),
		nodes:               make(map[string]*nodeEntry),
		username:            username,
		password:            password,
		pingTimeout:         pingTimeout,
		queryTimeout:        queryTimeout,
		heartbeatInterval:   heartbeatInterval,
		nodeRefreshInterval: nodeRefreshInterval,
	}
}

// Bootstrap connects to CrateDB and discovers all nodes.
func (r *Registry) Bootstrap(ctx context.Context) error {
	// Get cluster name
	resp, err := r.primary.Query(ctx, "SELECT name FROM sys.cluster")
	if err != nil {
		return fmt.Errorf("bootstrap cluster name: %w", err)
	}
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
		} else {
			slog.Warn("reconnect failed", "error", err)
		}
	}()
}

// Refresh re-discovers nodes via sys.nodes from any reachable endpoint.
func (r *Registry) Refresh(ctx context.Context) error {
	resp, err := r.queryAny(ctx, `SELECT id, name, hostname, rest_url,
		process['cpu']['percent'] AS cpu_percent,
		os['cpu']['system'] AS cpu_system,
		os['cpu']['user'] AS cpu_user,
		heap['used'] AS heap_used,
		heap['max'] AS heap_max,
		heap['free'] AS heap_free,
		fs['total']['size'] AS fs_total,
		fs['total']['used'] AS fs_used,
		fs['total']['available'] AS fs_avail,
		mem['used'] AS mem_used,
		mem['free'] AS mem_free,
		mem['used'] + mem['free'] AS mem_total,
		load['1'] AS load1,
		load['5'] AS load5,
		load['15'] AS load15,
		version['number'] AS version,
		fs['total']['reads'] AS fs_reads,
		fs['total']['writes'] AS fs_writes,
		fs['total']['bytes_read'] AS fs_bytes_read,
		fs['total']['bytes_written'] AS fs_bytes_written,
		is_master,
		os_info['available_processors'] AS num_cpus,
		os_info['jvm']['version'] AS jvm_version,
		os_info['jvm']['vm_name'] AS jvm_name,
		attributes['zone'] AS zone,
		attributes['node_name'] AS node_role
	FROM sys.nodes`)
	if err != nil {
		return fmt.Errorf("discover nodes: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make(map[string]bool)
	for _, row := range resp.Rows {
		info := parseNodeRow(row)
		seen[info.ID] = true

		if entry, ok := r.nodes[info.ID]; ok {
			entry.Info = info
		} else {
			restURL := info.RestURL
			if restURL == "" {
				restURL = info.Hostname + ":4200"
			}
			client := NewClient("http://"+restURL, r.username, r.password, r.queryTimeout)
			r.nodes[info.ID] = &nodeEntry{
				Info:   info,
				Health: NodeHealth{NodeID: info.ID, Reachable: true, LastSeen: time.Now()},
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
func (r *Registry) Query(ctx context.Context, stmt string, args ...interface{}) (*SQLResponse, error) {
	// Try primary first
	resp, err := r.primary.Query(ctx, stmt, args...)
	if err == nil {
		r.mu.Lock()
		r.lastActive = "loadbalancer"
		r.mu.Unlock()
		return resp, nil
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
			r.mu.Unlock()
			return resp, nil
		}
		lastErr = err
		slog.Debug("direct node query failed", "node", entry.Info.Name, "error", err)
	}

	return nil, fmt.Errorf("all nodes failed, last error: %w", lastErr)
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

		_, err := r.primary.Ping(pingCtx)
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

// queryAny tries the primary, then any node, to execute a query.
func (r *Registry) queryAny(ctx context.Context, stmt string, args ...interface{}) (*SQLResponse, error) {
	resp, err := r.primary.Query(ctx, stmt, args...)
	if err == nil {
		return resp, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, e := range r.nodes {
		resp, err := e.Client.Query(ctx, stmt, args...)
		if err == nil {
			return resp, nil
		}
	}

	return nil, fmt.Errorf("no reachable endpoint for query: %w", err)
}

// parseNodeRow parses a single row from the sys.nodes query.
func parseNodeRow(row []interface{}) NodeInfo {
	info := NodeInfo{}
	if v, ok := row[0].(string); ok {
		info.ID = v
	}
	if v, ok := row[1].(string); ok {
		info.Name = v
	}
	if v, ok := row[2].(string); ok {
		info.Hostname = v
	}
	if v, ok := row[3].(string); ok {
		info.RestURL = v
	}
	info.CPUPercent = toInt16(row[4])
	info.CPUSystem = toInt16(row[5])
	info.CPUUser = toInt16(row[6])
	info.HeapUsed = toInt64(row[7])
	info.HeapMax = toInt64(row[8])
	info.HeapFree = toInt64(row[9])
	info.FSTotal = toInt64(row[10])
	info.FSUsed = toInt64(row[11])
	info.FSAvail = toInt64(row[12])
	info.MemUsed = toInt64(row[13])
	info.MemFree = toInt64(row[14])
	info.MemTotal = toInt64(row[15])
	info.Load[0] = toFloat64(row[16])
	info.Load[1] = toFloat64(row[17])
	info.Load[2] = toFloat64(row[18])
	if v, ok := row[19].(string); ok {
		info.Version = v
	}
	info.FSReads = toInt64(row[20])
	info.FSWrites = toInt64(row[21])
	info.FSBytesRead = toInt64(row[22])
	info.FSBytesWritten = toInt64(row[23])
	if v, ok := row[24].(bool); ok {
		info.IsMaster = v
	}
	info.NumCPUs = int(toFloat64(row[25]))
	if v, ok := row[26].(string); ok {
		info.JVMVersion = v
	}
	if v, ok := row[27].(string); ok {
		info.JVMName = v
	}
	if v, ok := row[28].(string); ok {
		info.Zone = v
	}
	if v, ok := row[29].(string); ok {
		info.NodeRole = v
	}
	return info
}

func toInt16(v interface{}) int16 {
	switch n := v.(type) {
	case float64:
		return int16(n)
	case int64:
		return int16(n)
	}
	return 0
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	}
	return 0
}
