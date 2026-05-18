package jmx

import "time"

// JMXSnapshot holds the per-pod metrics extracted from one Prometheus scrape.
// Keys in the inner maps are taken verbatim from croudng's label values so
// callers can render them directly (e.g. pool names like "G1 Eden Space").
type JMXSnapshot struct {
	Pod       string    // full pod name; matches sys.nodes.hostname on CrateDB Cloud
	ScrapedAt time.Time // from croudng header; zero for raw JMX-exporter scrapes

	// JVM
	HeapMax     int64
	Pools       map[string]int64  // memory pool name → used bytes
	BufferPools map[string]int64  // "direct" / "mapped" → used bytes
	GC          map[string]GCStat // collector name → cumulative counters

	// CrateDB
	Breakers    map[string]Breaker        // breaker name → used/limit/tripped
	QueryStats  map[string]QueryTypeStat  // query type → counters
	Connections map[string]ConnStat       // protocol → counters
	ThreadPools map[string]ThreadPoolStat // thread pool → counters

	// cAdvisor — restricted to the "crate" container where labelled
	ContainerCPUSeconds float64
	ContainerMemBytes   int64
	NetRxBytes          int64            // cumulative across all interfaces
	NetTxBytes          int64
	DiskReadBytes       map[string]int64 // device → bytes
	DiskWriteBytes      map[string]int64

	// Operator
	LastUserActivity time.Time
}

// GCStat is a cumulative collector reading; per-interval rates are computed
// downstream by the store.
type GCStat struct {
	Count        int64
	TotalSeconds float64
}

// Breaker captures the three meaningful properties of a CrateDB circuit
// breaker. A Limit of -1 indicates unlimited (e.g. fielddata).
type Breaker struct {
	Used    int64
	Limit   int64
	Tripped int64
}

type QueryTypeStat struct {
	Total         int64
	Failed        int64
	DurationSumMs int64
	AffectedRows  int64
}

type ConnStat struct {
	Open             int64
	Total            int64
	MessagesReceived int64
	MessagesSent     int64
	BytesReceived    int64
	BytesSent        int64
}

type ThreadPoolStat struct {
	Active          int64
	Completed       int64
	PoolSize        int64
	LargestPoolSize int64
	QueueSize       int64
	Rejected        int64
}

// ClusterJMX holds scrape-wide context: the cluster identity used for the
// safety guard, the upstream metadata, and any cluster-level aggregates.
type ClusterJMX struct {
	Name             string // from cloud_clusters_health{cluster_name}
	ID               string // from cloud_clusters_health{cluster_id}
	Meta             ScrapeMeta
	LastUserActivity time.Time // max across all pods
}

// Extracted is the result of converting a Scrape into typed per-pod and
// cluster-level snapshots.
type Extracted struct {
	Cluster ClusterJMX
	Pods    map[string]*JMXSnapshot // key: full pod name
}
