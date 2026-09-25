package cratedb

import (
	"fmt"
	"time"
)

// CrateDBError represents an error response from the CrateDB SQL endpoint.
// This is distinct from connection/timeout errors — the server processed
// the request and returned an error, so failover to another node won't help.
type CrateDBError struct {
	StatusCode int
	Endpoint   string
	Body       string
}

func (e *CrateDBError) Error() string {
	return fmt.Sprintf("cratedb error (status %d) from %s: %s", e.StatusCode, e.Endpoint, e.Body)
}

// SQLRequest is the JSON body sent to POST /_sql.
type SQLRequest struct {
	Stmt string        `json:"stmt"`
	Args []interface{} `json:"args,omitempty"`
}

// SQLResponse is the JSON response from CrateDB's /_sql endpoint.
type SQLResponse struct {
	Cols     []string        `json:"cols"`
	ColTypes []int           `json:"col_types,omitempty"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int64           `json:"rowcount"`
	Duration float64         `json:"duration"`
}

// NodeInfo represents a discovered CrateDB node from sys.nodes.
type NodeInfo struct {
	ID             string
	Name           string
	Hostname       string
	RestURL        string
	Version        string
	Load           [3]float64
	HeapUsed       int64
	HeapMax        int64
	HeapFree       int64
	FSTotal        int64
	FSUsed         int64
	FSAvail        int64
	CPUPercent     int16 // from process['cpu']['percent'] (reliable across OS)
	CPUSystem      int16 // from os['cpu']['system'] (may be -1 on some OS)
	CPUUser        int16 // from os['cpu']['user'] (may be -1 on some OS)
	MemUsed        int64
	MemFree        int64
	MemTotal       int64
	FSReads        int64  // cumulative read ops from fs['total']['reads']
	FSWrites       int64  // cumulative write ops from fs['total']['writes']
	FSBytesRead    int64  // cumulative bytes read from fs['total']['bytes_read']
	FSBytesWritten int64  // cumulative bytes written from fs['total']['bytes_written']
	Zone           string // from attributes['zone']
	NodeRole       string // from attributes['node_name'] (hot, warm, cold, master)
	IsMaster       bool   // from is_master
	NumCPUs        int    // from os_info['available_processors']
	JVMVersion     string // from os_info['jvm']['version']
	JVMName        string // from os_info['jvm']['vm_name']

	ThreadPools []ThreadPoolStats // from sys.nodes thread_pools
}

// ThreadPoolStats represents one thread pool entry from sys.nodes.
type ThreadPoolStats struct {
	Name      string
	Active    int64
	Queue     int64
	Rejected  int64
	Completed int64
	Threads   int64
	Largest   int64
}

// NodeHealth tracks heartbeat state for one node.
type NodeHealth struct {
	NodeID           string
	Reachable        bool
	LastSeen         time.Time
	LastLatency      time.Duration
	ConsecutiveFails int
	BackoffCounter   int // used to space out pings on unreachable nodes
}

// ClusterSettings holds key cluster-level settings from sys.cluster.
type ClusterSettings struct {
	MaxShardsPerNode           int
	AllocationEnable           string // "all", "primaries", "new_primaries", "none"
	NodeConcurrentRecoveries   int
	ClusterConcurrentRebalance int
	RecoveryMaxBytesPerSec     string // e.g. "40mb"
	DiskWatermarkLow           string // e.g. "85%"
	DiskWatermarkHigh          string // e.g. "90%"
	DiskWatermarkFlood         string // e.g. "95%"
	RebalanceEnable            string // "all", "primaries", "replicas", "none"
}

// Summit is a random mountain from sys.summits — CrateDB's Easter egg.
type Summit struct {
	Mountain    string
	Height      int
	Region      string
	Country     string
	FirstAscent int
}

// SnapshotInfo represents a row from sys.snapshots.
type SnapshotInfo struct {
	Repository string
	Name       string
	State      string // SUCCESS, PARTIAL, FAILED, IN_PROGRESS, INCOMPATIBLE
	Started    time.Time
	Finished   time.Time
	Failures   int
}

// ClusterCheck represents a row from sys.checks.
type ClusterCheck struct {
	ID          int
	Severity    int
	Description string
	Passed      bool
}

// TableHealth represents a row from sys.health.
type TableHealth struct {
	TableSchema     string
	TableName       string
	Health          string
	MissingShards   int64
	UnderReplicated int64
	Partition       string
}

// ActiveQuery represents a row from sys.jobs, enriched with the running
// operations from sys.operations joined on job_id.
type ActiveQuery struct {
	ID         string
	Node       string
	Started    time.Time
	Stmt       string
	Username   string
	UsedBytes  int64       // sum of used_bytes across all operations of this job
	Operations []Operation // sorted by UsedBytes desc; empty when the job has no ops yet
}

// Operation represents a row from sys.operations.
type Operation struct {
	Name      string
	NodeName  string
	UsedBytes int64
	Started   time.Time
}

// JobLogEntry is a finished job from sys.jobs_log.
type JobLogEntry struct {
	ID       string
	Node     string
	Username string
	Stmt     string
	Started  time.Time
	Ended    time.Time
	Error    string // empty when the job succeeded
	Type     string // classification['type'], e.g. SELECT, DDL
}

// JobLogGroup aggregates sys.jobs_log rows sharing the exact stmt text.
// Parameterized statements are logged with placeholders, so they group well;
// literal-heavy SQL mostly doesn't.
type JobLogGroup struct {
	Stmt      string
	Count     int64
	Failed    int64
	Max       time.Duration
	Avg       time.Duration
	LastEnded time.Time
}

// JobLogCoverage is how far back one node's sys.jobs_log reaches. The log is
// an in-memory ring per node (stats.jobs_log_size), so on busy clusters this
// can be minutes.
type JobLogCoverage struct {
	Node    string
	Oldest  time.Time
	Entries int64
}

// ShardInfo represents a row from sys.shards.
type ShardInfo struct {
	ID                      int
	SchemaName              string
	TableName               string
	PartitionIdent          string
	NumDocs                 int64
	Primary                 bool
	State                   string
	RoutingState            string
	Relocating              bool
	RelocatingNode          string // target node name when relocating
	Size                    int64
	NodeID                  string
	NodeName                string
	RecoveryStage           string
	RecoveryPercent         float64
	RecoveryType            string // STORE, EXISTING_STORE, EMPTY_STORE, PEER, SNAPSHOT, LOCAL_SHARDS
	TranslogSize            int64
	TranslogUncommittedSize int64
	TranslogUncommittedOps  int64
}

// AllocationInfo represents a row from sys.allocations for non-STARTED shards.
type AllocationInfo struct {
	TableSchema    string
	TableName      string
	PartitionIdent string
	ShardID        int
	Primary        bool
	CurrentState   string
	NodeID         string
	NodeName       string
	Explanation    string
	Decisions      []AllocationDecision // per node, why the shard can't go there
}

// AllocationDecision is one node's entry in sys.allocations.decisions.
type AllocationDecision struct {
	NodeName     string
	Explanations []string
}

// DefaultTranslogFlushThreshold is CrateDB's default translog flush_threshold_size (512 MiB).
const DefaultTranslogFlushThreshold int64 = 512 << 20

// TableSettings holds table-level configuration from information_schema.tables.
type TableSettings struct {
	NumberOfShards         int
	NumberOfReplicas       string // can be "0-1", "1", etc.
	ClusteredBy            string
	PartitionedBy          []string
	ColumnPolicy           string
	RefreshInterval        int // ms
	Codec                  string
	TranslogFlushThreshold int64 // bytes
	TranslogSyncInterval   int   // ms
	TranslogDurability     string
}

// TableInfo is an aggregated view of a table with shard distribution.
type TableInfo struct {
	SchemaName                  string
	TableName                   string
	TotalShards                 int
	PrimaryShards               int
	ReplicaShards               int
	TotalRecords                int64
	TotalSize                   int64 // primary shards only
	TotalDiskSize               int64 // all shards including replicas
	Health                      string
	ShardsPerNode               map[string]int // nodeName -> shard count
	MinShardSize                int64
	MaxShardSize                int64
	AvgShardSize                int64
	TranslogSize                int64 // sum of translog size across all shards
	TranslogUncommittedSize     int64 // sum across all shards
	TranslogUncommittedOps      int64
	WorstTranslogSize           int64 // highest single shard
	WorstTranslogShardID        int
	WorstTranslogNodeName       string
	ShardsOverTranslogThreshold int // count of shards exceeding flush_threshold_size
	Settings                    TableSettings
}
