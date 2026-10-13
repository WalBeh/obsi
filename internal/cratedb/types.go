package cratedb

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
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
	if e.NotCrateDB() {
		msg := fmt.Sprintf("%s answered %d %q, which isn't CrateDB: something in front of it has no route to the cluster",
			e.Endpoint, e.StatusCode, strings.TrimSpace(e.Body))
		if u, err := url.Parse(e.Endpoint); err == nil && strings.HasSuffix(strings.TrimSuffix(u.Hostname(), "."), ".cratedb.net") {
			msg += ". On CrateDB Cloud a suspended cluster looks like this (suspending removes its route): resume it in the console"
		}
		return msg
	}
	return fmt.Sprintf("cratedb error (status %d) from %s: %s", e.StatusCode, e.Endpoint, e.Body)
}

// NotCrateDB is true for a 404 from /_sql that isn't JSON: CrateDB answers
// those as JSON, an ingress or proxy without a route as plain text
// ("404 page not found" from Traefik for a suspended Cloud cluster).
// CrateDB's own 401s are plain text too, so only 404 counts.
func (e *CrateDBError) NotCrateDB() bool {
	return e.StatusCode == http.StatusNotFound && !strings.HasPrefix(strings.TrimSpace(e.Body), "{")
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
	MaxShardsPerNode         int
	AllocationEnable         string // "all", "primaries", "new_primaries", "none"
	NodeConcurrentRecoveries int
	// NodeInitialPrimariesRecoveries caps primaries a node opens from its
	// own disk, a snapshot or a replication copy at once.
	NodeInitialPrimariesRecoveries int
	ClusterConcurrentRebalance     int
	RecoveryMaxBytesPerSec         string // e.g. "40mb"
	DiskWatermarkLow               string // e.g. "85%"
	DiskWatermarkHigh              string // e.g. "90%"
	DiskWatermarkFlood             string // e.g. "95%"
	RebalanceEnable                string // "all", "primaries", "replicas", "none"
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
	// Started and Finished are zero while running, and when CrateDB
	// couldn't read the snapshot's info from the repository (then FAILED).
	Started     time.Time
	Finished    time.Time
	Version     string // CrateDB version that wrote it
	Reason      string
	TotalShards int
	GlobalState bool     // include_global_state: settings, users, ... too
	Tables      []string // schema.table, whole tables
	Partitions  []SnapshotPartition
	Failures    []string // one per failed shard, as CrateDB words it
}

// SnapshotPartition is a partition a snapshot took on its own.
type SnapshotPartition struct {
	Table  string // schema.table
	Values []string
}

// Repository is a row of sys.repositories. CrateDB masks the credentials
// in Settings as [xxxxx].
type Repository struct {
	Name     string
	Type     string // fs, s3, azure, gcs, url
	Settings map[string]string
}

// ReadOnly is true when CrateDB refuses to write snapshots there.
func (r Repository) ReadOnly() bool {
	return r.Type == "url" || r.Settings["readonly"] == "true"
}

// replicationRepoPrefix names the internal repository a subscription's
// initial copy restores from (LogicalReplicationRepository).
const replicationRepoPrefix = "_logical_replication_"

// Subscription is the subscription whose initial copy this restore is,
// when it is one. sys.snapshot_restore lists it to every user, while
// pg_subscription lists a subscription only to its owner.
func (r SnapshotRestore) Subscription() (string, bool) {
	return strings.CutPrefix(r.Repository, replicationRepoPrefix)
}

// SnapshotRestore is a running RESTORE SNAPSHOT from sys.snapshot_restore,
// or a subscription's initial copy.
type SnapshotRestore struct {
	Repository string
	Name       string
	State      string // INIT, STARTED, SUCCESS, FAILURE
	Shards     int
	Done       int
	Failed     int
}

// ClusterCheck represents a row from sys.checks.
type ClusterCheck struct {
	ID          int
	Severity    int
	Description string
	Passed      bool
}

// NodeCheck represents a row from sys.node_checks: one check on one node.
type NodeCheck struct {
	ID           int
	NodeID       string
	Severity     int
	Description  string
	Passed       bool
	Acknowledged bool // a failed check the user acknowledged, see sys.node_checks
}

// ClusterHealth represents the row from sys.cluster_health. Missing and
// underreplicated shards are -1 when CrateDB can't count them, e.g. while no
// master is elected.
type ClusterHealth struct {
	Health          string // GREEN, YELLOW, RED
	Description     string
	PendingTasks    int64
	MissingShards   int64
	UnderReplicated int64
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
	AllocationFilters      map[string]string // e.g. "routing.allocation.exclude._name" -> "lab2"
	// NodeLeftDelay is unassigned.node_left.delayed_timeout as set on the
	// table, i.e. what new partitions copy. Each partition keeps its own.
	NodeLeftDelay time.Duration
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
	// NodeLeftDelayMin is the shortest delayed_timeout the table or any of
	// its partitions has, the one that decides when replicas get rebuilt;
	// PartitionsBelowDelay counts partitions shorter than the table's own.
	NodeLeftDelayMin     time.Duration
	PartitionsBelowDelay int
	Partitions           int // 0 for a table that isn't partitioned
}
