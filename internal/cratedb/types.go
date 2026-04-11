package cratedb

import "time"

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
	ID         string
	Name       string
	Hostname   string
	RestURL    string
	Version    string
	Load       [3]float64
	HeapUsed   int64
	HeapMax    int64
	HeapFree   int64
	FSTotal    int64
	FSUsed     int64
	FSAvail    int64
	CPUPercent int16  // from process['cpu']['percent'] (reliable across OS)
	CPUSystem  int16  // from os['cpu']['system'] (may be -1 on some OS)
	CPUUser    int16  // from os['cpu']['user'] (may be -1 on some OS)
	MemUsed    int64
	MemFree    int64
	MemTotal   int64
	FSReads        int64  // cumulative read ops from fs['total']['reads']
	FSWrites       int64  // cumulative write ops from fs['total']['writes']
	FSBytesRead    int64  // cumulative bytes read from fs['total']['bytes_read']
	FSBytesWritten int64  // cumulative bytes written from fs['total']['bytes_written']
	Zone           string // from attributes['zone']
	NodeRole       string // from attributes['node_name'] (hot, warm, cold, master)
	IsMaster       bool   // from is_master
	NumCPUs        int    // from os_info['available_processors']
	JVMVersion string // from os_info['jvm']['version']
	JVMName    string // from os_info['jvm']['vm_name']
}

// NodeHealth tracks heartbeat state for one node.
type NodeHealth struct {
	NodeID           string
	Reachable        bool
	LastSeen         time.Time
	LastLatency      time.Duration
	ConsecutiveFails int
}

// ClusterSettings holds key cluster-level settings from sys.cluster.
type ClusterSettings struct {
	MaxShardsPerNode          int
	AllocationEnable          string // "all", "primaries", "new_primaries", "none"
	NodeConcurrentRecoveries  int
	ClusterConcurrentRebalance int
	RecoveryMaxBytesPerSec    string // e.g. "40mb"
	DiskWatermarkLow          string // e.g. "85%"
	DiskWatermarkHigh         string // e.g. "90%"
	DiskWatermarkFlood        string // e.g. "95%"
}

// Summit is a random mountain from sys.summits — CrateDB's Easter egg.
type Summit struct {
	Mountain    string
	Height      int
	Region      string
	Country     string
	FirstAscent int
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
	TableSchema   string
	TableName     string
	Health        string
	MissingShards int64
	UnderReplicated int64
	Partition     string
}

// ActiveQuery represents a row from sys.jobs.
type ActiveQuery struct {
	ID      string
	Node    string
	Started time.Time
	Stmt    string
	Username string
}

// ShardInfo represents a row from sys.shards.
type ShardInfo struct {
	ID           int
	SchemaName   string
	TableName    string
	PartitionIdent string
	NumDocs      int64
	Primary      bool
	State        string
	RoutingState string
	Relocating   bool
	Size         int64
	NodeID       string
	NodeName     string
	RecoveryStage   string
	RecoveryPercent float64
}

// TableSettings holds table-level configuration from information_schema.tables.
type TableSettings struct {
	NumberOfShards   int
	NumberOfReplicas string // can be "0-1", "1", etc.
	ClusteredBy      string
	PartitionedBy    []string
	ColumnPolicy     string
	RefreshInterval  int    // ms
	Codec            string
}

// TableInfo is an aggregated view of a table with shard distribution.
type TableInfo struct {
	SchemaName     string
	TableName      string
	TotalShards    int
	PrimaryShards  int
	ReplicaShards  int
	TotalRecords   int64
	TotalSize      int64 // primary shards only
	TotalDiskSize  int64 // all shards including replicas
	Health         string
	ShardsPerNode  map[string]int // nodeName -> shard count
	MinShardSize   int64
	MaxShardSize   int64
	AvgShardSize   int64
	Settings       TableSettings
}
