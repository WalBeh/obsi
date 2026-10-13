package cratedb

import (
	"regexp"
	"strconv"
	"strings"
)

// ShardFailure is one entry of sys.snapshots.failures.
type ShardFailure struct {
	Table     string // schema.table
	Partition string // partition ident, "" for a table that isn't partitioned
	Shard     int
	NodeID    string
	Reason    string
}

// shardFailureRe matches SnapshotShardFailure.toString:
// SnapshotShardFailure{shardId=[t1/WvwdaDRrRDC-4FAgmTzb9Q][3], reason='node shutdown', nodeId='5eAQ...'}
var shardFailureRe = regexp.MustCompile(`(?s)^SnapshotShardFailure\{shardId=\[([^\]/]+)(?:/[^\]]*)?\]\[(\d+)\], reason='(.*)', nodeId='([^']*)'\}$`)

// ParseShardFailure splits a failure as CrateDB words it; false when the
// wording isn't the expected one.
func ParseShardFailure(s string) (ShardFailure, bool) {
	m := shardFailureRe.FindStringSubmatch(s)
	if m == nil {
		return ShardFailure{}, false
	}
	shard, _ := strconv.Atoi(m[2])
	table, partition := IndexTable(m[1])
	node := m[4]
	if node == "null" {
		node = ""
	}
	return ShardFailure{Table: table, Partition: partition, Shard: shard, NodeID: node, Reason: m[3]}, true
}

// IndexTable is the table (schema.table) and partition ident behind an
// index name: t, s.t, .partitioned.t.<ident> or s..partitioned.t.<ident>.
func IndexTable(index string) (table, partition string) {
	schema, rest := "doc", index
	if i := strings.Index(index, "..partitioned."); i > 0 {
		schema, rest = index[:i], index[i+1:]
	}
	if p, ok := strings.CutPrefix(rest, ".partitioned."); ok {
		if i := strings.LastIndexByte(p, '.'); i > 0 {
			return schema + "." + p[:i], p[i+1:]
		}
		return schema + "." + p, ""
	}
	if i := strings.IndexByte(rest, '.'); i > 0 {
		return rest, ""
	}
	return schema + "." + rest, ""
}
