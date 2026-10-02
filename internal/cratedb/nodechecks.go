package cratedb

import "strings"

// nodeCheckTitles are short names for sys.node_checks entries; CrateDB's
// descriptions are a paragraph plus a docs link (checked on 6.4.5).
var nodeCheckTitles = map[int]string{
	1: "gateway.expected_data_nodes unset or not the number of data nodes",
	2: "gateway.recover_after_data_nodes unset or low",
	3: "gateway.recover_after_time is 0 while expected data nodes are set",
	5: "high disk watermark exceeded",
	6: "low disk watermark exceeded",
	7: "flood stage disk watermark exceeded",
	8: "shards reach 90% of cluster.max_shards_per_node",
}

// NodeCheckTitle names a node check in one line: a known title, else the
// first sentence of its description.
func NodeCheckTitle(id int, description string) string {
	if t, ok := nodeCheckTitles[id]; ok {
		return t
	}
	line, _, _ := strings.Cut(strings.TrimSpace(description), "\n")
	if i := strings.Index(line, ". "); i > 0 {
		line = line[:i]
	}
	return strings.TrimSuffix(line, ".")
}
