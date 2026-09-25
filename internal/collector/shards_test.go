package collector

import (
	"encoding/json"
	"strings"
	"testing"
)

// decisions must come before explanation, see allocationsQuery.
func TestAllocationsQueryColumnOrder(t *testing.T) {
	d, e := strings.Index(allocationsQuery, "decisions"), strings.Index(allocationsQuery, "explanation")
	if d < 0 || e < 0 || d > e {
		t.Fatalf("decisions (%d) must be selected before explanation (%d)", d, e)
	}
}

// Row shape as returned by CrateDB 6.3 for an unassignable replica.
func TestParseAllocations(t *testing.T) {
	var rows [][]interface{}
	err := json.Unmarshal([]byte(`[["doc","events","04166",0,false,"UNASSIGNED",null,
		[{"node_name":"lab3","explanations":["a copy of this shard is already allocated to this node [[.partitioned.events.04166][0], node[9yH7], [P], s[STARTED], a[id=utcG]]"],"node_id":"9yH7"},
		 {"node_name":"lab2","explanations":["a copy of this shard is already allocated to this node [[.partitioned.events.04166][0], node[M1Ak], [R], s[STARTED], a[id=H_rx]]"],"node_id":"M1Ak"}],
		"cannot allocate because allocation is not permitted to any of the nodes"]]`), &rows)
	if err != nil {
		t.Fatal(err)
	}
	got := parseAllocations(rows)
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	a := got[0]
	if a.PartitionIdent != "04166" || a.Explanation != "cannot allocate because allocation is not permitted to any of the nodes" {
		t.Errorf("alloc = %+v", a)
	}
	if len(a.Decisions) != 2 || a.Decisions[0].NodeName != "lab3" || !strings.HasPrefix(a.Decisions[1].Explanations[0], "a copy of this shard") {
		t.Errorf("decisions = %+v", a.Decisions)
	}
}

// The fast-path query has no translog columns; parsing its rows used to
// index past the end and crash obsi 5s after opening the Shards tab.
func TestParseShardRowsFastPath(t *testing.T) {
	row := []interface{}{float64(0), "doc", "t", "", float64(0), false, "UNASSIGNED", "UNASSIGNED", false, float64(0), nil, nil, nil, float64(0), nil}
	got := parseShardRows([][]interface{}{row})
	if len(got) != 1 || got[0].RoutingState != "UNASSIGNED" || got[0].TranslogSize != 0 {
		t.Fatalf("got %+v", got)
	}
}
