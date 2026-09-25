package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func decision(node string, exps ...string) cratedb.AllocationDecision {
	return cratedb.AllocationDecision{NodeName: node, Explanations: exps}
}

// Explanation texts as CrateDB 6.3 returns them on the lab cluster.
func TestDiagnoseShard(t *testing.T) {
	replica := cratedb.ShardInfo{SchemaName: "doc", TableName: "events", RoutingState: "UNASSIGNED"}
	const holds = "a copy of this shard is already allocated to this node [[.partitioned.events.04166][0], node[x], [P], s[STARTED], a[id=y]]"
	for name, tc := range map[string]struct {
		a    cratedb.AllocationInfo
		cs   cratedb.ClusterSettings
		keys string
		stmt string
	}{
		"too many replicas": {
			a:    cratedb.AllocationInfo{Decisions: []cratedb.AllocationDecision{decision("lab1", holds), decision("lab2", holds), decision("lab3", holds)}},
			keys: "replicas/doc.events",
			stmt: `ALTER TABLE "doc"."events" SET (number_of_replicas = 2)`,
		},
		"allocation disabled": {
			a:    cratedb.AllocationInfo{Decisions: []cratedb.AllocationDecision{decision("lab1", "replica allocations are forbidden due to cluster setting [cluster.routing.allocation.enable=primaries]")}},
			cs:   cratedb.ClusterSettings{AllocationEnable: "primaries"},
			keys: "enable",
			stmt: `RESET GLOBAL "cluster.routing.allocation.enable"`,
		},
		"filter": {
			a:    cratedb.AllocationInfo{Decisions: []cratedb.AllocationDecision{decision("lab1", `node matches index setting [index.routing.allocation.exclude] filters [_name:"lab1"]`)}},
			keys: "filter/doc.events/routing.allocation.exclude._name",
			stmt: `ALTER TABLE "doc"."events" RESET ("routing.allocation.exclude._name")`,
		},
		"delayed": {
			a:    cratedb.AllocationInfo{Explanation: "cannot allocate because the cluster is still waiting 42.1s for the departed node holding a replica to rejoin, despite being allowed to allocate the shard to at least one other node"},
			keys: "delayed",
		},
		"watermark": {
			a:    cratedb.AllocationInfo{Decisions: []cratedb.AllocationDecision{decision("lab2", "the node is above the low watermark cluster setting [cluster.routing.allocation.disk.watermark.low=85%]"), decision("lab1")}},
			keys: "watermark",
		},
		"retry limit": {
			a:    cratedb.AllocationInfo{Decisions: []cratedb.AllocationDecision{decision("lab1", "shard has exceeded the maximum number of retries [5] on failed allocation attempts")}},
			keys: "retry",
			stmt: retryFailedStmt,
		},
	} {
		fixes := diagnoseShard(replica, tc.a, tc.cs)
		var keys []string
		stmt := ""
		for _, f := range fixes {
			keys = append(keys, f.key)
			if f.stmt != "" && stmt == "" {
				stmt = f.stmt
			}
		}
		if strings.Join(keys, ",") != tc.keys || stmt != tc.stmt {
			t.Errorf("%s: keys=%v stmt=%q, want %s / %q", name, keys, stmt, tc.keys, tc.stmt)
		}
	}
}

func fixModel(readOnly bool, a cratedb.AllocationInfo) ShardsModel {
	m := NewShardsModel(120, 40)
	m.readOnly = readOnly
	a.TableSchema, a.TableName, a.CurrentState = "doc", "t", "UNASSIGNED"
	return m.Refresh(store.StoreSnapshot{
		Shards:      []cratedb.ShardInfo{{SchemaName: "doc", TableName: "t", Primary: true, RoutingState: "STARTED", NodeID: "n1"}},
		Allocations: []cratedb.AllocationInfo{a},
	})
}

func TestRunFixReadOnly(t *testing.T) {
	retry := cratedb.AllocationInfo{Decisions: []cratedb.AllocationDecision{decision("lab1", "shard has exceeded the maximum number of retries [5]")}}
	filter := cratedb.AllocationInfo{Decisions: []cratedb.AllocationDecision{decision("lab1", `node does not match index setting [index.routing.allocation.require] filters [_name:"lab9"]`)}}

	// RETRY FAILED runs from read-only, behind the confirm.
	m, _ := fixModel(true, retry).HandleKey(keyRune('x'))
	if m.fixTarget == nil || m.fixTarget.stmt != retryFailedStmt {
		t.Fatalf("x did not open the confirm for RETRY FAILED: %+v", m.fixTarget)
	}
	m, cmd := m.HandleKey(keyRune('y'))
	if m.fixTarget != nil || cmd == nil {
		t.Fatal("y did not confirm")
	}
	if msg, ok := cmd().(RunFixMsg); !ok || msg.Stmt != retryFailedStmt {
		t.Errorf("cmd = %#v", msg)
	}

	// A DDL fix is refused in read-only mode...
	m, _ = fixModel(true, filter).HandleKey(keyRune('x'))
	if m.fixTarget != nil || !strings.Contains(m.noticeText, "read-only") {
		t.Errorf("DDL fix not refused: target=%v notice=%q", m.fixTarget, m.noticeText)
	}
	// ...and asked for with --read-write.
	m, _ = fixModel(false, filter).HandleKey(keyRune('x'))
	if m.fixTarget == nil || !strings.HasPrefix(m.fixTarget.stmt, `ALTER TABLE "doc"."t" RESET`) {
		t.Errorf("fixTarget = %+v", m.fixTarget)
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEsc})
	if m.fixTarget != nil {
		t.Error("esc did not cancel")
	}
}
