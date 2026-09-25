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
		fixes := diagnoseShard(replica, tc.a, tc.cs, "3")
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

func TestReplicasCause(t *testing.T) {
	for replicas, want := range map[string]string{
		"3":   "doc.events: 3 replicas + primary = 4 copies per shard, but only 3 nodes (one copy per node)",
		"0-5": "doc.events: number_of_replicas = 0-5 needs more than 3 nodes (one copy per node)",
		"":    "doc.events: more copies per shard than 3 nodes (one copy per node)",
	} {
		if got := replicasCause("doc.events", replicas, 3); got != want {
			t.Errorf("replicas %q: got %q", replicas, got)
		}
	}
}

// Every shard STARTED, but doc.big sits on lab2 against its own exclude
// filter with nowhere to go: the tab says so and offers the reset.
func TestStuckShards(t *testing.T) {
	stuck := cratedb.AllocationInfo{TableSchema: "doc", TableName: "big", ShardID: 2, NodeID: "n2", CurrentState: "STARTED"}
	m := NewShardsModel(160, 40).Refresh(store.StoreSnapshot{
		Shards: []cratedb.ShardInfo{{SchemaName: "doc", TableName: "big", ID: 2, Primary: true, RoutingState: "STARTED", NodeID: "n2", NodeName: "lab2"}},
		Tables: []cratedb.TableInfo{{SchemaName: "doc", TableName: "big",
			Settings: cratedb.TableSettings{AllocationFilters: map[string]string{"routing.allocation.exclude._name": "lab2"}}}},
		StuckShards: []cratedb.AllocationInfo{stuck},
	})
	v := m.View()
	for _, want := range []string{"All 1 shards started", "doc.big can't stay on lab2", `ALTER TABLE "doc"."big" RESET ("routing.allocation.exclude._name")`} {
		if !strings.Contains(v, want) {
			t.Errorf("view missing %q:\n%s", want, v)
		}
	}
	if f, ok := m.chosenFix(); !ok || !strings.HasPrefix(f.stmt, `ALTER TABLE "doc"."big" RESET`) {
		t.Errorf("chosenFix = %+v %v", f, ok)
	}

	// Without a table filter there's no statement to offer.
	if f := stuckFix(stuck, nil, "lab2"); f.stmt != "" || !strings.Contains(f.cause, "cluster allocation filter or high watermark") {
		t.Errorf("no-filter fix = %+v", f)
	}
}
