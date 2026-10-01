package tui

import (
	"strings"
	"testing"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func nodeSnap(id, name string) store.NodeSnapshot {
	return store.NodeSnapshot{NodeInfo: cratedb.NodeInfo{ID: id, Name: name}}
}

func TestRenderNodeChecks(t *testing.T) {
	m := NewOverviewModel(120, 40, true)
	if !strings.Contains(m.renderNodeChecks(), "not available") {
		t.Error("want a placeholder before sys.node_checks answered")
	}

	m.snap.Nodes = []store.NodeSnapshot{nodeSnap("i1", "hot-0"), nodeSnap("i2", "hot-1"), nodeSnap("i3", "hot-2")}
	m.snap.NodeChecks = []cratedb.NodeCheck{
		{ID: 1, NodeID: "i1", Severity: 2, Description: "recovery", Passed: true},
		{ID: 8, NodeID: "i1", Severity: 2, Description: "x", Passed: false},
		{ID: 8, NodeID: "i3", Severity: 2, Description: "x", Passed: false},
		{ID: 8, NodeID: "i2", Severity: 2, Description: "x", Passed: true},
		{ID: 5, NodeID: "i3", Severity: 3, Description: "x", Passed: false, Acknowledged: true},
	}
	out := m.renderNodeChecks()
	for _, want := range []string{"2 passed, 1 failed", "#8 shards reach 90% of cluster.max_shards_per_node on 2 of 3 nodes", "hot-0  hot-2", "fix: merge small tables"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "#5") && strings.Contains(out, "high disk watermark exceeded on") {
		t.Errorf("acknowledged check listed as failing:\n%s", out)
	}

	m.snap.NodeChecks = []cratedb.NodeCheck{{ID: 1, NodeID: "i1", Passed: true}, {ID: 2, NodeID: "i2", Passed: true}}
	if out := m.renderNodeChecks(); !strings.Contains(out, "All 2 checks passed on 2 nodes") {
		t.Errorf("all-green line:\n%s", out)
	}
}

func TestRenderClusterStatus(t *testing.T) {
	m := NewOverviewModel(120, 40, true)
	if m.renderClusterStatus() != "" {
		t.Error("no block before sys.cluster_health answered")
	}
	m.snap.ClusterStatus = &cratedb.ClusterHealth{Health: "RED", Description: "no master", PendingTasks: 2, MissingShards: -1, UnderReplicated: -1}
	out := m.renderClusterStatus()
	for _, want := range []string{"RED", "pending tasks 2", "missing shards unknown", "underreplicated unknown", "no master"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestCheckSeverityNames(t *testing.T) {
	for sev, want := range map[int]string{1: "LOW", 2: "WARN", 3: "CRIT", 9: "?"} {
		if got, _ := checkSeverity(sev); got != want {
			t.Errorf("severity %d = %s, want %s", sev, got, want)
		}
	}
}
