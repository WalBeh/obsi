package collector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// Row shapes follow docs/admin/system-information.rst of CrateDB 6.5.
func TestParseNodeChecks(t *testing.T) {
	var rows [][]interface{}
	if err := json.Unmarshal([]byte(`[
		[5,"n1",3,"The high disk watermark is exceeded on the node.",false,false],
		[8,"n2",2,"The amount of shards on the node reached 90 %",false,true],
		[8,"n3",2,"The amount of shards on the node reached 90 %",true,false],
		[1,"n1"]]`), &rows); err != nil {
		t.Fatal(err)
	}
	got := parseNodeChecks(rows)
	if len(got) != 3 {
		t.Fatalf("len = %d, the short row must be skipped", len(got))
	}
	if c := got[0]; c.ID != 5 || c.NodeID != "n1" || c.Severity != 3 || c.Passed || c.Acknowledged {
		t.Errorf("row 0 = %+v", c)
	}
	if c := got[1]; !c.Acknowledged || c.Passed {
		t.Errorf("row 1 = %+v", c)
	}
}

func TestParseClusterHealthUnknownCounts(t *testing.T) {
	var rows [][]interface{}
	_ = json.Unmarshal([]byte(`[["RED","no master",3,-1,-1]]`), &rows)
	h := parseClusterHealth(rows)
	if h == nil || h.Health != "RED" || h.PendingTasks != 3 || h.MissingShards != -1 || h.UnderReplicated != -1 {
		t.Fatalf("got %+v", h)
	}
	if parseClusterHealth(nil) != nil {
		t.Error("no row must give nil")
	}
}

func healthServer(t *testing.T, nodeChecksFail bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Stmt string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		var rows [][]interface{}
		switch {
		case strings.Contains(req.Stmt, "FROM sys.cluster_health"):
			rows = [][]interface{}{{"YELLOW", "", 0, 0, 4}}
		case strings.Contains(req.Stmt, "FROM sys.node_checks"):
			if nodeChecksFail {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": map[string]interface{}{"message": "Relation 'sys.node_checks' unknown", "code": 4041}})
				return
			}
			rows = [][]interface{}{
				{8, "n1", 2, "shards reach 90%", false, false},
				{8, "n2", 2, "shards reach 90%", true, false},
				{1, "n1", 1, "recovery settings", true, false},
			}
		case strings.Contains(req.Stmt, "FROM sys.checks"):
			rows = [][]interface{}{{2, 2, "partitions", true}}
		case strings.Contains(req.Stmt, "FROM sys.health"):
			rows = [][]interface{}{}
		case strings.Contains(req.Stmt, "FROM sys.cluster"):
			rows = [][]interface{}{{"aaaa-1111", "crate"}}
		case strings.Contains(req.Stmt, "FROM sys.nodes"):
			rows = [][]interface{}{{"n1", "n1", "n1", ""}}
		}
		_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{Rows: rows})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func collectHealth(t *testing.T, srv *httptest.Server) *store.Store {
	t.Helper()
	cfg := config.DefaultConfig()
	reg := cratedb.NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
	st := store.New(10, cfg.Collectors)
	c := NewHealthCollector(cfg.Collectors["health"], NewQueryTracker(cfg.Collectors, cfg.Connection))
	if err := c.Collect(context.Background(), reg, st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestHealthCollectorStoresClusterStatusAndNodeChecks(t *testing.T) {
	st := collectHealth(t, healthServer(t, false))
	snap := st.Snapshot(1, store.SnapshotHint{IncludeHealth: true})
	if snap.ClusterStatus == nil || snap.ClusterStatus.Health != "YELLOW" || snap.ClusterStatus.UnderReplicated != 4 {
		t.Errorf("cluster status = %+v", snap.ClusterStatus)
	}
	if len(snap.NodeChecks) != 3 || len(snap.ClusterChecks) != 1 {
		t.Errorf("node checks %d, cluster checks %d", len(snap.NodeChecks), len(snap.ClusterChecks))
	}
}

// An older CrateDB, or a user without privileges, has no sys.node_checks:
// the cluster checks collected before it must survive.
func TestHealthCollectorSurvivesMissingNodeChecks(t *testing.T) {
	st := collectHealth(t, healthServer(t, true))
	snap := st.Snapshot(1, store.SnapshotHint{IncludeHealth: true})
	if len(snap.ClusterChecks) != 1 || snap.ClusterStatus == nil || len(snap.NodeChecks) != 0 {
		t.Errorf("checks %d, status %+v, node checks %d", len(snap.ClusterChecks), snap.ClusterStatus, len(snap.NodeChecks))
	}
}
