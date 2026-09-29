package cratedb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCluster answers /_sql as whichever cluster is set, like a
// port-forward pointed at one cluster and then another.
type fakeCluster struct {
	id atomic.Value // "uuid/name"
}

func (f *fakeCluster) set(id, name string) { f.id.Store(id + "/" + name) }

func (f *fakeCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct{ Stmt string }
	_ = json.NewDecoder(r.Body).Decode(&req)
	id, name, _ := strings.Cut(f.id.Load().(string), "/")
	var rows [][]interface{}
	switch {
	case strings.Contains(req.Stmt, "FROM sys.cluster"):
		rows = [][]interface{}{{id, name}}
	case strings.Contains(req.Stmt, "FROM sys.nodes"):
		rows = [][]interface{}{{name + "-node", name + "-0", name + "-0", ""}}
	}
	_ = json.NewEncoder(w).Encode(SQLResponse{Rows: rows})
}

func TestRegistryNoticesAnotherCluster(t *testing.T) {
	fake := &fakeCluster{}
	fake.set("aaaa-1111", "crate")
	srv := httptest.NewServer(fake)
	defer srv.Close()

	ctx := context.Background()
	reg := NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	recovered := make(chan struct{}, 4)
	reg.OnRecovery(func() { recovered <- struct{}{} })

	// Same name, different cluster: only the id tells them apart.
	fake.set("bbbb-2222", "crate")
	reg.runHeartbeat(ctx)
	st := reg.Status()
	if st.Foreign == nil || st.Foreign.ID != "bbbb-2222" || st.ClusterID != "aaaa-1111" {
		t.Fatalf("foreign cluster not noticed: %+v", st)
	}
	if _, err := reg.Query(ctx, "SELECT 1"); !errors.Is(err, ErrClusterChanged) {
		t.Errorf("query against the other cluster ran: %v", err)
	}
	if err := reg.Refresh(ctx); !errors.Is(err, ErrClusterChanged) {
		t.Errorf("node refresh took the other cluster's nodes: %v", err)
	}

	// Back to the original: carries on by itself.
	fake.set("aaaa-1111", "crate")
	reg.runHeartbeat(ctx)
	if st := reg.Status(); st.Foreign != nil {
		t.Fatalf("still foreign after the original came back: %+v", st.Foreign)
	}
	if _, err := reg.Query(ctx, "SELECT 1"); err != nil {
		t.Errorf("query after the original came back: %v", err)
	}
	<-recovered

	fake.set("cccc-3333", "other")
	reg.runHeartbeat(ctx)
	reg.AcceptCluster(ctx)
	<-recovered
	st = reg.Status()
	if st.Foreign != nil || st.ClusterName != "other" || st.ClusterID != "cccc-3333" {
		t.Fatalf("after switch: %+v", st)
	}
	if len(st.Nodes) != 1 {
		t.Errorf("nodes after switch = %d, want the new cluster's one", len(st.Nodes))
	}
	if _, err := reg.Query(ctx, "SELECT 1"); err != nil {
		t.Errorf("query after switch: %v", err)
	}
}
