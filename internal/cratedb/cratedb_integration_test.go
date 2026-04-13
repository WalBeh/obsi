//go:build integration

package cratedb_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/WalBeh/obsi/internal/cratedb"
	"github.com/WalBeh/obsi/internal/testutil"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.Cleanup()
	os.Exit(code)
}

func TestClientCrateDBErrorType(t *testing.T) {
	cdb := testutil.StartCrateDB(t)
	client := cratedb.NewClient(cdb.Endpoint, "crate", "", 5*time.Second, false)
	ctx := context.Background()

	// CrateDB application errors (bad SQL, missing table) must surface as
	// *CrateDBError so the Registry knows not to failover.
	_, err := client.Query(ctx, "SELECT * FROM nonexistent_schema.nonexistent_table")
	if err == nil {
		t.Fatal("expected error for invalid table")
	}
	var crateErr *cratedb.CrateDBError
	if !errors.As(err, &crateErr) {
		t.Fatalf("expected CrateDBError, got %T: %v", err, err)
	}
	if crateErr.StatusCode < 400 || crateErr.StatusCode >= 500 {
		t.Errorf("expected 4xx status, got %d", crateErr.StatusCode)
	}
}

func TestRegistryDoesNotFailoverOnCrateDBError(t *testing.T) {
	cdb := testutil.StartCrateDB(t)
	ctx := context.Background()

	reg := cratedb.NewRegistry(
		cdb.Endpoint, "crate", "",
		3*time.Second, 10*time.Second, 5*time.Second, 30*time.Second, false,
	)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// First, do a successful query so ActiveNode is set to "loadbalancer"
	if _, err := reg.Query(ctx, "SELECT 1"); err != nil {
		t.Fatalf("setup query: %v", err)
	}

	// Now send bad SQL. If the registry incorrectly attempts failover,
	// the error would be wrapped as "all nodes failed" instead of CrateDBError.
	_, err := reg.Query(ctx, "SELECT * FROM nonexistent_schema.nonexistent_table")
	if err == nil {
		t.Fatal("expected error")
	}

	var crateErr *cratedb.CrateDBError
	if !errors.As(err, &crateErr) {
		t.Fatalf("expected CrateDBError (no failover attempted), got %T: %v", err, err)
	}

	// ActiveNode should still be "loadbalancer" — no failover happened
	status := reg.Status()
	if status.ActiveNode != "loadbalancer" {
		t.Errorf("expected ActiveNode='loadbalancer' (no failover), got %q", status.ActiveNode)
	}
}

func TestRegistryFailoverToDirect(t *testing.T) {
	cdb := testutil.StartCrateDB(t)
	ctx := context.Background()

	// Set up a proxy in front of CrateDB that we can break mid-test.
	proxyUp := true
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !proxyUp {
			// Simulate dead LB: kill the connection immediately
			hj, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "hijack unsupported", http.StatusInternalServerError)
				return
			}
			conn, _, _ := hj.Hijack()
			conn.Close()
			return
		}
		// Forward to real CrateDB
		proxyReq, _ := http.NewRequestWithContext(r.Context(), r.Method, cdb.Endpoint+r.URL.Path, r.Body)
		proxyReq.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(proxyReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	// Bootstrap through the proxy — this discovers real nodes as direct endpoints
	reg := cratedb.NewRegistry(
		proxy.URL, "crate", "",
		500*time.Millisecond, // short ping timeout
		2*time.Second,
		5*time.Second,
		30*time.Second,
		false,
	)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	status := reg.Status()
	if status.TotalNodes < 1 {
		t.Skip("need at least 1 discovered direct node for failover test")
	}

	// Confirm normal path: query goes through the proxy ("loadbalancer")
	if _, err := reg.Query(ctx, "SELECT 1"); err != nil {
		t.Fatalf("query through proxy: %v", err)
	}
	if s := reg.Status(); s.ActiveNode != "loadbalancer" {
		t.Fatalf("expected loadbalancer, got %q", s.ActiveNode)
	}

	// Kill the proxy — simulates LB going down
	proxyUp = false

	// Query should failover to a direct node
	resp, err := reg.Query(ctx, "SELECT 1 AS val")
	if err != nil {
		t.Fatalf("failover query failed: %v", err)
	}
	if resp.RowCount != 1 {
		t.Errorf("expected 1 row from failover, got %d", resp.RowCount)
	}

	s := reg.Status()
	if s.ActiveNode == "loadbalancer" || s.ActiveNode == "" {
		t.Errorf("expected direct node as active after failover, got %q", s.ActiveNode)
	}
	t.Logf("failover: primary down → routed to %q", s.ActiveNode)
}
