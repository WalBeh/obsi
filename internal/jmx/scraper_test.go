package jmx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// TestScraper_Success serves the captured croudng fixture from a test server
// and asserts the end-to-end Fetch produces the expected cluster identity.
func TestScraper_Success(t *testing.T) {
	srv := serveFile(t, "testdata/croudng-sample.txt")
	defer srv.Close()

	s := NewScraper(srv.URL, 5*time.Second)
	ex, err := s.Fetch(context.Background(), fixtureCluster)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if ex.Cluster.Name != fixtureCluster {
		t.Errorf("cluster = %q, want %q", ex.Cluster.Name, fixtureCluster)
	}
	if len(ex.Pods) == 0 {
		t.Error("no pods extracted")
	}
}

// TestScraper_ClusterMismatch ensures the safety guard fires end-to-end:
// even with a healthy 200 response, a wrong expected cluster aborts the
// fetch with ErrClusterMismatch.
func TestScraper_ClusterMismatch(t *testing.T) {
	srv := serveFile(t, "testdata/croudng-sample.txt")
	defer srv.Close()

	s := NewScraper(srv.URL, 5*time.Second)
	_, err := s.Fetch(context.Background(), "wrong-cluster")
	if !errors.Is(err, ErrClusterMismatch) {
		t.Errorf("err = %v, want wrapping ErrClusterMismatch", err)
	}
}

// TestScraper_HTTPError verifies non-200 responses surface as descriptive
// errors that include status and any body croudng put in there.
func TestScraper_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream rate limited", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	s := NewScraper(srv.URL, 5*time.Second)
	_, err := s.Fetch(context.Background(), fixtureCluster)
	if err == nil {
		t.Fatal("expected error for 503 response")
	}
	if !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "rate limited") {
		t.Errorf("error %q should mention status and body", err)
	}
}

// TestScraper_UnreachableEndpoint covers the "croudng not started" path —
// the most common operational failure. The error must surface the URL so
// the collector can render a useful reminder.
func TestScraper_UnreachableEndpoint(t *testing.T) {
	s := NewScraper("http://127.0.0.1:1/metrics", 200*time.Millisecond)
	_, err := s.Fetch(context.Background(), fixtureCluster)
	if err == nil {
		t.Fatal("expected error for unreachable endpoint")
	}
	if !strings.Contains(err.Error(), "127.0.0.1:1") {
		t.Errorf("error %q should mention the endpoint URL", err)
	}
}

func serveFile(t *testing.T, path string) *httptest.Server {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(body)
	}))
}
