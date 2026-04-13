// Package testutil provides helpers for integration tests that need a real CrateDB instance.
package testutil

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	crateImage     = "crate:latest"
	startupTimeout = 60 * time.Second
	pollInterval   = 500 * time.Millisecond
)

// CrateDB holds connection details for the test container.
type CrateDB struct {
	Endpoint string // e.g. "http://localhost:44200"
	Port     string
}

var (
	sharedInstance *CrateDB
	sharedOnce    sync.Once
	sharedCleanup func()
)

// StartCrateDB returns a shared CrateDB instance for the current test binary.
// The first call starts a Docker container; subsequent calls reuse it.
// The container is cleaned up when the process exits.
// If CRATEDB_TEST_ENDPOINT is set, it uses that instead of starting a container.
func StartCrateDB(t *testing.T) *CrateDB {
	t.Helper()

	if ep := os.Getenv("CRATEDB_TEST_ENDPOINT"); ep != "" {
		t.Logf("using existing CrateDB at %s", ep)
		return &CrateDB{Endpoint: ep}
	}

	sharedOnce.Do(func() {
		port := freePort(t)
		containerName := fmt.Sprintf("obsi-test-%d", os.Getpid())

		// Stop any leftover container from a previous interrupted run
		_ = exec.Command("docker", "rm", "-f", containerName).Run()

		args := []string{
			"run", "-d",
			"--name", containerName,
			"-p", port + ":4200",
			"-e", "CRATE_HEAP_SIZE=256m",
			crateImage,
			"-Cdiscovery.type=single-node",
		}
		out, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker run failed: %v\n%s", err, out)
		}
		t.Logf("started CrateDB container %s on port %s", containerName, port)

		sharedCleanup = func() {
			out, err := exec.Command("docker", "rm", "-f", containerName).CombinedOutput()
			if err != nil {
				// Can't use t.Logf here — test may have finished
				fmt.Fprintf(os.Stderr, "docker rm failed: %v\n%s", err, out)
			}
		}

		endpoint := "http://localhost:" + port
		waitForCrateDB(t, endpoint)

		sharedInstance = &CrateDB{Endpoint: endpoint, Port: port}
	})

	t.Cleanup(func() {
		// The last test to finish triggers container removal.
		// sync.Once ensures the container isn't removed while other tests still use it,
		// because cleanup funcs in Go run in LIFO order within a single test,
		// but across tests the container outlives them all — it's cleaned up at process exit.
	})

	if sharedInstance == nil {
		t.Fatal("CrateDB container failed to start")
	}
	return sharedInstance
}

// Cleanup removes the shared container. Call from TestMain after m.Run().
func Cleanup() {
	if sharedCleanup != nil {
		sharedCleanup()
	}
}

func waitForCrateDB(t *testing.T, endpoint string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	client := &http.Client{Timeout: 2 * time.Second}

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("CrateDB did not become ready within %s", startupTimeout)
		default:
		}

		body := strings.NewReader(`{"stmt":"SELECT 1"}`)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/_sql", body)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Logf("CrateDB ready at %s", endpoint)
				return
			}
		}
		time.Sleep(pollInterval)
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return fmt.Sprintf("%d", port)
}
