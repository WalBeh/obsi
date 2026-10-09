package cratedb

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client talks to a single CrateDB endpoint via the HTTP /_sql API.
type Client struct {
	baseURL     string
	httpClient  *http.Client
	username    string
	password    string
	baseTimeout time.Duration // configured timeout (before latency adjustment)
	lastLatency time.Duration // latency of the most recent successful query
}

// NewClient creates a new CrateDB HTTP client.
// If skipVerify is true, TLS certificate verification is disabled
// (useful for port-forwarding to clusters with certs for their real hostname).
func NewClient(baseURL, username, password string, timeout time.Duration, skipVerify bool) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if skipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // user-requested for port-forwarding
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		username:    username,
		password:    password,
		baseTimeout: timeout,
	}
}

// unbounded is a copy whose requests are bounded by their context only, for
// statements expected to outlast the query timeout.
func (c *Client) unbounded() *Client {
	cp := *c
	hc := *c.httpClient
	hc.Timeout = 0
	cp.httpClient = &hc
	return &cp
}

// Query executes a SQL statement against CrateDB and returns the response.
func (c *Client) Query(ctx context.Context, stmt string, args ...interface{}) (*SQLResponse, error) {
	reqBody := SQLRequest{
		Stmt: stmt,
	}
	if len(args) > 0 {
		reqBody.Args = args
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/_sql", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request to %s: %w", c.baseURL, err)
	}
	c.lastLatency = time.Since(start)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &CrateDBError{
			StatusCode: resp.StatusCode,
			Endpoint:   c.baseURL,
			Body:       string(respBody),
		}
	}

	var sqlResp SQLResponse
	if err := json.Unmarshal(respBody, &sqlResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &sqlResp, nil
}

// Ping sends a lightweight SELECT 1 and returns the latency.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	_, err := c.Query(ctx, "SELECT 1"+QueryTag)
	return time.Since(start), err
}

// Identify returns sys.cluster's id and name. The id is random per cluster
// and survives restarts; names repeat across clusters (e.g. "crate" on k8s).
func (c *Client) Identify(ctx context.Context) (ClusterIdentity, time.Duration, error) {
	start := time.Now()
	resp, err := c.Query(ctx, "SELECT id, name FROM sys.cluster"+QueryTag)
	if err != nil {
		return ClusterIdentity{}, time.Since(start), err
	}
	var id ClusterIdentity
	if len(resp.Rows) > 0 {
		id = ClusterIdentity{ID: ToString(resp.Rows[0][0]), Name: ToString(resp.Rows[0][1])}
	}
	return id, time.Since(start), nil
}

// BaseURL returns the client's base URL.
func (c *Client) BaseURL() string {
	return c.baseURL
}
