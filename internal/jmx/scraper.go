package jmx

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Scraper fetches a Prometheus text exposition over HTTP and turns it into
// typed snapshots. It is a thin shell over (Parse, Extract) so that
// retry/back-off, logging and store-update concerns can live in the
// collector layer above.
type Scraper struct {
	URL    string
	Client *http.Client
}

// NewScraper builds a scraper with a sensible default HTTP client. The
// caller may swap Client out for tests or to share a transport.
func NewScraper(url string, timeout time.Duration) *Scraper {
	return &Scraper{
		URL: url,
		Client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Fetch performs one scrape cycle: GET, parse, extract. expectedClusterName
// is forwarded to Extract; pass "" to skip the safety guard (do not do this
// in production paths).
func (s *Scraper) Fetch(ctx context.Context, expectedClusterName string) (*Extracted, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "text/plain")

	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", s.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Drain a small slice of the body for diagnostics — useful when
		// croudng returns 503 with a reason string in the body.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("get %s: status %d: %s", s.URL, resp.StatusCode, body)
	}

	scrape, err := Parse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return Extract(scrape, expectedClusterName)
}
