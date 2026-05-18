package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// formatQueryDump is what `y` writes to the clipboard. Lock the format so a
// drive-by render change doesn't silently break what users paste into tickets.
func TestFormatQueryDump(t *testing.T) {
	jobStart := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	q := cratedb.ActiveQuery{
		ID:        "abc-123",
		Node:      "data-0",
		Started:   jobStart,
		Stmt:      "SELECT * FROM big_table",
		Username:  "alice",
		UsedBytes: 42 << 20,
		Operations: []cratedb.Operation{
			{Name: "COLLECT", NodeName: "data-1", UsedBytes: 40 << 20, Started: jobStart.Add(100 * time.Millisecond)},
			{Name: "MERGE", NodeName: "data-2", UsedBytes: 2 << 20, Started: jobStart.Add(500 * time.Millisecond)},
		},
	}

	dump := formatQueryDump(q)

	for _, want := range []string{
		"Job ID:   abc-123",
		"User:     alice",
		"Memory:   42.0MB (sum of 2 operation(s))",
		"Operations:",
		"- COLLECT on data-1: 40.0MB",
		"- MERGE on data-2: 2.0MB",
		"Statement:\nSELECT * FROM big_table",
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("dump missing %q\nfull dump:\n%s", want, dump)
		}
	}

	// Planning-phase job with no operations still produces a usable dump.
	bare := cratedb.ActiveQuery{
		ID:       "bare",
		Stmt:     "SELECT 1",
		Started:  jobStart,
		Username: "bob",
	}
	bareDump := formatQueryDump(bare)
	if strings.Contains(bareDump, "Operations:") {
		t.Errorf("expected no Operations section for op-less job, got:\n%s", bareDump)
	}
	if !strings.HasSuffix(bareDump, "\n") {
		t.Errorf("dump must end with newline")
	}
}
