package collector

import (
	"testing"
	"time"
)

// Three jobs from a LEFT JOIN against sys.operations:
//   - job-A has two ops, larger first
//   - job-B has one op
//   - job-C has no ops yet (NULL columns from the LEFT JOIN)
//
// Verifies sum, sort, NodeName mapping, and that ops-less jobs survive.
func TestAggregateActiveQueries(t *testing.T) {
	jobStart := float64(time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC).UnixMilli())
	opStartA1 := jobStart + 100
	opStartA2 := jobStart + 400
	opStartB := jobStart + 50

	rows := [][]interface{}{
		// SQL ORDER BY puts the larger op first, but the aggregator must
		// also sort defensively. Feed them in reverse to prove it.
		{"job-A", "data-0", jobStart, "SELECT * FROM big", "alice",
			"MERGE", float64(2 << 20), "data-1", opStartA2},
		{"job-A", "data-0", jobStart, "SELECT * FROM big", "alice",
			"COLLECT", float64(40 << 20), "data-2", opStartA1},
		{"job-B", "data-1", jobStart + 10, "INSERT ...", "bob",
			"INSERT", float64(8 << 20), "data-0", opStartB},
		// LEFT JOIN: no operation row yet for job-C.
		{"job-C", "data-2", jobStart + 20, "SELECT pg_sleep(1)", "carol",
			nil, nil, nil, nil},
	}

	got := aggregateActiveQueries(rows)

	if len(got) != 3 {
		t.Fatalf("want 3 jobs, got %d", len(got))
	}

	jobA := got[0]
	if jobA.ID != "job-A" || jobA.Username != "alice" || jobA.Node != "data-0" {
		t.Errorf("job-A header wrong: %+v", jobA)
	}
	if jobA.UsedBytes != (40<<20)+(2<<20) {
		t.Errorf("job-A UsedBytes: want %d, got %d", (40<<20)+(2<<20), jobA.UsedBytes)
	}
	if len(jobA.Operations) != 2 {
		t.Fatalf("job-A: want 2 ops, got %d", len(jobA.Operations))
	}
	if jobA.Operations[0].Name != "COLLECT" {
		t.Errorf("job-A: want largest op COLLECT first, got %s", jobA.Operations[0].Name)
	}
	if jobA.Operations[0].NodeName != "data-2" {
		t.Errorf("job-A op[0] node: want data-2, got %q", jobA.Operations[0].NodeName)
	}

	jobC := got[2]
	if jobC.ID != "job-C" {
		t.Fatalf("expected job-C at index 2, got %s", jobC.ID)
	}
	if jobC.UsedBytes != 0 || len(jobC.Operations) != 0 {
		t.Errorf("job-C should have no operations: %+v", jobC)
	}
	if jobC.Stmt == "" {
		t.Errorf("job-C statement lost despite LEFT JOIN nulls")
	}
}
