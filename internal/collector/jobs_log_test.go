package collector

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestParseJobLogEntries(t *testing.T) {
	rows := [][]interface{}{
		{"id-1", "data-0", "app", "SELECT 1", float64(1790232523000), float64(1790232528381), nil, "SELECT"},
		{"id-2", "data-1", "bob", "SELECT x", float64(1790232500000), float64(1790232501000), "SQLParseException[...]", "SELECT"},
	}
	got := parseJobLogEntries(rows)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if d := got[0].Ended.Sub(got[0].Started); d != 5381*time.Millisecond {
		t.Errorf("duration = %s, want 5.381s", d)
	}
	if got[0].Error != "" || got[1].Error == "" {
		t.Errorf("errors = %q / %q, want empty / set", got[0].Error, got[1].Error)
	}
}

func TestParseJobLogGroups(t *testing.T) {
	rows := [][]interface{}{{"SELECT * FROM t WHERE id = ?", float64(7), float64(1), float64(12016), float64(3250.5), float64(1790232555166)}}
	got := parseJobLogGroups(rows)
	g := got[0]
	if g.Count != 7 || g.Failed != 1 || g.Max != 12016*time.Millisecond {
		t.Errorf("group = %+v", g)
	}
	if g.Avg != 3250500*time.Microsecond {
		t.Errorf("Avg = %s, want 3.2505s", g.Avg)
	}
}

// Every jobs_log statement must skip stuck jobs, take its bounds as
// parameters rather than inlined values, and fetch enough rows for dropOwn.
func TestJobsLogQueriesFilter(t *testing.T) {
	for name, q := range map[string]string{
		"slowest": jobsLogSlowestQuery,
		"failed":  jobsLogFailedQuery,
		"grouped": jobsLogGroupedQuery,
	} {
		if strings.Contains(q, "NOT LIKE") {
			t.Errorf("%s: obsi filter belongs in dropOwn", name)
		}
		if strings.Count(q, "?") != 2 {
			t.Errorf("%s: want 2 placeholders, got %d", name, strings.Count(q, "?"))
		}
		if !strings.HasSuffix(q, "LIMIT 40") {
			t.Errorf("%s: want LIMIT %d", name, 2*store.SlowestLimit)
		}
	}
}

func TestJobsLogSetView(t *testing.T) {
	c := &JobsLogCollector{}
	if !c.SetView(true, store.JobsLogSlowest) {
		t.Error("activation must report a change")
	}
	if c.SetView(true, store.JobsLogSlowest) {
		t.Error("same view must not report a change")
	}
	if !c.SetView(true, store.JobsLogGrouped) {
		t.Error("mode switch must report a change")
	}
}

func TestDropOwn(t *testing.T) {
	var rows []cratedb.JobLogEntry
	for i := range 40 {
		stmt := fmt.Sprintf("SELECT %d", i)
		if i%3 == 0 {
			stmt += cratedb.QueryTag
		}
		rows = append(rows, cratedb.JobLogEntry{Stmt: stmt})
	}
	got := dropOwn(rows, func(e cratedb.JobLogEntry) string { return e.Stmt })
	if len(got) != store.SlowestLimit {
		t.Fatalf("len = %d, want %d", len(got), store.SlowestLimit)
	}
	for _, e := range got {
		if strings.Contains(e.Stmt, "obsi") {
			t.Errorf("own statement kept: %q", e.Stmt)
		}
	}
	if got[0].Stmt != "SELECT 1" || got[19].Stmt != "SELECT 29" {
		t.Errorf("order changed: first %q last %q", got[0].Stmt, got[19].Stmt)
	}
}
