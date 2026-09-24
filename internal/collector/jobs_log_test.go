package collector

import (
	"strings"
	"testing"
	"time"

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

// Every jobs_log statement must skip obsi's own polling and stuck jobs, and
// take its bounds as parameters rather than inlined values.
func TestJobsLogQueriesFilter(t *testing.T) {
	for name, q := range map[string]string{
		"slowest": jobsLogSlowestQuery,
		"failed":  jobsLogFailedQuery,
		"grouped": jobsLogGroupedQuery,
	} {
		if !strings.Contains(q, "stmt NOT LIKE '%/* obsi */'") {
			t.Errorf("%s: missing obsi filter", name)
		}
		if strings.Count(q, "?") != 2 {
			t.Errorf("%s: want 2 placeholders, got %d", name, strings.Count(q, "?"))
		}
		if !strings.HasSuffix(q, "LIMIT 20") {
			t.Errorf("%s: want LIMIT %d", name, store.SlowestLimit)
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
