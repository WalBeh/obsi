package collector

import (
	"reflect"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestParseChange(t *testing.T) {
	ended := float64(1790232600000)
	if ch := parseChange([]interface{}{"j0", "n1", "crate", "SET extra_float_digits = 3", ended, nil}); ch != nil {
		t.Errorf("session SET became a change: %+v", ch)
	}
	ch := parseChange([]interface{}{"j1", "n1", "crate", "CREATE USER u WITH (password = 'hunter2')", ended, nil})
	if ch == nil || ch.Kind != cratedb.ChangeSecurity || ch.Stmt != "CREATE USER u WITH (password = '***')" {
		t.Fatalf("got %+v", ch)
	}
	ch = parseChange([]interface{}{"j2", "n1", "crate", `SET GLOBAL TRANSIENT "a.b" = ?` + cratedb.ChangeTag, ended, "boom"})
	if ch == nil || !ch.Obsi || ch.Stmt != `SET GLOBAL TRANSIENT "a.b" = ?` || ch.Error != "boom" {
		t.Errorf("got %+v", ch)
	}
}

func TestFlattenAndDiffSettings(t *testing.T) {
	old := map[string]string{}
	flattenSettings("", map[string]interface{}{
		"cluster": map[string]interface{}{
			"routing": map[string]interface{}{
				"allocation": map[string]interface{}{"enable": "all", "exclude": map[string]interface{}{"_name": "n1"}},
			},
		},
		"indices": map[string]interface{}{"recovery": map[string]interface{}{"max_bytes_per_sec": "40mb"}},
		"stats":   map[string]interface{}{"enabled": true, "jobs_log_size": float64(10000)},
	}, old)
	cur := map[string]string{}
	flattenSettings("", map[string]interface{}{
		"cluster": map[string]interface{}{
			"routing": map[string]interface{}{
				"allocation": map[string]interface{}{"enable": "primaries", "exclude": map[string]interface{}{}},
			},
		},
		"indices": map[string]interface{}{"recovery": map[string]interface{}{"max_bytes_per_sec": "40mb"}},
		"stats":   map[string]interface{}{"enabled": true, "jobs_log_size": float64(10000)},
	}, cur)
	if cur["stats.jobs_log_size"] != "10000" {
		t.Errorf("number flattened to %q", cur["stats.jobs_log_size"])
	}
	want := []store.SettingDiff{
		{Key: "cluster.routing.allocation.enable", Old: "all", New: "primaries"},
		{Key: "cluster.routing.allocation.exclude._name", Old: "n1"},
	}
	if got := diffSettings(old, cur); !reflect.DeepEqual(got, want) {
		t.Errorf("diff = %+v", got)
	}
}

func TestPairClusterDiffs(t *testing.T) {
	t0 := time.Unix(1790232600, 0)
	c := NewChangesCollector(defaultChangesConfig(), nil)
	set := &store.Change{ID: "j1", Kind: cratedb.ChangeCluster, Ended: t0, Stmt: `RESET GLOBAL "indices.recovery"`}
	failed := &store.Change{ID: "j2", Kind: cratedb.ChangeCluster, Ended: t0.Add(time.Second), Stmt: `RESET GLOBAL "indices.recovery"`, Error: "x"}
	diffs := []store.SettingDiff{
		{Key: "indices.recovery.max_bytes_per_sec", Old: "1mb", New: "40mb"},
		{Key: "cluster.routing.allocation.enable", Old: "all", New: "none"},
	}
	out := c.pairClusterDiffs([]*store.Change{set, failed}, diffs, t0)
	if len(out) != 2 || len(set.Diffs) != 1 || set.Diffs[0].Key != "indices.recovery.max_bytes_per_sec" || len(failed.Diffs) != 0 {
		t.Fatalf("first poll: out=%d set=%+v failed=%+v", len(out), set.Diffs, failed.Diffs)
	}
	if len(c.unpaired) != 1 {
		t.Fatalf("allocation diff should wait a poll, unpaired=%+v", c.unpaired)
	}

	// Statement shows up one poll late: paired, not reported unseen.
	late := &store.Change{ID: "j3", Kind: cratedb.ChangeCluster, Ended: t0, Stmt: `SET GLOBAL "cluster.routing.allocation.enable" = 'none'`}
	out = c.pairClusterDiffs([]*store.Change{late}, nil, t0.Add(10*time.Second))
	if len(out) != 1 || len(late.Diffs) != 1 || len(c.unpaired) != 0 {
		t.Fatalf("late statement: out=%d diffs=%+v unpaired=%+v", len(out), late.Diffs, c.unpaired)
	}

	// No statement within a poll: unseen.
	c.pairClusterDiffs(nil, diffs[1:], t0.Add(20*time.Second))
	out = c.pairClusterDiffs(nil, nil, t0.Add(30*time.Second))
	if len(out) != 1 || !out[0].Unseen || !out[0].Ended.Equal(t0.Add(20*time.Second)) {
		t.Errorf("unseen = %+v", out)
	}
}

func TestChangesCoverageGaps(t *testing.T) {
	ms := func(s int64) float64 { return float64(1790232600000 + s*1000) }
	c := NewChangesCollector(defaultChangesConfig(), nil)
	gaps, start, floor := c.coverage([][]interface{}{{"n1", ms(0), ms(100)}, {"n2", ms(50), ms(90)}})
	if len(gaps) != 0 || start.UnixMilli() != int64(ms(50)) || floor.UnixMilli() != int64(ms(90)) {
		t.Fatalf("first poll: gaps=%v start=%v floor=%v", gaps, start, floor)
	}
	// n1 rotated past its previous newest entry, n2 didn't.
	gaps, _, _ = c.coverage([][]interface{}{{"n1", ms(120), ms(200)}, {"n2", ms(60), ms(190)}})
	if len(gaps) != 1 || gaps[0].Node != "n1" || gaps[0].From.UnixMilli() != int64(ms(100)) || gaps[0].To.UnixMilli() != int64(ms(120)) {
		t.Errorf("gaps = %+v", gaps)
	}
}

func defaultChangesConfig() config.CollectorConfig {
	return config.DefaultConfig().Collectors["changes"]
}
