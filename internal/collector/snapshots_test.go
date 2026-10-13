package collector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func jsonRows(t *testing.T, s string) [][]interface{} {
	t.Helper()
	var rows [][]interface{}
	if err := json.Unmarshal([]byte(s), &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

// Rows as CrateDB 6.4.5 returns them (lab, profile backup).
func TestParseSnapshots(t *testing.T) {
	rows := jsonRows(t, `[
["lab_s3","one_day","SUCCESS",1791885149584,1791885149985,"6.4.5",null,2,false,["doc.parted"],["doc"],["parted"],[["d1"]],[]],
["lab_slow","running","IN_PROGRESS",1791885156849,0,"6.4.5",null,10,true,[],[],[],[],[]],
["lab_fs","gone",  "FAILED",null,null,null,"can't read snapshot info",0,false,[],null,null,null,[]],
["lab_fs","nightly1","PARTIAL",1791885148580,1791885149384,"6.4.5",null,10,true,["doc.t1","doc.parted"],["doc","doc"],["parted","parted"],[["d2"],["d1"]],
 ["SnapshotShardFailure{shardId=[t1/W][3], reason='node shutdown', nodeId='n2'}"]]]`)
	got := parseSnapshots(rows)
	if names := []string{got[0].Name, got[1].Name, got[2].Name, got[3].Name}; !reflect.DeepEqual(names, []string{"running", "one_day", "nightly1", "gone"}) {
		t.Fatalf("order = %v, want newest first, unreadable last", names)
	}
	if !got[0].Finished.IsZero() || got[0].TotalShards != 10 || !got[0].GlobalState {
		t.Errorf("running = %+v", got[0])
	}
	if p := got[1].Partitions; len(p) != 1 || p[0].Table != "doc.parted" || !reflect.DeepEqual(p[0].Values, []string{"d1"}) {
		t.Errorf("partitions = %+v", p)
	}
	n := got[2]
	if n.State != "PARTIAL" || len(n.Failures) != 1 || n.Finished.Sub(n.Started) != 804*time.Millisecond || len(n.Partitions) != 2 {
		t.Errorf("partial = %+v", n)
	}
	if g := got[3]; !g.Started.IsZero() || g.Reason == "" || g.Partitions != nil {
		t.Errorf("unreadable = %+v", g)
	}
}

func TestParseRepositories(t *testing.T) {
	got := parseRepositories(jsonRows(t, `[
["lab_s3","s3",{"bucket":"lab-bucket","use_path_style_access":"true","secret_key":"[xxxxx]","access_key":"[xxxxx]"}],
["lab_fs","fs",{"compress":"true","location":"/repo/lab_fs","readonly":"true"}]]`))
	if got[0].Name != "lab_fs" || !got[0].ReadOnly() || got[1].ReadOnly() || got[1].Settings["secret_key"] != "[xxxxx]" {
		t.Errorf("repositories = %+v", got)
	}
}

func TestParseRestores(t *testing.T) {
	got := parseRestores(jsonRows(t, `[["lab_fs","nightly1","STARTED",["SUCCESS","STARTED","FAILURE","INIT"]]]`))
	want := []cratedb.SnapshotRestore{{Repository: "lab_fs", Name: "nightly1", State: "STARTED", Shards: 4, Done: 1, Failed: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("restores = %+v", got)
	}
}

// The listing is re-read when a repository changes, not when only the
// order of its settings does.
func TestRepositoriesKey(t *testing.T) {
	a := []cratedb.Repository{{Name: "r", Type: "fs", Settings: map[string]string{"location": "/a", "compress": "true"}}}
	b := []cratedb.Repository{{Name: "r", Type: "fs", Settings: map[string]string{"compress": "true", "location": "/a"}}}
	c := []cratedb.Repository{{Name: "r", Type: "fs", Settings: map[string]string{"compress": "true", "location": "/a", "readonly": "true"}}}
	if repositoriesKey(a) != repositoriesKey(b) || repositoriesKey(a) == repositoriesKey(c) || repositoriesKey(a) == repositoriesKey(nil) {
		t.Error("repositoriesKey doesn't follow the repositories")
	}
}

// sys.snapshots reads every repository, so it's listed only when due.
func TestSnapshotsCollectorListsWhenDue(t *testing.T) {
	var lists atomic.Int32
	var location, state atomic.Value
	location.Store("/a")
	state.Store("SUCCESS")
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Stmt string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		var rows [][]interface{}
		switch {
		case strings.Contains(req.Stmt, "FROM sys.repositories"):
			rows = [][]interface{}{{"r", "fs", map[string]interface{}{"location": location.Load()}}}
		case strings.Contains(req.Stmt, "FROM sys.snapshot_restore"):
		case strings.Contains(req.Stmt, "FROM sys.snapshots"):
			lists.Add(1)
			if fail.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": map[string]interface{}{"message": "timeout", "code": 5000}})
				return
			}
			rows = [][]interface{}{{"r", "s", state.Load(), 1.0, 2.0, "6.4.5", nil, 1.0, false, nil, nil, nil, nil, nil}}
		}
		_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{Rows: rows})
	}))
	t.Cleanup(srv.Close)
	cfg := config.DefaultConfig()
	reg := cratedb.NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
	st := store.New(10, cfg.Collectors)
	c := NewSnapshotsCollector(cfg.Collectors["snapshots"], NewQueryTracker(cfg.Collectors, cfg.Connection))
	step := func(what string, want int32) {
		t.Helper()
		before := lists.Load()
		_ = c.Collect(context.Background(), reg, st)
		if got := lists.Load() - before; got != want {
			t.Errorf("%s: listed %d times, want %d", what, got, want)
		}
	}
	step("first poll", 1)
	step("nothing changed", 0)
	location.Store("/b")
	step("repository changed", 1)
	c.Force()
	step("forced", 1)
	state.Store("IN_PROGRESS")
	c.Force()
	step("saw it running", 1)
	step("while running", 1)
	state.Store("SUCCESS")
	step("finished", 1)
	step("then quiet", 0)
	// 15m in 1m ticks; the throttle stretches the ticks, so the listing.
	for range 13 {
		step("within the interval", 0)
	}
	step("interval reached", 1)
	fail.Store(true)
	c.Force()
	step("failing", 1)
	step("after a failure", 0)
	c.Reset()
	step("after Reset", 1)
	if st.Snapshot(1, store.SnapshotHint{IncludeCluster: true}).Snapshots.ListErr == "" {
		t.Error("failed listing not recorded")
	}
}
