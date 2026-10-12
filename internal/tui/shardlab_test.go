//go:build shardlab

package tui

import (
	"context"
	"fmt"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// TestShardlab prints the Shards tab for the local shardlab cluster, see
// docs/shardlab.md. go test -tags shardlab -run Shardlab -v ./internal/tui
func TestShardlab(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	ctx := context.Background()
	reg := cratedb.NewRegistry("http://127.0.0.1:44200", "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	st := store.New(10, cfg.Collectors)
	tracker := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
	for _, c := range []collector.Collector{
		collector.NewClusterCollector(cfg.Collectors["cluster"], tracker),
		collector.NewNodesCollector(cfg.Collectors["nodes"], tracker),
	} {
		if err := c.Collect(ctx, reg, st); err != nil {
			t.Fatal(err)
		}
	}
	shards := collector.NewShardsCollector(cfg.Collectors["shards"], tracker)
	if err := shards.Collect(ctx, reg, st); err != nil {
		t.Fatal(err)
	}
	if err := shards.CollectFastPath(ctx, reg, st); err != nil { // stuck check
		t.Fatal(err)
	}
	snap := st.Snapshot(1, store.SnapshotHint{IncludeShards: true, IncludeCluster: true, IncludeNodes: true})
	fmt.Println(NewShardsModel(160, 50).Refresh(snap).View())
}

// TestChangeslab runs a few config changes on the shardlab and prints the
// changes board. go test -tags shardlab -run Changeslab -v ./internal/tui
func TestChangeslab(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	ctx := context.Background()
	reg := cratedb.NewRegistry("http://127.0.0.1:44200", "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	st := store.New(10, cfg.Collectors)
	tracker := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
	changes := collector.NewChangesCollector(cfg.Collectors["changes"], tracker)
	run := func(stmt string, args ...interface{}) {
		if _, err := reg.Query(ctx, stmt, args...); err != nil {
			t.Logf("%s: %v", stmt, err)
		}
	}
	run(`CREATE TABLE IF NOT EXISTS doc.changelab (a INT) WITH (number_of_replicas = 0)`)
	if err := changes.Collect(ctx, reg, st); err != nil {
		t.Fatal(err)
	}
	run(`SET GLOBAL TRANSIENT "indices.recovery.max_bytes_per_sec" = ?`+cratedb.ChangeTag, "123mb")
	run(`SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 'primaries'`)
	run(`SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 7`)
	run(`SET search_path TO doc`)
	run(`ALTER TABLE doc.changelab SET (number_of_replicas = 1, refresh_interval = 5000)`)
	run(`CREATE USER changelab_u WITH (password = 'not-shown')`)
	run(`DROP USER changelab_u`)
	run(`CREATE PUBLICATION changelab_p FOR TABLE doc.changelab`)
	run(`CREATE SUBSCRIPTION changelab_s CONNECTION 'crate://changelab-nohost:4300?user=u&password=not-shown' PUBLICATION changelab_p`)
	run(`DROP PUBLICATION changelab_p`)
	time.Sleep(500 * time.Millisecond)
	if err := changes.Collect(ctx, reg, st); err != nil {
		t.Fatal(err)
	}
	run(`RESET GLOBAL "indices.recovery.max_bytes_per_sec", "cluster.routing.allocation.enable"`)
	time.Sleep(500 * time.Millisecond)
	for range 2 { // the second finds nothing new; no unseen diffs either
		if err := changes.Collect(ctx, reg, st); err != nil {
			t.Fatal(err)
		}
	}
	run(`DROP TABLE doc.changelab`)
	snap := st.Snapshot(1, store.SnapshotHint{IncludeQueries: true})
	m := NewQueriesModel(170, 40).Refresh(snap)
	for _, r := range "cjjj" { // down to the ALTER TABLE
		m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	fmt.Println(m.View())
	for _, s := range tracker.Snapshot() {
		if s.Category == "changes" {
			fmt.Printf("%s: %d runs, last %s, %d rows\n", s.Label, s.ExecCount, s.LastDur, s.LastRows)
		}
	}
}

// TestReplicationlab prints the Replication tab for the publisher (44200)
// and the subscriber (44210) after ./scenarios.sh replication.
// go test -tags shardlab -run Replicationlab -v ./internal/tui
func TestReplicationlab(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	ctx := context.Background()
	cfg := config.DefaultConfig()
	type side struct {
		reg *cratedb.Registry
		st  *store.Store
		c   *collector.ReplicationCollector
		tr  *collector.QueryTracker
	}
	var sides []side
	for _, url := range []string{"http://127.0.0.1:44200", "http://127.0.0.1:44210"} {
		reg := cratedb.NewRegistry(url, "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
		if err := reg.Bootstrap(ctx); err != nil {
			t.Logf("%s: %v", url, err) // sub-down
			continue
		}
		tr := collector.NewQueryTracker(cfg.Collectors, cfg.Connection)
		sides = append(sides, side{reg, store.New(10, cfg.Collectors), collector.NewReplicationCollector(cfg.Collectors["replication"], tr), tr})
	}
	collect := func() {
		for _, s := range sides {
			if err := s.c.Collect(ctx, s.reg, s.st); err != nil {
				t.Fatal(err)
			}
		}
	}
	collect()
	if _, err := sides[0].reg.Query(ctx, `INSERT INTO doc.repl SELECT g, md5(g::TEXT) FROM generate_series(1, 2000) g`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	collect()
	for _, s := range sides {
		snap := s.st.Snapshot(1, store.SnapshotHint{})
		fmt.Printf("==== %s\n%s\n", s.reg.Status().ClusterName, NewReplicationModel(120, 40).Refresh(snap).View())
		for _, a := range snap.Alerts.History {
			fmt.Println("alert:", a.Message)
		}
		for _, q := range s.tr.Snapshot() {
			if q.Category == "replication" && q.ExecCount > 0 {
				fmt.Printf("%s: %d runs, last %s, %d rows\n", q.Label, q.ExecCount, q.LastDur, q.LastRows)
			}
		}
	}
}

// TestPublishlab runs the publisher setup's checks and statements on the
// shardlab after ./scenarios.sh replication, and prints the ticket.
// go test -tags shardlab -run Publishlab -v ./internal/tui
func TestPublishlab(t *testing.T) {
	ctx := context.Background()
	reg := cratedb.NewRegistry("http://127.0.0.1:44200", "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	checks := runPublicationChecks(ctx, reg, PublicationChecksRequest{Tables: []string{"doc.repl", "doc.repl_p"}})
	fmt.Printf("checks: %+v\n", checks)

	m := NewReplicationModel(120, 40)
	m.repl.Publications = []cratedb.Publication{{Name: "lab_pub", Tables: []string{"doc.repl", "doc.repl_p", "doc.repl_x"}}}
	m.tables = []string{"doc.big", "doc.repl", "doc.repl_p"}
	f := m.newPubForm(nil)
	f.name, f.checked["doc.repl"], f.checked["doc.big"] = "lab_pub2", true, true
	fmt.Println("create:", f.stmts())
	fmt.Printf("%+v\n", applyPublication(ctx, reg, ApplyPublicationMsg{Stmts: f.stmts(), Done: "created"}))
	pub := cratedb.Publication{Name: "lab_pub2", Tables: []string{"doc.big", "doc.repl"}}
	m.repl.Publications = append(m.repl.Publications, pub)
	e := m.newPubForm(&pub)
	e.checked["doc.big"], e.checked["doc.repl_p"] = false, true
	fmt.Println("edit:", e.stmts())
	fmt.Printf("%+v\n", applyPublication(ctx, reg, ApplyPublicationMsg{Stmts: e.stmts(), Done: "changed"}))
	resp, err := reg.Query(ctx, `SELECT pubname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = 'lab_pub2' ORDER BY 2`)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("lab_pub2 now:", resp.Rows)
	text, err := ticket(ctx, reg, cratedb.Publication{Name: "lab_pub2", Tables: []string{"doc.repl", "doc.repl_p"}})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Print(text)
	fmt.Printf("%+v\n", applyPublication(ctx, reg, ApplyPublicationMsg{Stmts: []string{`DROP PUBLICATION "lab_pub2"`}, Done: "dropped"}))
}

// TestSubscribelab runs the subscriber setup end to end: a publication on
// the shardlab, its ticket pasted into the form on the subscriber (44210),
// CREATE and DROP SUBSCRIPTION. Needs the replication profile.
// go test -tags shardlab -run Subscribelab -v ./internal/tui
func TestSubscribelab(t *testing.T) {
	ctx := context.Background()
	open := func(url string) *cratedb.Registry {
		reg := cratedb.NewRegistry(url, "crate", "", 3*time.Second, 10*time.Second, time.Hour, time.Hour, false)
		if err := reg.Bootstrap(ctx); err != nil {
			t.Fatal(err)
		}
		return reg
	}
	pub, sub := open("http://127.0.0.1:44200"), open("http://127.0.0.1:44210")
	q := func(reg *cratedb.Registry, stmt string) [][]interface{} {
		resp, err := reg.Query(ctx, stmt)
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		return resp.Rows
	}
	q(pub, `CREATE TABLE doc.sublab (id INT) WITH (number_of_replicas = 0)`)
	q(pub, `INSERT INTO doc.sublab SELECT g FROM generate_series(1, 100) g`)
	defer func() {
		sub.Query(ctx, `DROP SUBSCRIPTION IF EXISTS sublab_pub`)
		sub.Query(ctx, `DROP TABLE IF EXISTS doc.sublab`)
		pub.Query(ctx, `DROP PUBLICATION IF EXISTS sublab_pub`)
		pub.Query(ctx, `DROP TABLE IF EXISTS doc.sublab`)
	}()
	fmt.Printf("%+v\n", applyPublication(ctx, pub, ApplyPublicationMsg{Stmts: []string{`CREATE PUBLICATION "sublab_pub" FOR TABLE "doc"."sublab"`}}))
	text, err := ticket(ctx, pub, cratedb.Publication{Name: "sublab_pub", Tables: []string{"doc.sublab"}})
	if err != nil {
		t.Fatal(err)
	}

	m := NewReplicationModel(120, 40).Refresh(store.StoreSnapshot{Replication: store.ReplicationState{UpdatedAt: time.Now()}})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")})
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(text), Paste: true})
	f := m.subForm
	f.vals[subHost], f.vals[subUser], f.vals[subPassword] = "lab1", "crate", "not-in-the-log&1"
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	m, cmd := m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	apply := cmd().(ApplySubscriptionMsg)
	fmt.Println("run:", apply.Stmt)
	fmt.Printf("%+v\n", applySubscription(ctx, sub, apply))
	for i := 0; i < 30; i++ {
		rows := q(sub, `SELECT r.srsubstate FROM pg_catalog.pg_subscription_rel r WHERE r.srrelid::text = 'doc.sublab'`)
		if len(rows) > 0 && rows[0][0] == "r" {
			fmt.Printf("in sync after %ds\n", i)
			break
		}
		time.Sleep(time.Second)
	}
	q(sub, `REFRESH TABLE doc.sublab`)
	fmt.Println("rows on the subscriber:", q(sub, `SELECT count(*) FROM doc.sublab`))
	fmt.Println("jobs_log:", q(sub, `SELECT stmt FROM sys.jobs_log WHERE stmt LIKE 'CREATE SUBSCRIPTION%sublab%'`))
	fmt.Println("password anywhere in jobs_log:", q(sub, `SELECT count(*) FROM sys.jobs_log WHERE stmt LIKE '%not-in-the-log%' AND stmt NOT LIKE '%sys.jobs_log%'`))
	leases := `SELECT count(*) FROM sys.shards WHERE table_name = 'sublab' AND "primary" AND array_to_string(retention_leases['leases']['source'], ',') LIKE '%logical_replication:%'`
	fmt.Println("publisher shards with a subscriber lease:", q(pub, leases))

	d := &subDrop{name: "sublab_pub"}
	for _, r := range "sublab_pub" {
		d.key(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	_, cmd = d.key(tea.KeyMsg{Type: tea.KeyEnter})
	fmt.Printf("%+v\n", applySubscription(ctx, sub, cmd().(ApplySubscriptionMsg)))
	if _, err := sub.Query(ctx, `INSERT INTO doc.sublab VALUES (-1)`); err != nil {
		t.Errorf("not writable after the drop: %v", err)
	} else {
		fmt.Println("writable after the drop")
	}
	for i := 0; i < 60; i++ {
		if n := q(pub, leases)[0][0]; cratedb.ToInt64(n) == 0 {
			fmt.Printf("publisher lease gone after %ds\n", i)
			return
		}
		time.Sleep(time.Second)
	}
	fmt.Println("publisher lease still there after 60s")
}
