package tui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestClusterSwitchNotice(t *testing.T) {
	var cluster atomic.Value
	cluster.Store([2]string{"aaaa-1111", "prod"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Stmt string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		c := cluster.Load().([2]string)
		var rows [][]interface{}
		if strings.Contains(req.Stmt, "FROM sys.cluster") {
			rows = [][]interface{}{{c[0], c[1]}}
		}
		_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{Rows: rows})
	}))
	defer srv.Close()

	ctx := context.Background()
	cfg := config.DefaultConfig()
	reg := cratedb.NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
	if err := reg.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	st := store.New(10, cfg.Collectors)
	mgr := collector.NewManager(reg, st, collector.NewQueryTracker(cfg.Collectors, cfg.Connection))
	a := NewApp(st, reg, mgr, ctx, cfg.TUI, true)
	a.Update(tea.WindowSizeMsg{Width: 120, Height: 40})

	cluster.Store([2]string{"bbbb-2222", "staging"})
	reg.Reconnect(ctx)
	for i := 0; reg.Status().Foreign == nil; i++ {
		if i == 100 {
			t.Fatal("registry never noticed the other cluster")
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.Update(StoreTickMsg{})
	out := a.View()
	for _, want := range []string{"Different cluster behind the endpoint", "prod (aaaa-111)", "staging (bbbb-222)"} {
		if !strings.Contains(out, want) {
			t.Errorf("view lacks %q:\n%s", want, out)
		}
	}
	if a := st.Snapshot(1, store.SnapshotHint{}).Alerts; a.Firing != 1 ||
		a.History[0].Message != "different cluster behind the endpoint: staging (bbbb-222), was prod (aaaa-111)" {
		t.Errorf("alert: %+v", a.History)
	}

	a.Update(keyRune('5'))
	a.Update(keyRune('a'))
	if a.activeTab != TabOverview || a.showAlerts {
		t.Error("keys got past the notice")
	}

	a.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if a.foreign != nil || reg.Status().ClusterName != "staging" {
		t.Fatalf("enter did not switch: foreign=%v name=%s", a.foreign, reg.Status().ClusterName)
	}
	alerts := st.Snapshot(1, store.SnapshotHint{}).Alerts
	if alerts.Firing != 0 || alerts.History[0].Message != "switched to cluster staging (bbbb-222) (was prod (aaaa-111))" {
		t.Errorf("alerts after switch: %+v", alerts.History)
	}
}
