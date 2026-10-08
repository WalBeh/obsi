package tui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func delayTable(name string, own, min time.Duration, below int) cratedb.TableInfo {
	return cratedb.TableInfo{SchemaName: "doc", TableName: name,
		Settings: cratedb.TableSettings{NodeLeftDelay: own}, NodeLeftDelayMin: min, PartitionsBelowDelay: below}
}

func TestParseAndSuggestDelay(t *testing.T) {
	for in, want := range map[string]time.Duration{"5m": 5 * time.Minute, "300s": 5 * time.Minute, "1h": time.Hour, "2d": 48 * time.Hour, "500ms": 500 * time.Millisecond} {
		if got, ok := parseDelay(in); !ok || got != want {
			t.Errorf("parseDelay(%q) = %v %v", in, got, ok)
		}
	}
	for _, bad := range []string{"5", "5 m", "five", "-1m", ""} {
		if _, ok := parseDelay(bad); ok {
			t.Errorf("parseDelay(%q) accepted", bad)
		}
	}
	now := time.Now()
	if d := suggestDelay(nil); d != 5*time.Minute {
		t.Errorf("no absence: %v", d)
	}
	// 4m40s away: plus half is 7m, rounded up to a minute.
	if d := suggestDelay([]store.NodeAbsence{{Name: "n", Left: now.Add(-280 * time.Second), Back: now}}); d != 7*time.Minute {
		t.Errorf("suggest = %v, want 7m", d)
	}
}

func TestNodeLeftLine(t *testing.T) {
	now := time.Now()
	snap := store.StoreSnapshot{
		Tables: []cratedb.TableInfo{
			delayTable("a", time.Minute, time.Minute, 0),
			delayTable("b", time.Minute, time.Minute, 0),
			delayTable("parted", 5*time.Minute, time.Minute, 2), // ALTER TABLE ONLY left old partitions at 1m
			delayTable("c", 5*time.Minute, 5*time.Minute, 0),
		},
		BlobTables:   2,
		NodeAbsences: []store.NodeAbsence{{Name: "n2", Left: now.Add(-160 * time.Second), Back: now}},
	}
	m := NewOverviewModel(160, 60, true).Refresh(snap)
	line := m.renderNodeLeft()
	for _, want := range []string{
		"Node-left delay: 1m",
		"1m on 3 tables (1 via older partitions)",
		"5m on 1 table",
		"2 blob tables at 1m, fixed",
		"longest node away 2m40s (n2",
		"longer than the delay",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line lacks %q:\n%s", want, line)
		}
	}
	// 2m40s plus half is 4m, but at least 5m.
	if m.editor.slots[slotNodeLeftDelay].value != "1m" || !strings.Contains(m.editor.nodeLeftHint, "suggested 5m: longest node away 2m40s") {
		t.Errorf("slot %q hint %q", m.editor.slots[slotNodeLeftDelay].value, m.editor.nodeLeftHint)
	}
}

func TestNodeLeftEditAndConfirm(t *testing.T) {
	snap := store.StoreSnapshot{
		Tables: []cratedb.TableInfo{
			delayTable("a", time.Minute, time.Minute, 0),
			delayTable("parted", 5*time.Minute, time.Minute, 2),
			delayTable("slow", 10*time.Minute, 10*time.Minute, 0),
		},
		BlobTables: 1,
	}
	m := NewOverviewModel(160, 60, true).Refresh(snap)
	m.editor.active, m.editor.cursor = true, slotNodeLeftDelay
	m.editor.activateInput()
	m.editor.inputBuf = "five"
	m, cmd := m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd != nil || !strings.Contains(m.editor.errorMsg, "isn't a time value") {
		t.Fatalf("bad value: cmd=%v err=%q", cmd, m.editor.errorMsg)
	}

	m.editor.activateInput()
	m.editor.inputBuf = "5m"
	m, cmd = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	req, ok := cmd().(NodeLeftDelayRequest)
	if !ok || req.Value != "5m" {
		t.Fatalf("enter gave %#v", cmd())
	}
	m = m.openDelayConfirm(req.Value)
	view := m.View()
	for _, want := range []string{
		"Set unassigned.node_left.delayed_timeout = 5m on 3 tables?",
		"doc.parted: 2 partitions at 1m, overwritten",
		"doc.slow: lowered from 10m",
		"Skipped: 1 blob tables",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("confirm lacks %q:\n%s", want, view)
		}
	}
	m, cmd = m.HandleKey(keyRune('y'))
	apply, ok := cmd().(ApplyNodeLeftDelayMsg)
	if !ok || len(apply.Tables) != 3 || apply.Value != "5m" || apply.Blob != 1 || m.delayConfirm != nil {
		t.Errorf("apply = %+v", apply)
	}
}

// One plain ALTER TABLE per table (no ONLY), identifiers quoted, value as a
// parameter; stops at the first error and says how far it got.
func TestApplyNodeLeftDelay(t *testing.T) {
	var mu sync.Mutex
	var stmts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Stmt string
			Args []interface{}
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		stmts = append(stmts, req.Stmt)
		mu.Unlock()
		if strings.Contains(req.Stmt, `"broken"`) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"nope"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{})
	}))
	defer srv.Close()
	reg := cratedb.NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)

	res := applyNodeLeftDelay(context.Background(), reg, ApplyNodeLeftDelayMsg{Value: "5m", Blob: 2,
		Tables: [][2]string{{"doc", "a"}, {"My Schema", `we"ird`}}})
	if res.Error != "" || res.Note != "5m set on 2 tables; 2 blob tables keep 1m" {
		t.Errorf("result = %+v", res)
	}
	if !strings.HasPrefix(stmts[1], `ALTER TABLE "My Schema"."we""ird" SET ("unassigned.node_left.delayed_timeout" = ?)`) || strings.Contains(stmts[0], "ONLY") {
		t.Errorf("statements = %q", stmts)
	}

	res = applyNodeLeftDelay(context.Background(), reg, ApplyNodeLeftDelayMsg{Value: "5m",
		Tables: [][2]string{{"doc", "a"}, {"doc", "broken"}, {"doc", "c"}}})
	if !strings.HasPrefix(res.Error, "set on 1 of 3 tables, then doc.broken:") {
		t.Errorf("error = %q", res.Error)
	}
}

// While a node is away, a replica being rebuilt means the delay ran out.
func TestShardsRebuiltWhileAway(t *testing.T) {
	s := cratedb.ShardInfo{SchemaName: "doc", TableName: "t", ID: 0, RoutingState: "INITIALIZING", NodeName: "lab3"}
	fixes := diagnoseShard(s, cratedb.AllocationInfo{}, cratedb.ClusterSettings{}, "1", []string{"lab2"})
	if len(fixes) != 1 || fixes[0].key != "rebuilt" || !strings.Contains(fixes[0].cause, "while lab2 is away") {
		t.Errorf("fixes = %+v", fixes)
	}
	if f := diagnoseShard(s, cratedb.AllocationInfo{}, cratedb.ClusterSettings{}, "1", nil); len(f) != 0 {
		t.Errorf("no node away: %+v", f)
	}
}
