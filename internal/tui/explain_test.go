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

func TestParseValue(t *testing.T) {
	for in, want := range map[string]interface{}{"123": float64(123), `"a b"`: "a b", "null": nil, "true": true, "plain text": "plain text", "2026-10-08": "2026-10-08"} {
		if got := parseValue(in); got != want {
			t.Errorf("parseValue(%q) = %#v", in, got)
		}
	}
	if got, ok := parseValue("[1, 2]").([]interface{}); !ok || len(got) != 2 {
		t.Errorf("array = %#v", got)
	}
}

// Shape from CrateDB 6.4.5 on the shardlab.
const analyzeJSON = `{"Execute":{"n1":{"QueryBreakdown":[{"ShardId":0,"TableName":"big","QueryDescription":"1:[1001 TO 9223372036854775807]","Time":35.58,"BreakDown":{"next_doc_count":265905.0},"SchemaName":"doc","QueryName":"PointRangeQuery"}]},
"Phases":{"0-collect":{"nodes":{"n1":27.9,"n2":99.3}},"1-merge":{"nodes":{"n1":262.6}}},"Total":278.1},"Plan":0.32,"Analyze":0.34}`

func TestParseAnalyze(t *testing.T) {
	var raw interface{}
	_ = json.Unmarshal([]byte(analyzeJSON), &raw)
	r := parseAnalyze(raw, map[string]string{"n1": "data-hot-0", "n2": "data-hot-1"})
	if r.total != 278.1 || len(r.phases) != 2 || r.phases[0].nodes[0].node != "data-hot-1" || r.phases[1].max != 262.6 {
		t.Errorf("phases = %+v", r.phases)
	}
	if len(r.breakdown) != 1 || r.breakdown[0].node != "data-hot-0" || r.breakdown[0].docs != 265905 {
		t.Errorf("breakdown = %+v", r.breakdown)
	}
	if strings.Contains(r.pretty, `"n1"`) || !strings.Contains(r.pretty, `"data-hot-0"`) {
		t.Errorf("pretty keeps node ids:\n%s", r.pretty)
	}
}

func TestExplainFlow(t *testing.T) {
	stmt := "SELECT count(*) FROM doc.big WHERE id > ? AND pad = $2;"
	snap := store.StoreSnapshot{ActiveQueries: []cratedb.ActiveQuery{{ID: "j1", Stmt: stmt, Started: time.Now().Add(-9 * time.Second)}}}
	m := NewQueriesModel(160, 50).Refresh(snap)
	m.explainTimeout = 2 * time.Minute

	m, cmd := m.HandleKey(keyRune('E'))
	if req, ok := cmd().(ExplainPlanMsg); !ok || req.Stmt != "SELECT count(*) FROM doc.big WHERE id > ? AND pad = $2" {
		t.Fatalf("E gave %#v", cmd())
	}
	m = m.setExplainPlan(ExplainPlanResultMsg{Plan: "Count[doc.big | ((id > $1) AND (pad = $2))]"})
	if !strings.Contains(m.View(), "[a] EXPLAIN ANALYZE (runs the query again)") {
		t.Fatalf("plan view:\n%s", m.View())
	}

	m, _ = m.HandleKey(keyRune('a'))
	view := m.View()
	for _, want := range []string{"last run 9.0s", "obsi stops it after 2m0s", "$1", "id > ?", "$2", "pad = ?"} {
		if !strings.Contains(view, want) {
			t.Errorf("form lacks %q:\n%s", want, view)
		}
	}
	for _, r := range "100" {
		m, _ = m.HandleKey(keyRune(r))
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyTab})
	for _, r := range "a b" {
		if r == ' ' {
			m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeySpace, Runes: []rune{' '}})
			continue
		}
		m, _ = m.HandleKey(keyRune(r))
	}
	m, cmd = m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	run, ok := cmd().(ExplainAnalyzeMsg)
	if !ok || len(run.Args) != 2 || run.Args[0] != float64(100) || run.Args[1] != "a b" {
		t.Fatalf("run = %#v", cmd())
	}

	var raw interface{}
	_ = json.Unmarshal([]byte(analyzeJSON), &raw)
	m = m.setExplainResult(ExplainAnalyzeResultMsg{Raw: raw}, map[string]string{"n1": "data-hot-0"})
	if v := m.View(); !strings.Contains(v, "total 278.1ms   plan 0.3ms") || !strings.Contains(v, "← slowest phase") {
		t.Errorf("result view:\n%s", v)
	}
	text := m.explain.copyText()
	for _, want := range []string{stmt[:len(stmt)-1] + ";", "-- $1 = 100", `-- $2 = "a b"`, "-- EXPLAIN\nCount[", "-- EXPLAIN ANALYZE", `"data-hot-0"`} {
		if !strings.Contains(text, want) {
			t.Errorf("copy lacks %q:\n%s", want, text)
		}
	}

	// UPDATE can't be explained; no request goes out.
	m.explain = nil
	m.snap.ActiveQueries[0].Stmt = "UPDATE t SET a = 1"
	m, cmd = m.HandleKey(keyRune('E'))
	if cmd != nil || !strings.Contains(m.View(), "CrateDB only explains queries") {
		t.Errorf("update: cmd=%v view:\n%s", cmd, m.View())
	}
}

// A run that outlasts its context is killed on the server, found by its tag.
func TestExplainAnalyzeKill(t *testing.T) {
	var mu sync.Mutex
	var killed []string
	var tagged string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Stmt string
			Args []interface{}
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch {
		case strings.HasPrefix(req.Stmt, "EXPLAIN ANALYZE"):
			mu.Lock()
			tagged = req.Stmt
			mu.Unlock()
			<-r.Context().Done()
			return
		case strings.Contains(req.Stmt, "FROM sys.jobs"):
			pat := req.Args[0].(string)
			mu.Lock()
			ok := strings.Contains(tagged, strings.Trim(pat, "%"))
			mu.Unlock()
			rows := [][]interface{}{}
			if ok {
				rows = append(rows, []interface{}{"job-1"})
			}
			_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{Rows: rows})
		case strings.HasPrefix(req.Stmt, "KILL"):
			mu.Lock()
			killed = append(killed, req.Args[0].(string))
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{})
		}
	}))
	defer srv.Close()
	reg := cratedb.NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	res := runExplainAnalyze(ctx, reg, ExplainAnalyzeMsg{Stmt: "SELECT 1"})
	if !res.Killed || res.Err != "timed out; killed the job on the cluster" || len(killed) != 1 || killed[0] != "job-1" {
		t.Errorf("res=%+v killed=%v", res, killed)
	}
	if !strings.Contains(tagged, "/* obsi explain ") {
		t.Errorf("statement not tagged: %q", tagged)
	}
}

// KILL only returns once the job has stopped, which can take a minute; obsi
// doesn't wait for it and says so.
func TestExplainAnalyzeKillPending(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Stmt string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch {
		case strings.HasPrefix(req.Stmt, "EXPLAIN ANALYZE"), strings.HasPrefix(req.Stmt, "KILL"):
			<-r.Context().Done()
		default:
			_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{Rows: [][]interface{}{{"job-1"}}})
		}
	}))
	defer srv.Close()
	reg := cratedb.NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // esc
	res := runExplainAnalyze(ctx, reg, ExplainAnalyzeMsg{Stmt: "SELECT 1"})
	if res.Err != "stopped; KILL sent, CrateDB is still stopping the job (see the Queries tab)" {
		t.Errorf("err = %q", res.Err)
	}
}
