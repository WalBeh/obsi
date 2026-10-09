package tui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func TestSuggestionAdd(t *testing.T) {
	var s suggestion
	s.add("123", "text")         // digits in a text column stay text
	s.add("abc", "text")         //
	s.add(float64(42), "bigint") //
	s.add("1790812800000", "timestamp with time zone")
	s.add("abc", "text") // duplicate
	if strings.Join(s.insert, "|") != `"123"|abc|42|1790812800000` {
		t.Errorf("insert = %q", s.insert)
	}
	if s.display[3] != "2026-10-01" {
		t.Errorf("display = %q", s.display)
	}
}

// pg_stats answers with most common values for one column and a histogram
// for the other; a partition column gets partition values, newest first.
func TestFetchSuggestions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Stmt string
			Args []interface{}
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		col := ""
		if len(req.Args) > 2 {
			col = req.Args[2].(string)
		}
		var rows [][]interface{}
		switch {
		case strings.Contains(req.Stmt, "information_schema.columns"):
			rows = [][]interface{}{{map[string]string{"id": "bigint", "kind": "text", "day": "timestamp with time zone"}[col]}}
		case strings.Contains(req.Stmt, "partitioned_by"):
			rows = [][]interface{}{{[]interface{}{"day"}}}
		case strings.Contains(req.Stmt, "table_partitions"):
			rows = [][]interface{}{{map[string]interface{}{"day": "1790812800000"}}, {map[string]interface{}{"day": "1790899200000"}}}
		case strings.Contains(req.Stmt, "pg_stats") && col == "kind":
			rows = [][]interface{}{{[]interface{}{"a", "b"}, nil, float64(2)}}
		case strings.Contains(req.Stmt, "pg_stats") && col == "id":
			h := make([]interface{}, 101)
			for i := range h {
				h[i] = float64(i * 10)
			}
			rows = [][]interface{}{{nil, h, float64(59264)}}
		}
		_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{Rows: rows})
	}))
	defer srv.Close()
	reg := cratedb.NewRegistry(srv.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
	got := fetchSuggestions(context.Background(), reg, `SELECT * FROM doc.t WHERE id > ? AND kind = ? AND day = ? AND x + 1 = ?`).By
	if s := got[1]; s.source != "low · quartiles · high, ≈59.3K distinct" || strings.Join(s.insert, " ") != "0 250 500 750 1000" {
		t.Errorf("id = %+v", s)
	}
	if s := got[2]; !strings.HasPrefix(s.source, "most common") || strings.Join(s.insert, " ") != "a b" {
		t.Errorf("kind = %+v", s)
	}
	if s := got[3]; !strings.HasPrefix(s.source, "partitions, newest first") || s.display[0] != "2026-10-02" || s.insert[0] != "1790899200000" {
		t.Errorf("day = %+v", s)
	}
	if _, ok := got[4]; ok {
		t.Error("expression got a suggestion")
	}
}
