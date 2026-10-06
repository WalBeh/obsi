package tui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// settingsServer keeps persistent and transient values for one setting the
// way CrateDB does: transient wins, values come back normalised.
type settingsServer struct {
	mu                    sync.Mutex
	persistent, transient string
	def                   string
	stmts                 []string
}

var setRe = regexp.MustCompile(`SET GLOBAL (PERSISTENT|TRANSIENT) "[^"]+" = \?`)

func (s *settingsServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Stmt string
		Args []interface{}
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stmts = append(s.stmts, req.Stmt)
	var rows [][]interface{}
	switch {
	case strings.Contains(req.Stmt, "FROM sys.cluster") && strings.Contains(req.Stmt, "settings["):
		v := s.def
		if s.persistent != "" {
			v = s.persistent
		}
		if s.transient != "" {
			v = s.transient
		}
		rows = [][]interface{}{{v}}
	case strings.Contains(req.Stmt, "FROM sys.cluster"):
		rows = [][]interface{}{{"id", "crate"}}
	default:
		if m := setRe.FindStringSubmatch(req.Stmt); m != nil {
			v := strings.ToLower(req.Args[0].(string))
			if v == "1g" {
				v = "1gb"
			}
			if m[1] == "PERSISTENT" {
				s.persistent = v
			} else {
				s.transient = v
			}
		}
	}
	_ = json.NewEncoder(w).Encode(cratedb.SQLResponse{Rows: rows})
}

func (s *settingsServer) sets() string {
	var out []string
	for _, st := range s.stmts {
		if m := setRe.FindStringSubmatch(st); m != nil {
			out = append(out, m[1])
		}
	}
	return strings.Join(out, " ")
}

func TestApplySettingOverriddenByTransient(t *testing.T) {
	cases := []struct {
		name, transient, persistent, value string
		wantSets, wantNote                 string
	}{
		{"plain", "", "", "400mb", "PERSISTENT", ""},
		{"transient wins", "80mb", "", "400mb", "PERSISTENT TRANSIENT", "a TRANSIENT 80mb was overriding it"},
		{"same value", "", "400mb", "400MB", "PERSISTENT", ""},
		{"same value, other spelling", "", "1gb", "1g", "PERSISTENT TRANSIENT", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := &settingsServer{def: "40mb", transient: c.transient, persistent: c.persistent}
			ts := httptest.NewServer(srv)
			defer ts.Close()
			reg := cratedb.NewRegistry(ts.URL, "", "", time.Second, time.Second, time.Hour, time.Hour, false)
			res := applySetting(context.Background(), reg, SetSettingMsg{
				SettingPath: "indices.recovery.max_bytes_per_sec", Value: c.value, Persistent: true})
			if res.Error != "" {
				t.Fatal(res.Error)
			}
			if got := srv.sets(); got != c.wantSets {
				t.Errorf("statements %q, want %q", got, c.wantSets)
			}
			if !strings.Contains(res.Note, c.wantNote) || (c.wantNote == "" && res.Note != "") {
				t.Errorf("note %q, want %q", res.Note, c.wantNote)
			}
		})
	}
}
