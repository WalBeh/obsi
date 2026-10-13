package cratedb

import (
	"testing"
	"time"
)

func TestSplitRegclass(t *testing.T) {
	for in, want := range map[string][2]string{
		`doc.t`:             {"doc", "t"},
		`"My Schema"."a.b"`: {"My Schema", "a.b"},
		`s."say ""hi"""`:    {"s", `say "hi"`},
		`t`:                 {"doc", "t"},
	} {
		if s, tb := SplitRegclass(in); s != want[0] || tb != want[1] {
			t.Errorf("SplitRegclass(%q) = %q, %q", in, s, tb)
		}
	}
}

func TestLeasePeriodFromDDL(t *testing.T) {
	ddl := `CREATE TABLE IF NOT EXISTS "doc"."t" (...) WITH (
   "soft_deletes.retention_lease.period" = 120000,
   "store.type" = 'fs'
)`
	if d, ok := LeasePeriodFromDDL(ddl); !ok || d != 2*time.Minute {
		t.Errorf("got %s %v", d, ok)
	}
	if _, ok := LeasePeriodFromDDL(`CREATE TABLE "doc"."t" (a INT)`); ok {
		t.Error("found a period that isn't set")
	}
}
