package cratedb

import (
	"reflect"
	"testing"
)

func TestClassifyChange(t *testing.T) {
	cases := []struct {
		stmt string
		want ChangeKind
	}{
		{`SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 'all'`, ChangeCluster},
		{`reset global "indices.recovery"`, ChangeCluster},
		{`/* from psql */ SET GLOBAL stats.enabled = false`, ChangeCluster},
		{"-- note\nALTER CLUSTER REROUTE RETRY FAILED", ChangeCluster},
		{`ALTER CLUSTER DECOMMISSION 'n1'`, ChangeCluster},
		{`ALTER TABLE doc.t SET (number_of_replicas = 1)`, ChangeTable},
		{`ALTER TABLE "My Schema"."t" RESET (number_of_replicas)`, ChangeTable},
		{`ALTER TABLE t PARTITION (p = 1, q = 'a)b') SET (number_of_replicas = 0)`, ChangeTable},
		{`ALTER TABLE ONLY doc.t SET ("routing.allocation.exclude._name" = 'n1')`, ChangeTable},
		{`ALTER TABLE doc.t CLOSE`, ChangeTable},
		{`ALTER TABLE doc.t REROUTE MOVE SHARD 0 FROM 'a' TO 'b'`, ChangeTable},
		{`CREATE USER u WITH (password = 'x')`, ChangeSecurity},
		{`alter role r set (password = 'x')`, ChangeSecurity},
		{`DROP USER IF EXISTS u`, ChangeSecurity},
		{`GRANT DQL ON TABLE doc.t TO u`, ChangeSecurity},
		{`DENY DML ON SCHEMA doc TO u`, ChangeSecurity},
		{`REVOKE r FROM u`, ChangeSecurity},

		{`SET search_path TO doc`, ""},
		{`SET SESSION enable_hashjoin = true`, ""},
		{`SET extra_float_digits = 3`, ""},
		{`ALTER TABLE doc.t ADD COLUMN b TEXT`, ""},
		{`ALTER TABLE doc.t RENAME TO u`, ""},
		{`CREATE TABLE doc.t (a INT)`, ""},
		{`REFRESH TABLE doc.t`, ""},
		{`KILL ALL`, ""},
		{`SELECT 'SET GLOBAL'`, ""},
		{``, ""},
	}
	for _, c := range cases {
		if got := ClassifyChange(c.stmt); got != c.want {
			t.Errorf("ClassifyChange(%q) = %q, want %q", c.stmt, got, c.want)
		}
	}
}

func TestAlteredTable(t *testing.T) {
	cases := []struct {
		stmt          string
		schema, table string
		partition, ok bool
	}{
		{`ALTER TABLE doc.t SET (x = 1)`, "doc", "t", false, true},
		{`ALTER TABLE T OPEN`, "", "t", false, true},
		{`ALTER TABLE "Doc"."a.b" SET (x = 1)`, "Doc", "a.b", false, true},
		{`ALTER TABLE doc.t PARTITION (p = (1)) SET (x = 1)`, "doc", "t", true, true},
		{`ALTER TABLE doc.t ADD COLUMN c INT`, "", "", false, false},
	}
	for _, c := range cases {
		s, tb, p, ok := AlteredTable(c.stmt)
		if s != c.schema || tb != c.table || p != c.partition || ok != c.ok {
			t.Errorf("AlteredTable(%q) = %q %q %v %v", c.stmt, s, tb, p, ok)
		}
	}
}

func TestSettingNames(t *testing.T) {
	got := SettingNames(`SET GLOBAL TRANSIENT "indices.recovery.max_bytes_per_sec" = '40mb', stats.enabled = ?`)
	want := []string{"set", "global", "transient", "indices.recovery.max_bytes_per_sec", "stats.enabled"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRedact(t *testing.T) {
	cases := map[string]string{
		`CREATE USER u WITH (password = 'se''cr3t')`:                              `CREATE USER u WITH (password = '***')`,
		`ALTER USER u SET ("password"='x')`:                                       `ALTER USER u SET ("password"='***')`,
		`CREATE REPOSITORY r TYPE s3 WITH (access_key = 'AK', secret_key = 'SK')`: `CREATE REPOSITORY r TYPE s3 WITH (access_key = '***', secret_key = '***')`,
		`ALTER USER u SET (password = ?)`:                                         `ALTER USER u SET (password = ?)`,
		`SELECT name FROM sys.users WHERE name = 'password'`:                      `SELECT name FROM sys.users WHERE name = 'password'`,
	}
	for in, want := range cases {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPlaceholders(t *testing.T) {
	got := Placeholders(`SELECT * FROM t WHERE id > ? AND name = '?' /* ? */ AND "a?" = ? -- ?
AND tags = ANY(?)`)
	want := []Placeholder{{1, "t WHERE id >"}, {2, `'?' AND "a?" =`}, {3, "tags = ANY("}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("? style:\n got %+v\nwant %+v", got, want)
	}
	// pg style: numbered, may repeat, may come out of order.
	got = Placeholders(`SELECT * FROM t WHERE b = $2 AND a = $1 OR a = $1`)
	if len(got) != 2 || got[0].N != 1 || got[0].Context != "$2 AND a =" || got[1].N != 2 {
		t.Errorf("$ style: %+v", got)
	}
	if got := Placeholders(`SELECT 1`); len(got) != 0 {
		t.Errorf("none: %+v", got)
	}
}

func TestExplainable(t *testing.T) {
	for stmt, want := range map[string]bool{
		"SELECT 1": true, "/* x */ with a as (select 1) select * from a": true,
		"UPDATE t SET a = 1": false, "INSERT INTO t VALUES (1)": false, "EXPLAIN SELECT 1": false,
	} {
		if got := Explainable(stmt); got != want {
			t.Errorf("Explainable(%q) = %v", stmt, got)
		}
	}
}
