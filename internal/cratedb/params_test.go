package cratedb

import (
	"reflect"
	"testing"
)

func TestParamColumns(t *testing.T) {
	big := func(c string) ParamColumn { return ParamColumn{"doc", "big", c} }
	cases := []struct {
		stmt string
		want map[int]ParamColumn
	}{
		{`SELECT * FROM doc.big WHERE id > ? AND pad LIKE ?`, map[int]ParamColumn{1: big("id"), 2: big("pad")}},
		{`SELECT * FROM big WHERE id BETWEEN $1 AND $2 AND pad = $3`, map[int]ParamColumn{1: big("id"), 2: big("id"), 3: big("pad")}},
		{`SELECT * FROM doc.big b WHERE b.id IN (?, ?, ?)`, map[int]ParamColumn{1: big("id"), 2: big("id"), 3: big("id")}},
		{`SELECT * FROM doc.big AS b JOIN doc.events e ON e.id = b.id WHERE e.day = ? AND b.id >= ?`,
			map[int]ParamColumn{1: {"doc", "events", "day"}, 2: big("id")}},
		{`SELECT * FROM doc.big WHERE id = ANY(?) AND pad NOT LIKE ?`, map[int]ParamColumn{1: big("id"), 2: big("pad")}},
		{`SELECT * FROM "My"."T" WHERE "Col" <> ?`, map[int]ParamColumn{1: {"My", "T", "Col"}}},
		// No guesses: expressions, functions, LIMIT, an unqualified column with two tables.
		{`SELECT * FROM doc.big WHERE date_trunc('day', ts) = ? AND a + b > ? LIMIT ?`, map[int]ParamColumn{}},
		{`SELECT * FROM doc.big a JOIN doc.events e ON a.id = e.id WHERE id = ?`, map[int]ParamColumn{}},
		{`SELECT * FROM (SELECT * FROM doc.big) s WHERE s.id = ?`, map[int]ParamColumn{}},
	}
	for _, c := range cases {
		if got := ParamColumns(c.stmt); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s\n got %+v\nwant %+v", c.stmt, got, c.want)
		}
	}
}
