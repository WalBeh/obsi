package cratedb

import "strings"

// Subscription is a row of pg_subscription with its tables from
// pg_subscription_rel.
type Subscription struct {
	Name         string
	ConnInfo     string // CrateDB masks user and password
	Publications []string
	Tables       []SubscribedTable
}

// SubscribedTable is a row of pg_subscription_rel. State is i
// (initializing), d (the initial copy), r (in sync) or e (failed, see
// Reason). Partitions don't get rows of their own.
type SubscribedTable struct {
	Schema string
	Table  string
	State  string
	Reason string
}

func (t SubscribedTable) FQN() string { return t.Schema + "." + t.Table }

// Publication is a row of pg_publication with its tables from
// pg_publication_tables, which lists every table for FOR ALL TABLES too.
type Publication struct {
	Name      string
	AllTables bool
	Tables    []string // schema.table
}

// SplitRegclass splits regclass text such as doc.t or "My Schema"."T" into
// schema and table, unquoted.
func SplitRegclass(s string) (schema, table string) {
	var parts []string
	var cur strings.Builder
	quoted := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' && quoted && i+1 < len(s) && s[i+1] == '"':
			cur.WriteByte('"')
			i++
		case c == '"':
			quoted = !quoted
		case c == '.' && !quoted:
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	parts = append(parts, cur.String())
	if len(parts) == 1 {
		return "doc", parts[0]
	}
	return parts[len(parts)-2], parts[len(parts)-1]
}
