package cratedb

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

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

// LeasePeriodSetting is the table setting that decides how long a
// subscriber's retention lease keeps operations it hasn't pulled.
const LeasePeriodSetting = "soft_deletes.retention_lease.period"

// DefaultLeasePeriod applies when a table doesn't set LeasePeriodSetting.
const DefaultLeasePeriod = 12 * time.Hour

var leasePeriodRe = regexp.MustCompile(`"soft_deletes\.retention_lease\.period"\s*=\s*'?(\d+)`)

// LeasePeriodFromDDL reads LeasePeriodSetting from SHOW CREATE TABLE. It's
// listed (in ms) only when the table sets it; information_schema doesn't
// have it at all.
func LeasePeriodFromDDL(ddl string) (time.Duration, bool) {
	if m := leasePeriodRe.FindStringSubmatch(ddl); m != nil {
		if ms, err := strconv.ParseInt(m[1], 10, 64); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond, true
		}
	}
	return 0, false
}

// QuoteIdent quotes a SQL identifier, doubling quotes inside.
func QuoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// MinReplicas is the replicas a number_of_replicas value insists on: n,
// or the lower end of a range like 0-1 that expands to the nodes there
// are.
func MinReplicas(setting string) (int, bool) {
	lo, _, _ := strings.Cut(setting, "-")
	n, err := strconv.Atoi(strings.TrimSpace(lo))
	return n, err == nil
}
