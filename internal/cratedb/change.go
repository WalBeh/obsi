package cratedb

import (
	"regexp"
	"strings"
)

// ChangeKind is what a configuration change touches. Empty for statements
// that aren't one.
type ChangeKind string

const (
	ChangeCluster  ChangeKind = "cluster"
	ChangeTable    ChangeKind = "table"
	ChangeSecurity ChangeKind = "security"
)

// ChangeTag marks changes obsi runs itself (settings editor, shard fixes,
// recovery throttle). Unlike QueryTag they stay in the Queries tab; the
// changes board uses the tag to label them.
const ChangeTag = " /* obsi change */"

// ClassifyChange says whether stmt changes cluster settings, table settings
// or users and privileges. Session SETs, schema DDL, REFRESH and friends
// are not changes.
func ClassifyChange(stmt string) ChangeKind {
	t := lexSQL(stmt, 4)
	switch {
	case t.word(0, "SET") && t.word(1, "GLOBAL"),
		t.word(0, "RESET") && t.word(1, "GLOBAL"),
		t.word(0, "ALTER") && t.word(1, "CLUSTER"):
		return ChangeCluster
	case t.word(0, "GRANT"), t.word(0, "REVOKE"), t.word(0, "DENY"):
		return ChangeSecurity
	case (t.word(0, "CREATE") || t.word(0, "ALTER") || t.word(0, "DROP")) &&
		(t.word(1, "USER") || t.word(1, "ROLE")):
		return ChangeSecurity
	case t.word(0, "ALTER") && t.word(1, "TABLE"):
		if _, _, _, ok := AlteredTable(stmt); ok {
			return ChangeTable
		}
	}
	return ""
}

// AlteredTable parses ALTER TABLE statements that change settings, open or
// close a table or move shards. Schema changes (ADD COLUMN, RENAME, ...)
// return ok=false. schema is empty when the statement doesn't name one.
// Unquoted names are lowercased, as CrateDB does.
func AlteredTable(stmt string) (schema, table string, partition, ok bool) {
	t := lexSQL(stmt, 64)
	if !t.word(0, "ALTER") || !t.word(1, "TABLE") {
		return "", "", false, false
	}
	i := 2
	if t.word(i, "ONLY") {
		i++
	}
	var parts []string
	for i < len(t) && (t[i].kind == tokWord || t[i].kind == tokIdent) {
		part := t[i].text
		if t[i].kind == tokWord {
			part = strings.ToLower(part)
		}
		parts = append(parts, part)
		i++
		if i < len(t) && t[i].text == "." {
			i++
			continue
		}
		break
	}
	switch len(parts) {
	case 1:
		table = parts[0]
	case 2:
		schema, table = parts[0], parts[1]
	default:
		return "", "", false, false
	}
	if t.word(i, "PARTITION") {
		partition = true
		i++
		depth := 0
		for ; i < len(t); i++ {
			if t[i].text == "(" {
				depth++
			} else if t[i].text == ")" {
				depth--
				if depth == 0 {
					i++
					break
				}
			}
		}
	}
	for _, w := range []string{"SET", "RESET", "OPEN", "CLOSE", "REROUTE"} {
		if t.word(i, w) {
			return schema, table, partition, true
		}
	}
	return "", "", false, false
}

// SettingNames lists the dotted names a SET/RESET GLOBAL statement refers
// to, lowercased: `"indices.recovery.max_bytes_per_sec"`, `stats.enabled`.
// Keywords come along too; they never prefix a real setting.
func SettingNames(stmt string) []string {
	t := lexSQL(stmt, 256)
	var names []string
	for i := 0; i < len(t); i++ {
		if t[i].kind != tokWord && t[i].kind != tokIdent {
			continue
		}
		name := t[i].text
		for i+2 < len(t) && t[i+1].text == "." && (t[i+2].kind == tokWord || t[i+2].kind == tokIdent) {
			name += "." + t[i+2].text
			i += 2
		}
		names = append(names, strings.ToLower(name))
	}
	return names
}

// secretRe finds string literals given for password-like options, e.g.
// CREATE USER u WITH (password = 'x'). sys.jobs and sys.jobs_log keep the
// statement as typed.
var secretRe = regexp.MustCompile(`(?i)("?\b(?:password|secret\w*|\w*access_key|\w*token)\b"?\s*=?\s*)'(?:[^']|'')*'`)

// Redact replaces password-like literals in stmt with '***'.
func Redact(stmt string) string {
	return secretRe.ReplaceAllString(stmt, "$1'***'")
}

type tokKind int

const (
	tokWord  tokKind = iota // keyword or bare identifier
	tokIdent                // "quoted identifier", unquoted
	tokString
	tokSymbol
)

type token struct {
	kind tokKind
	text string
}

type tokens []token

func (t tokens) word(i int, w string) bool {
	return i < len(t) && t[i].kind == tokWord && strings.EqualFold(t[i].text, w)
}

// lexSQL splits the start of s into at most n tokens, skipping comments.
func lexSQL(s string, n int) tokens {
	var out tokens
	for len(out) < n {
		s = strings.TrimLeft(s, " \t\r\n")
		if s == "" {
			break
		}
		switch {
		case strings.HasPrefix(s, "--"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return out
			}
			s = s[i+1:]
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s, "*/")
			if i < 0 {
				return out
			}
			s = s[i+2:]
		case s[0] == '"' || s[0] == '\'':
			q := s[0]
			var b strings.Builder
			i := 1
			for ; i < len(s); i++ {
				if s[i] == q {
					if i+1 < len(s) && s[i+1] == q {
						b.WriteByte(q)
						i++
						continue
					}
					break
				}
				b.WriteByte(s[i])
			}
			kind := tokIdent
			if q == '\'' {
				kind = tokString
			}
			out = append(out, token{kind, b.String()})
			s = s[min(i+1, len(s)):]
		case isWordByte(s[0]):
			i := 1
			for i < len(s) && isWordByte(s[i]) {
				i++
			}
			out = append(out, token{tokWord, s[:i]})
			s = s[i:]
		default:
			out = append(out, token{tokSymbol, s[:1]})
			s = s[1:]
		}
	}
	return out
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
}
