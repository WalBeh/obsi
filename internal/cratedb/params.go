package cratedb

import "strings"

// ParamColumn is the table column a placeholder is compared with.
type ParamColumn struct {
	Schema, Table, Column string
}

// ParamColumns maps placeholder numbers to the column they're compared
// with, for the shapes where that's plain: col = ?, col > ? (any
// comparison), col LIKE ?, col BETWEEN ? AND ?, col IN (?, ?) and
// col = ANY(?). The table comes from the FROM clause: an alias, or the
// only table. Expressions, function arguments, LIMIT and subqueries get
// nothing rather than a guess.
func ParamColumns(stmt string) map[int]ParamColumn {
	t := lexSQL(stmt, 1<<20)
	tables := fromTables(t)
	out := map[int]ParamColumn{}
	next := 0
	for i, tok := range t {
		n := 0
		switch {
		case tok.kind == tokSymbol && tok.text == "?":
			next++
			n = next
		case tok.kind == tokSymbol && tok.text == "$" && i+1 < len(t) && t[i+1].kind == tokWord && isDigits(t[i+1].text):
			n = atoi(t[i+1].text)
		default:
			continue
		}
		if _, done := out[n]; done {
			continue
		}
		qual, col, ok := columnFor(t, i)
		if !ok {
			continue
		}
		tbl, ok := tables[strings.ToLower(qual)]
		if !ok {
			continue
		}
		tbl.Column = col
		out[n] = tbl
	}
	return out
}

// columnFor walks back from the placeholder at i over the operator to the
// column, returning its qualifier ("" when unqualified).
func columnFor(t tokens, i int) (qual, col string, ok bool) {
	j := i - 1
	isParam := func(k int) bool {
		return k >= 0 && (t[k].text == "?" || (t[k].kind == tokWord && isDigits(t[k].text) && k > 0 && t[k-1].text == "$"))
	}
	paramStart := func(k int) int { // index of the first token of the placeholder ending at k
		if t[k].text == "?" {
			return k
		}
		return k - 1
	}
	// col IN (?, ?, ?) and col = ANY(?): back to the paren.
	if j >= 0 && (t[j].text == "(" || t[j].text == ",") {
		for j >= 0 && t[j].text != "(" {
			if t[j].text != "," && !isParam(j) {
				return "", "", false
			}
			if isParam(j) {
				j = paramStart(j)
			}
			j--
		}
		j-- // before (
		switch {
		case j >= 0 && t.word(j, "IN"):
			j--
			if t.word(j, "NOT") {
				j--
			}
			return columnAt(t, j)
		case j >= 0 && (t.word(j, "ANY") || t.word(j, "ALL")):
			j--
		default:
			return "", "", false
		}
	}
	// col BETWEEN ? AND ?: the second placeholder walks back to the first.
	if t.word(j, "AND") && isParam(j-1) {
		k := paramStart(j - 1)
		if t.word(k-1, "BETWEEN") {
			j = k - 1
		}
	}
	if t.word(j, "BETWEEN") {
		j--
		if t.word(j, "NOT") {
			j--
		}
		return columnAt(t, j)
	}
	if t.word(j, "LIKE") || t.word(j, "ILIKE") {
		j--
		if t.word(j, "NOT") {
			j--
		}
		return columnAt(t, j)
	}
	// Comparison: =, <>, !=, <, <=, >, >= (lexed as one or two symbols).
	if j >= 0 && t[j].kind == tokSymbol && strings.Contains("=<>!", t[j].text) {
		j--
		if j >= 0 && t[j].kind == tokSymbol && strings.Contains("<>!", t[j].text) {
			j--
		}
		return columnAt(t, j)
	}
	return "", "", false
}

// columnAt reads a column name ending at j: col or qual.col. A closing paren
// or literal there means an expression.
func columnAt(t tokens, j int) (qual, col string, ok bool) {
	if j < 0 || (t[j].kind != tokWord && t[j].kind != tokIdent) || (t[j].kind == tokWord && isDigits(t[j].text)) {
		return "", "", false
	}
	col = identText(t[j])
	start := j
	if j >= 2 && t[j-1].text == "." && (t[j-2].kind == tokWord || t[j-2].kind == tokIdent) {
		qual = identText(t[j-2])
		start = j - 2
		if j >= 4 && t[j-3].text == "." {
			return "", "", false // schema.table.col: rare, skip
		}
	}
	// a + b > ?: the column is part of an expression.
	if start > 0 && t[start-1].kind == tokSymbol && strings.Contains("+-*/%|", t[start-1].text) {
		return "", "", false
	}
	return qual, col, true
}

// fromTables maps aliases and table names to tables, from FROM and JOIN.
// The key "" is the only table when there is exactly one.
func fromTables(t tokens) map[string]ParamColumn {
	out := map[string]ParamColumn{}
	var all []ParamColumn
	for i := 0; i < len(t); i++ {
		if !t.word(i, "FROM") && !t.word(i, "JOIN") {
			continue
		}
		j := i + 1
		if j < len(t) && t[j].text == "(" {
			continue // subquery
		}
		var parts []string
		for j < len(t) && (t[j].kind == tokWord || t[j].kind == tokIdent) {
			parts = append(parts, identText(t[j]))
			j++
			if j < len(t) && t[j].text == "." {
				j++
				continue
			}
			break
		}
		var tbl ParamColumn
		switch len(parts) {
		case 1:
			tbl = ParamColumn{Schema: "doc", Table: parts[0]}
		case 2:
			tbl = ParamColumn{Schema: parts[0], Table: parts[1]}
		default:
			continue
		}
		all = append(all, tbl)
		out[strings.ToLower(tbl.Table)] = tbl
		if t.word(j, "AS") {
			j++
		}
		if j < len(t) && (t[j].kind == tokIdent || t[j].kind == tokWord && !isClauseWord(t[j].text)) {
			out[strings.ToLower(identText(t[j]))] = tbl
		}
	}
	if len(all) == 1 {
		out[""] = all[0]
	}
	return out
}

func identText(tk token) string {
	if tk.kind == tokWord {
		return strings.ToLower(tk.text)
	}
	return tk.text
}

var clauseWords = map[string]bool{"WHERE": true, "JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true,
	"ON": true, "GROUP": true, "ORDER": true, "LIMIT": true, "OFFSET": true, "HAVING": true, "UNION": true, "USING": true}

func isClauseWord(s string) bool { return clauseWords[strings.ToUpper(s)] }

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}
