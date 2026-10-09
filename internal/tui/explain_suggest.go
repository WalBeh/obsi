package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// Suggestions for the EXPLAIN ANALYZE form come from what CrateDB already
// knows about a column, never from scanning it: partition values for a
// partition column, else pg_stats (refreshed every stats.service.interval,
// 24h by default). They're plausible values, not the ones the slow run used.

type suggestion struct {
	source  string   // where they come from, shown before the values
	display []string // shortened for the form
	insert  []string // what ctrl+n puts into the field
	note    string   // why there are none
}

// ExplainSuggestMsg asks the App for suggestions for stmt's placeholders.
type ExplainSuggestMsg struct{ Stmt string }

// ExplainSuggestionsMsg carries them back, by placeholder number.
type ExplainSuggestionsMsg struct {
	Stmt string
	By   map[int]suggestion
}

const suggestLimit = 5

func fetchSuggestions(ctx context.Context, reg *cratedb.Registry, stmt string) ExplainSuggestionsMsg {
	out := ExplainSuggestionsMsg{Stmt: stmt, By: map[int]suggestion{}}
	cache := map[cratedb.ParamColumn]suggestion{}
	for n, col := range cratedb.ParamColumns(stmt) {
		s, ok := cache[col]
		if !ok {
			s = suggestFor(ctx, reg, col)
			cache[col] = s
		}
		out.By[n] = s
	}
	return out
}

func suggestFor(ctx context.Context, reg *cratedb.Registry, col cratedb.ParamColumn) suggestion {
	q := func(stmt string, args ...interface{}) [][]interface{} {
		resp, err := reg.Query(ctx, stmt+cratedb.QueryTag, args...)
		if err != nil {
			return nil
		}
		return resp.Rows
	}
	dataType := ""
	if rows := q(`SELECT data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
		col.Schema, col.Table, col.Column); len(rows) > 0 {
		dataType = cratedb.ToString(rows[0][0])
	}
	var partitioned []string
	if rows := q(`SELECT partitioned_by FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`, col.Schema, col.Table); len(rows) > 0 {
		if arr, ok := rows[0][0].([]interface{}); ok {
			for _, v := range arr {
				partitioned = append(partitioned, cratedb.ToString(v))
			}
		}
	}
	if contains(partitioned, col.Column) {
		var vals []interface{}
		for _, r := range q(`SELECT values FROM information_schema.table_partitions WHERE table_schema = ? AND table_name = ?`, col.Schema, col.Table) {
			if obj, ok := r[0].(map[string]interface{}); ok {
				vals = append(vals, obj[col.Column])
			}
		}
		sort.Slice(vals, func(i, j int) bool {
			return lessValues([]interface{}{numOrRaw(vals[j])}, []interface{}{numOrRaw(vals[i])})
		})
		s := suggestion{source: fmt.Sprintf("partitions, newest first, %d in all", len(vals))}
		for _, v := range vals[:min(len(vals), suggestLimit)] {
			s.add(v, dataType)
		}
		return s
	}

	rows := q(`SELECT most_common_vals, histogram_bounds, n_distinct FROM pg_catalog.pg_stats
WHERE schemaname = ? AND tablename = ? AND attname = ?`, col.Schema, col.Table, col.Column)
	if len(rows) == 0 {
		return suggestion{note: fmt.Sprintf("no statistics for %s.%s yet (CrateDB collects them every 24h, or after ANALYZE)", col.Table, col.Column)}
	}
	mcv, _ := rows[0][0].([]interface{})
	hist, _ := rows[0][1].([]interface{})
	distinct := cratedb.ToFloat64(rows[0][2])
	var s suggestion
	switch {
	case len(mcv) > 0:
		s.source = fmt.Sprintf("most common, ≈%s distinct", formatRecords(int64(distinct)))
		for _, v := range mcv[:min(len(mcv), suggestLimit)] {
			s.add(v, dataType)
		}
	case len(hist) > 0:
		s.source = fmt.Sprintf("low · quartiles · high, ≈%s distinct", formatRecords(int64(distinct)))
		last := len(hist) - 1
		for _, i := range []int{0, last / 4, last / 2, last * 3 / 4, last} {
			s.add(hist[i], dataType)
		}
	default:
		s.note = fmt.Sprintf("statistics for %s.%s have no values", col.Table, col.Column)
	}
	return s
}

// add appends a value, as the form would read it back: text columns get
// digits quoted so they stay text; timestamps show as dates but insert the
// number CrateDB stores.
func (s *suggestion) add(v interface{}, dataType string) {
	raw := cratedb.ToString(v)
	if f, ok := v.(float64); ok {
		raw = strconv.FormatFloat(f, 'f', -1, 64)
	}
	if v == nil {
		raw = "null"
	}
	if contains(s.insert, raw) {
		return
	}
	insert, display := raw, raw
	switch {
	case strings.HasPrefix(dataType, "timestamp"):
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			t := time.UnixMilli(ms).UTC()
			display = t.Format("2006-01-02 15:04")
			if t.Hour() == 0 && t.Minute() == 0 {
				display = t.Format("2006-01-02")
			}
		}
	case dataType == "text" || strings.HasPrefix(dataType, "character") || dataType == "varchar":
		if back, ok := parseValue(raw).(string); !ok || back != raw {
			b, _ := json.Marshal(raw)
			insert = string(b)
		}
	}
	s.insert = append(s.insert, insert)
	s.display = append(s.display, truncateString(display, 24))
}

func numOrRaw(v interface{}) interface{} {
	if s, ok := v.(string); ok {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	return v
}

func (m QueriesModel) setSuggestions(msg ExplainSuggestionsMsg) QueriesModel {
	if v := m.explain; v != nil && v.stmt == msg.Stmt {
		v.suggest, v.sugNext = msg.By, map[int]int{}
	}
	return m
}

func (v *explainView) suggestLine(n int, focused bool) string {
	if v.suggest == nil {
		if focused {
			return styleDim.Render("looking up suggestions…")
		}
		return ""
	}
	s, ok := v.suggest[n]
	switch {
	case !ok:
		return ""
	case s.note != "":
		return styleDim.Render(s.note)
	}
	return styleDim.Render(fmt.Sprintf("%s: %s", s.source, strings.Join(s.display, " · ")))
}
