package tui

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// E on the Queries tab: EXPLAIN first (the plan, no values needed, the query
// doesn't run), then on request EXPLAIN ANALYZE, which runs the query again
// and needs the bind values. CrateDB doesn't keep those anywhere (sys.jobs
// and sys.jobs_log hold the text with ? or $n), so the user types them.

type explainPhase int

const (
	explainPlanning explainPhase = iota
	explainPlanned
	explainForm
	explainRunning
	explainDone
)

type explainView struct {
	stmt    string
	lastRun time.Duration // how long the query took when obsi saw it
	phase   explainPhase
	plan    string
	err     string

	params  []cratedb.Placeholder
	inputs  []string
	focus   int
	suggest map[int]suggestion // by placeholder number; nil until read
	sugNext map[int]int

	args      []interface{}
	startedAt time.Time
	result    *analyzeResult

	scroll    int // first body line shown in the plan/result view
	maxScroll int // set while rendering, for clamping the keys
}

// ExplainPlanMsg asks the App for EXPLAIN <stmt>.
type ExplainPlanMsg struct{ Stmt string }

// ExplainPlanResultMsg carries the plan back.
type ExplainPlanResultMsg struct{ Plan, Err string }

// ExplainAnalyzeMsg asks the App to run EXPLAIN ANALYZE with args.
type ExplainAnalyzeMsg struct {
	Stmt string
	Args []interface{}
}

// ExplainAnalyzeResultMsg carries the raw result back; Killed says obsi
// stopped the job (timeout or esc).
type ExplainAnalyzeResultMsg struct {
	Raw    interface{}
	Err    string
	Killed bool
}

// ExplainCancelMsg asks the App to stop a running EXPLAIN ANALYZE.
type ExplainCancelMsg struct{}

func trimStmt(s string) string {
	return strings.TrimRight(strings.TrimSpace(s), "; \n\t")
}

func (m QueriesModel) openExplain(stmt string, lastRun time.Duration) (QueriesModel, tea.Cmd) {
	stmt = trimStmt(stmt)
	v := &explainView{stmt: stmt, lastRun: lastRun}
	m.explain = v
	if !cratedb.Explainable(stmt) {
		v.phase, v.err = explainPlanned, "CrateDB only explains queries (SELECT, WITH, VALUES), not this statement"
		return m, nil
	}
	return m, func() tea.Msg { return ExplainPlanMsg{Stmt: stmt} }
}

func runExplainPlan(ctx context.Context, reg *cratedb.Registry, msg ExplainPlanMsg) ExplainPlanResultMsg {
	resp, err := reg.Query(ctx, "EXPLAIN "+msg.Stmt+cratedb.QueryTag)
	if err != nil {
		return ExplainPlanResultMsg{Err: firstLine(err.Error())}
	}
	var lines []string
	for _, r := range resp.Rows {
		if len(r) > 0 {
			lines = append(lines, cratedb.ToString(r[0]))
		}
	}
	return ExplainPlanResultMsg{Plan: strings.Join(lines, "\n")}
}

// runExplainAnalyze tags the statement so the job can be found and killed:
// ending the HTTP request doesn't stop the query on the server. The tag is
// visible in the Queries tab on purpose.
func runExplainAnalyze(ctx context.Context, reg *cratedb.Registry, msg ExplainAnalyzeMsg) ExplainAnalyzeResultMsg {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	tag := "obsi explain " + hex.EncodeToString(b)
	resp, err := reg.QueryUnbounded(ctx, "EXPLAIN ANALYZE "+msg.Stmt+" /* "+tag+" */", msg.Args...)
	if err == nil {
		if len(resp.Rows) == 0 || len(resp.Rows[0]) == 0 {
			return ExplainAnalyzeResultMsg{Err: "EXPLAIN ANALYZE returned nothing"}
		}
		return ExplainAnalyzeResultMsg{Raw: resp.Rows[0][0]}
	}
	if ctx.Err() == nil {
		return ExplainAnalyzeResultMsg{Err: firstLine(err.Error())}
	}
	reason := "stopped"
	if ctx.Err() == context.DeadlineExceeded {
		reason = "timed out"
	}
	found, done := killTagged(reg, tag)
	switch {
	case found == 0:
		return ExplainAnalyzeResultMsg{Killed: true, Err: reason + "; the job had already finished"}
	case done == found:
		return ExplainAnalyzeResultMsg{Killed: true, Err: reason + "; killed the job on the cluster"}
	}
	return ExplainAnalyzeResultMsg{Killed: true, Err: reason + "; KILL sent, CrateDB is still stopping the job (see the Queries tab)"}
}

// killTagged kills the jobs carrying tag. CrateDB keeps running a query
// whose HTTP request went away, and KILL only returns once the job has
// stopped, which took over a minute for a cross join on the shardlab; so
// each KILL gets a few seconds and is otherwise left to finish on its own.
func killTagged(reg *cratedb.Registry, tag string) (found, done int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := reg.Query(ctx, "SELECT id FROM sys.jobs WHERE stmt LIKE ?"+cratedb.QueryTag, "%"+tag+"%")
	if err != nil {
		return 0, 0
	}
	for _, r := range resp.Rows {
		found++
		kctx, kcancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, err := reg.QueryUnbounded(kctx, "KILL ?", cratedb.ToString(r[0])); err == nil {
			done++
		}
		kcancel()
	}
	return found, done
}

// parseValue reads a typed bind value: JSON when it parses (123, "text",
// [1,2], null, true), the plain text otherwise.
func parseValue(s string) interface{} {
	var v interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(s)), &v); err == nil {
		return v
	}
	return s
}

// analyzeResult is EXPLAIN ANALYZE's JSON, node ids replaced by names.
type analyzeResult struct {
	total, plan, analyze float64 // ms
	phases               []phaseTime
	breakdown            []queryTime
	pretty               string
}

type phaseTime struct {
	name  string
	nodes []nodeTime // slowest first
	max   float64
}

type nodeTime struct {
	node string
	ms   float64
}

type queryTime struct {
	node, table, query, desc string
	shard                    int
	ms, docs                 float64
}

func parseAnalyze(raw interface{}, names map[string]string) *analyzeResult {
	name := func(id string) string {
		if n, ok := names[id]; ok && n != "" {
			return n
		}
		return id
	}
	r := &analyzeResult{}
	obj, _ := raw.(map[string]interface{})
	r.plan, r.analyze = cratedb.ToFloat64(obj["Plan"]), cratedb.ToFloat64(obj["Analyze"])
	exec, _ := obj["Execute"].(map[string]interface{})
	r.total = cratedb.ToFloat64(exec["Total"])
	phases, _ := exec["Phases"].(map[string]interface{})
	for pname, p := range phases {
		pm, _ := p.(map[string]interface{})
		nodes, _ := pm["nodes"].(map[string]interface{})
		pt := phaseTime{name: pname}
		for id, ms := range nodes {
			nt := nodeTime{node: name(id), ms: cratedb.ToFloat64(ms)}
			pt.nodes = append(pt.nodes, nt)
			pt.max = max(pt.max, nt.ms)
		}
		sort.Slice(pt.nodes, func(i, j int) bool { return pt.nodes[i].ms > pt.nodes[j].ms })
		r.phases = append(r.phases, pt)
	}
	sort.Slice(r.phases, func(i, j int) bool { return r.phases[i].name < r.phases[j].name })
	for id, v := range exec {
		nm, ok := v.(map[string]interface{})
		if !ok || id == "Phases" {
			continue
		}
		qb, _ := nm["QueryBreakdown"].([]interface{})
		for _, q := range qb {
			qm, _ := q.(map[string]interface{})
			bd, _ := qm["BreakDown"].(map[string]interface{})
			r.breakdown = append(r.breakdown, queryTime{
				node: name(id), table: cratedb.ToString(qm["TableName"]), query: cratedb.ToString(qm["QueryName"]),
				desc: cratedb.ToString(qm["QueryDescription"]), shard: int(cratedb.ToInt64(qm["ShardId"])),
				ms: cratedb.ToFloat64(qm["Time"]), docs: cratedb.ToFloat64(bd["next_doc_count"]),
			})
		}
	}
	sort.Slice(r.breakdown, func(i, j int) bool { return r.breakdown[i].ms > r.breakdown[j].ms })
	pretty, _ := json.MarshalIndent(raw, "", "  ")
	r.pretty = string(pretty)
	for id, n := range names {
		if n != "" {
			r.pretty = strings.ReplaceAll(r.pretty, `"`+id+`"`, `"`+n+`"`)
		}
	}
	return r
}

func (m QueriesModel) handleExplainKey(msg tea.KeyMsg) (QueriesModel, tea.Cmd) {
	v := m.explain
	switch v.phase {
	case explainForm:
		switch msg.Type {
		case tea.KeyEsc:
			v.phase = explainPlanned
		case tea.KeyEnter:
			v.args = make([]interface{}, len(v.params))
			for i := range v.params {
				v.args[i] = parseValue(v.inputs[i])
			}
			v.phase, v.startedAt, v.err = explainRunning, time.Now(), ""
			args, stmt := v.args, v.stmt
			return m, func() tea.Msg { return ExplainAnalyzeMsg{Stmt: stmt, Args: args} }
		case tea.KeyTab, tea.KeyDown:
			if len(v.params) > 0 {
				v.focus = (v.focus + 1) % len(v.params)
			}
		case tea.KeyShiftTab, tea.KeyUp:
			if len(v.params) > 0 {
				v.focus = (v.focus - 1 + len(v.params)) % len(v.params)
			}
		case tea.KeyCtrlN:
			if len(v.params) > 0 {
				p := v.params[v.focus]
				if s := v.suggest[p.N]; len(s.insert) > 0 {
					v.inputs[v.focus] = s.insert[v.sugNext[p.N]%len(s.insert)]
					v.sugNext[p.N]++
				}
			}
		case tea.KeyBackspace:
			if len(v.params) > 0 && len(v.inputs[v.focus]) > 0 {
				r := []rune(v.inputs[v.focus])
				v.inputs[v.focus] = string(r[:len(r)-1])
			}
		case tea.KeyRunes, tea.KeySpace:
			if len(v.params) > 0 {
				v.inputs[v.focus] += string(msg.Runes)
			}
		}
		return m, nil
	case explainRunning:
		if msg.Type == tea.KeyEsc {
			return m, func() tea.Msg { return ExplainCancelMsg{} }
		}
		return m, nil
	}
	page := max(m.height-10, 5)
	switch {
	case key.Matches(msg, m.keyMap.Up):
		v.scroll = max(v.scroll-1, 0)
	case key.Matches(msg, m.keyMap.Down):
		v.scroll = min(v.scroll+1, v.maxScroll)
	case msg.Type == tea.KeyPgUp || key.Matches(msg, m.keyMap.DetailUp):
		v.scroll = max(v.scroll-page, 0)
	case msg.Type == tea.KeyPgDown || key.Matches(msg, m.keyMap.DetailDown):
		v.scroll = min(v.scroll+page, v.maxScroll)
	case msg.Type == tea.KeyHome:
		v.scroll = 0
	case msg.Type == tea.KeyEnd:
		v.scroll = v.maxScroll
	case msg.Type == tea.KeyEsc || key.Matches(msg, m.keyMap.Explain):
		m.explain = nil
	case msg.String() == "a" && v.plan != "" && (v.phase == explainPlanned || v.phase == explainDone):
		v.params = cratedb.Placeholders(v.stmt)
		if len(v.inputs) != len(v.params) {
			v.inputs = make([]string, len(v.params))
		}
		v.phase, v.focus = explainForm, 0
		if v.suggest == nil && len(v.params) > 0 {
			stmt := v.stmt
			return m, func() tea.Msg { return ExplainSuggestMsg{Stmt: stmt} }
		}
	case key.Matches(msg, m.keyMap.Yank) && (v.phase == explainPlanned || v.phase == explainDone):
		text := v.copyText()
		return m, func() tea.Msg { return YankResultMsg{Error: writeClipboard(text)} }
	}
	return m, nil
}

func (v *explainView) copyText() string {
	var b strings.Builder
	fmt.Fprintf(&b, "-- obsi explain, %s\n", time.Now().UTC().Format(time.RFC3339))
	b.WriteString(v.stmt + ";\n")
	if len(v.args) > 0 {
		b.WriteString("\n-- parameters\n")
		for i, p := range v.params {
			val, _ := json.Marshal(v.args[i])
			fmt.Fprintf(&b, "-- $%d = %s\n", p.N, val)
		}
	}
	if v.plan != "" {
		b.WriteString("\n-- EXPLAIN\n" + v.plan + "\n")
	}
	if v.result != nil {
		b.WriteString("\n-- EXPLAIN ANALYZE (node ids replaced by names)\n" + v.result.pretty + "\n")
	}
	return b.String()
}

func (m QueriesModel) renderExplain(timeout time.Duration) string {
	v := m.explain
	innerWidth := modalInnerWidth(m.width, 90, 60)
	var lines []string
	title := "EXPLAIN"
	if v.phase >= explainForm {
		title = "EXPLAIN ANALYZE"
	}
	lines = append(lines, styleModalTitle.Render(title), "")
	stmtLines := wrapText(v.stmt, innerWidth)
	if len(stmtLines) > 6 && v.phase != explainPlanned && v.phase != explainDone {
		stmtLines = append(stmtLines[:6], styleDim.Render(fmt.Sprintf("… %d more lines (y copies all)", len(stmtLines)-6)))
	}
	lines = append(lines, stmtLines...)
	lines = append(lines, "")

	switch v.phase {
	case explainPlanning:
		lines = append(lines, styleDim.Render("planning…"))
	case explainPlanned, explainDone:
		if v.err != "" {
			lines = append(lines, styleHealthRed.Render(v.err), "")
		}
		if v.result != nil {
			lines = append(lines, v.renderAnalyze(innerWidth)...)
			lines = append(lines, "")
		}
		if v.plan != "" {
			lines = append(lines, styleTitle.Render("Plan"))
			for _, l := range strings.Split(v.plan, "\n") {
				lines = append(lines, wrapText(l, innerWidth)...)
			}
			lines = append(lines, "")
		}
		footer := "[y] copy   [esc] close"
		if v.plan != "" {
			footer = "[a] EXPLAIN ANALYZE (runs the query again)   " + footer
		}
		// Title stays, the rest scrolls; the modal's border and padding
		// take 4 lines.
		lines = v.scrolled(lines[:2], lines[2:], m.height-4-2-1)
		if v.maxScroll > 0 {
			footer = "[↑↓ pgup pgdn] scroll   " + footer
		}
		lines = append(lines, styleDim.Render(footer))
	case explainForm:
		last := "obsi didn't see it finish"
		if v.lastRun > 0 {
			last = "last run " + formatDuration(v.lastRun)
		}
		lines = append(lines, styleHealthYellow.Render(fmt.Sprintf("Runs the query again on the cluster (%s); obsi stops it after %s.", last, formatDuration(timeout))))
		if len(v.params) == 0 {
			lines = append(lines, "No parameters.")
		} else {
			lines = append(lines, "CrateDB doesn't keep the values a client bound, so type them. Suggestions are plausible",
				"values from what CrateDB knows about the column, not the ones the slow run used.", "")
			for i, p := range v.params {
				marker := "  "
				val := v.inputs[i]
				if i == v.focus {
					marker = "▸ "
					val = styleEditInput.Render(val + "▏")
				}
				lines = append(lines, fmt.Sprintf("%s$%-3d %-36s %s", marker, p.N, truncateString(p.Context+" ?", 36), val))
				if l := v.suggestLine(p.N, i == v.focus); l != "" {
					lines = append(lines, truncateString("        "+l, innerWidth))
				}
			}
			lines = append(lines, "",
				styleDim.Render("Type each value as the application would send it: 100000, a%, 2026-10-08, [1, 2], null."),
				styleDim.Render(`Digits become a number; to send them as text, quote them: "123".`))
		}
		lines = append(lines, "", styleDim.Render("[enter] run   [tab ↑↓] field   [ctrl+n] next suggestion   [esc] back"))
	case explainRunning:
		lines = append(lines, fmt.Sprintf("running EXPLAIN ANALYZE… %s", formatDuration(time.Since(v.startedAt).Round(time.Second))),
			"", styleDim.Render(fmt.Sprintf("[esc] stop (obsi kills the job; it also does after %s)", formatDuration(timeout))))
	}
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, m.width, m.height)
}

func (v *explainView) renderAnalyze(width int) []string {
	r := v.result
	lines := []string{styleTitle.Render("Analyze") + fmt.Sprintf("  total %s   plan %s   analyze %s", ms(r.total), ms(r.plan), ms(r.analyze))}
	var slowest float64
	for _, p := range r.phases {
		slowest = max(slowest, p.max)
	}
	for _, p := range r.phases {
		var nodes []string
		for _, n := range p.nodes {
			nodes = append(nodes, fmt.Sprintf("%s %s", n.node, ms(n.ms)))
		}
		row := fmt.Sprintf("  %-20s %s", truncateString(p.name, 20), strings.Join(nodes, "   "))
		if p.max == slowest && len(r.phases) > 1 {
			row += styleHealthYellow.Render("  ← slowest phase")
		}
		lines = append(lines, truncateString(row, width+20))
	}
	if len(r.breakdown) > 0 {
		lines = append(lines, "", styleDim.Render("  Lucene, slowest shards:"))
		for _, q := range r.breakdown {
			lines = append(lines, truncateString(fmt.Sprintf("  %-14s %s shard %d  %s %s  %s, %s docs",
				truncateString(q.node, 14), q.table, q.shard, q.query, truncateString(q.desc, 40), ms(q.ms), formatRecords(int64(q.docs))), width))
		}
	}
	return lines
}

// ms formats CrateDB's millisecond floats; plan and analyze are often
// below 1ms, which formatDuration would show as 0ms.
func ms(v float64) string {
	if v < 1000 {
		return strconv.FormatFloat(v, 'f', 1, 64) + "ms"
	}
	return formatDuration(time.Duration(v * float64(time.Millisecond)))
}

// selectedStmt is the statement under the cursor in whichever view is open,
// with how long it ran.
func (m QueriesModel) selectedStmt() (string, time.Duration, bool) {
	switch m.view() {
	case queriesLive:
		visible, _ := m.visibleQueries()
		if m.selected < len(visible) {
			q := visible[m.selected]
			return q.Stmt, time.Since(q.Started), true
		}
	case queriesSlowest:
		switch m.logMode {
		case store.JobsLogFailed:
			if m.slowSelected < len(m.snap.JobsLog.Failed) {
				e := m.snap.JobsLog.Failed[m.slowSelected]
				return e.Stmt, e.Ended.Sub(e.Started), true
			}
		case store.JobsLogGrouped:
			if m.slowSelected < len(m.snap.JobsLog.Groups) {
				g := m.snap.JobsLog.Groups[m.slowSelected]
				return g.Stmt, g.Max, true
			}
		default:
			if m.slowSelected < len(m.snap.SlowestQueries) {
				o := m.snap.SlowestQueries[m.slowSelected]
				return o.Stmt, o.Duration(), true
			}
		}
	}
	return "", 0, false
}

func (m QueriesModel) setExplainPlan(msg ExplainPlanResultMsg) QueriesModel {
	if v := m.explain; v != nil && v.phase == explainPlanning {
		v.plan, v.err, v.phase = msg.Plan, msg.Err, explainPlanned
	}
	return m
}

func (m QueriesModel) setExplainResult(msg ExplainAnalyzeResultMsg, names map[string]string) QueriesModel {
	v := m.explain
	if v == nil || v.phase != explainRunning {
		return m
	}
	v.phase, v.err, v.scroll = explainDone, msg.Err, 0
	if msg.Err == "" {
		v.result = parseAnalyze(msg.Raw, names)
	}
	return m
}

// scrolled shows head, then the part of body that fits in height lines
// from v.scroll, with markers for what's above and below.
func (v *explainView) scrolled(head, body []string, height int) []string {
	height = max(height-len(head), 3)
	if len(body) <= height {
		v.maxScroll, v.scroll = 0, 0
		return append(head, body...)
	}
	// Scrolled to the end, the "↑ more" marker takes a line.
	v.maxScroll = len(body) - (height - 1)
	v.scroll = min(max(v.scroll, 0), v.maxScroll)
	// The markers take a line each when shown.
	room := height
	if v.scroll > 0 {
		room--
	}
	if v.scroll+room < len(body) {
		room--
	}
	out := append([]string{}, head...)
	if v.scroll > 0 {
		out = append(out, styleDim.Render(fmt.Sprintf("↑ %d more lines", v.scroll)))
	}
	end := min(v.scroll+room, len(body))
	out = append(out, body[v.scroll:end]...)
	if end < len(body) {
		out = append(out, styleDim.Render(fmt.Sprintf("↓ %d more lines", len(body)-end)))
	}
	return out
}
