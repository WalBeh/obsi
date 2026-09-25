package tui

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// shardFix is a diagnosed cause for shards not being allocated, with the
// statement that fixes it where there is one.
type shardFix struct {
	key        string // same cause across shards
	cause      string
	stmt       string // empty when there's nothing sensible to run
	readOnlyOK bool   // only asks CrateDB to retry, allowed in read-only mode
}

// shardDiagnosis is a fix plus how many shards it covers.
type shardDiagnosis struct {
	shardFix
	shards int
}

const retryFailedStmt = "ALTER CLUSTER REROUTE RETRY FAILED"

var filterRe = regexp.MustCompile(`index setting \[index\.(routing\.allocation\.(?:exclude|include|require))\] filters \[([^:\]]+):`)

func quoteTable(schema, table string) string {
	q := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	return q(schema) + "." + q(table)
}

// diagnoseShard matches the explanation texts CrateDB 6.3 gives in
// sys.allocations against the common causes. replicas is the table's
// number_of_replicas setting, "" when unknown.
func diagnoseShard(s cratedb.ShardInfo, a cratedb.AllocationInfo, cs cratedb.ClusterSettings, replicas string) []shardFix {
	table := s.SchemaName + "." + s.TableName
	texts := []string{a.Explanation}
	allHoldCopy := len(a.Decisions) > 0
	var watermarkNodes []string
	for _, d := range a.Decisions {
		holds := false
		for _, e := range d.Explanations {
			texts = append(texts, e)
			if strings.Contains(e, "a copy of this shard is already allocated to this node") {
				holds = true
			}
			if strings.Contains(e, "watermark") && !contains(watermarkNodes, d.NodeName) {
				watermarkNodes = append(watermarkNodes, d.NodeName)
			}
		}
		allHoldCopy = allHoldCopy && holds
	}
	all := strings.Join(texts, "\n")

	var fixes []shardFix
	add := func(f shardFix) { fixes = append(fixes, f) }
	if strings.Contains(all, "exceeded the maximum number of retries") {
		add(shardFix{key: "retry", cause: "allocation failed too often and CrateDB stopped retrying",
			stmt: retryFailedStmt, readOnlyOK: true})
	}
	if strings.Contains(all, "can no longer be found on the nodes") {
		add(shardFix{key: "lost/" + table, cause: "the last copy of " + table + " was on a node that is gone: bring that node back"})
	}
	if v := cs.AllocationEnable; (v != "" && v != "all" && s.RoutingState == "UNASSIGNED") || strings.Contains(all, "cluster.routing.allocation.enable=") {
		add(shardFix{key: "enable", cause: fmt.Sprintf("cluster.routing.allocation.enable = %s", v),
			stmt: `RESET GLOBAL "cluster.routing.allocation.enable"`})
	}
	if m := filterRe.FindStringSubmatch(all); m != nil {
		setting := m[1] + "." + m[2]
		add(shardFix{key: "filter/" + table + "/" + setting, cause: fmt.Sprintf("%s on %s keeps it off nodes", setting, table),
			stmt: fmt.Sprintf(`ALTER TABLE %s RESET ("%s")`, quoteTable(s.SchemaName, s.TableName), setting)})
	}
	if allHoldCopy && !s.Primary {
		n := len(a.Decisions)
		add(shardFix{key: "replicas/" + table, cause: replicasCause(table, replicas, n),
			stmt: fmt.Sprintf("ALTER TABLE %s SET (number_of_replicas = %d)", quoteTable(s.SchemaName, s.TableName), n-1)})
	}
	if len(watermarkNodes) > 0 {
		sort.Strings(watermarkNodes)
		add(shardFix{key: "watermark", cause: "past a disk watermark: " + strings.Join(watermarkNodes, ", ") + " (free disk or add nodes)"})
	}
	if strings.Contains(all, "for the departed node") {
		add(shardFix{key: "delayed", cause: "waiting for the node that left to come back (unassigned.node_left.delayed_timeout)"})
	}
	if strings.Contains(all, "reached the limit of incoming shard recoveries") || strings.Contains(all, "reached the limit of outgoing shard recoveries") {
		add(shardFix{key: "throttled", cause: "waiting for a recovery slot (node_concurrent_recoveries)"})
	}
	return fixes
}

// stuckFix explains a STARTED copy that a rule sends away from its node
// while no other node can take it. CrateDB gives no per-node decisions for
// these, so the table's own allocation filters are the only lead.
func stuckFix(a cratedb.AllocationInfo, filters map[string]string, node string) shardFix {
	table := a.TableSchema + "." + a.TableName
	f := shardFix{key: "stuck/" + table}
	if len(filters) == 0 {
		f.cause = fmt.Sprintf("%s can't stay on %s and no other node takes it (cluster allocation filter or high watermark?)", table, node)
		return f
	}
	keys := make([]string, 0, len(filters))
	for k := range filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var set, quoted []string
	for _, k := range keys {
		set = append(set, k+" = "+filters[k])
		quoted = append(quoted, `"`+k+`"`)
	}
	f.cause = fmt.Sprintf("%s can't stay on %s and no other node takes it: %s (reset it, lower number_of_replicas or add a node)",
		table, node, strings.Join(set, ", "))
	f.stmt = fmt.Sprintf("ALTER TABLE %s RESET (%s)", quoteTable(a.TableSchema, a.TableName), strings.Join(quoted, ", "))
	return f
}

// diagnose folds the per-shard fixes into one line per cause, most shards
// first.
func diagnose(fixesPerShard [][]shardFix) []shardDiagnosis {
	idx := map[string]int{}
	var out []shardDiagnosis
	for _, fixes := range fixesPerShard {
		for _, f := range fixes {
			i, ok := idx[f.key]
			if !ok {
				i = len(out)
				idx[f.key] = i
				out = append(out, shardDiagnosis{shardFix: f})
			}
			out[i].shards++
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].shards > out[j].shards })
	return out
}

// replicasCause spells out the arithmetic: CrateDB puts at most one copy of
// a shard on a node.
func replicasCause(table, replicas string, nodes int) string {
	if r, err := strconv.Atoi(replicas); err == nil {
		return fmt.Sprintf("%s: %d replicas + primary = %d copies per shard, but only %d nodes (one copy per node)", table, r, r+1, nodes)
	}
	if replicas != "" {
		return fmt.Sprintf("%s: number_of_replicas = %s needs more than %d nodes (one copy per node)", table, replicas, nodes)
	}
	return fmt.Sprintf("%s: more copies per shard than %d nodes (one copy per node)", table, nodes)
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

const maxDiagnoses = 4

// RunFixMsg asks the App to run a suggested fix.
type RunFixMsg struct{ Stmt string }

// ShardNoticeMsg reports the outcome of y or x on the Shards tab.
type ShardNoticeMsg struct {
	Text  string
	IsErr bool
	Ran   bool // a fix ran, refresh the shards
}

// chosenFix is what y and x act on: the selected shard's first runnable
// fix, else the top diagnosis that has one.
func (m ShardsModel) chosenFix() (shardFix, bool) {
	if m.selected < len(m.sorted) {
		for _, f := range m.fixes[m.sorted[m.selected]] {
			if f.stmt != "" {
				return f, true
			}
		}
	}
	for _, d := range m.diagnoses {
		if d.stmt != "" {
			return d.shardFix, true
		}
	}
	return shardFix{}, false
}

func (m ShardsModel) handleFixKey(msg tea.KeyMsg) (ShardsModel, tea.Cmd, bool) {
	if m.fixTarget != nil {
		switch msg.String() {
		case "y", "enter":
			stmt := m.fixTarget.stmt
			m.fixTarget = nil
			return m, func() tea.Msg { return RunFixMsg{Stmt: stmt} }, true
		case "n", "esc":
			m.fixTarget = nil
		}
		return m, nil, true
	}
	switch {
	case key.Matches(msg, m.keyMap.Yank):
		f, ok := m.chosenFix()
		if !ok {
			m = m.notice("no fix to copy", true)
			return m, nil, true
		}
		return m, func() tea.Msg {
			if err := writeClipboard(f.stmt + ";\n"); err != "" {
				return ShardNoticeMsg{Text: "copy failed: " + err, IsErr: true}
			}
			return ShardNoticeMsg{Text: "copied: " + f.stmt}
		}, true
	case key.Matches(msg, m.keyMap.RunFix):
		f, ok := m.chosenFix()
		switch {
		case !ok:
			m = m.notice("no fix to run", true)
		case m.readOnly && !f.readOnlyOK:
			m = m.notice(readOnlyRefusal("y copies it"), true)
		default:
			m.fixTarget = &f
		}
		return m, nil, true
	}
	return m, nil, false
}

func (m ShardsModel) notice(text string, isErr bool) ShardsModel {
	m.noticeText, m.noticeIsErr, m.noticeAt = text, isErr, time.Now()
	return m
}

// renderDiagnoses is the block under the verdict: one line per cause, the
// fix indented below it.
func (m ShardsModel) renderDiagnoses() []string {
	var lines []string
	for i, d := range m.diagnoses {
		if i == maxDiagnoses {
			lines = append(lines, styleDim.Render(fmt.Sprintf("  … %d more causes, see the detail panel", len(m.diagnoses)-i)))
			break
		}
		style := styleHealthYellow
		if d.stmt == "" {
			style = styleDim
		}
		lines = append(lines, style.Render(fmt.Sprintf("  ⚠ %d shard%s: %s", d.shards, plural(d.shards, "", "s"), d.cause)))
		if d.stmt != "" {
			lines = append(lines, "      fix: "+styleValue.Render(d.stmt))
		}
	}
	if m.noticeText != "" {
		style := styleHealthGreen
		if m.noticeIsErr {
			style = styleHealthRed
		}
		lines = append(lines, "  "+style.Render(m.noticeText))
	}
	return lines
}

func (m ShardsModel) renderFixModal() string {
	inner := modalInnerWidth(m.width, 70, 50)
	content := strings.Join([]string{
		styleModalTitle.Render("Run this statement?"),
		"",
		styleValue.Render(m.fixTarget.stmt),
		"",
		styleDim.Render(m.fixTarget.cause),
		"",
		styleDim.Render("[y]es  [n]o"),
	}, "\n")
	return placeModal(content, inner, m.width, m.height)
}
