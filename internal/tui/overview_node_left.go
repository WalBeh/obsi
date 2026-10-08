package tui

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// unassigned.node_left.delayed_timeout is per table and per partition
// (each partition keeps its own copy), there's no cluster-wide value, and
// the default is 1m. A node away longer than that, e.g. a pod restart,
// gets its replicas rebuilt elsewhere and then moved back.
const nodeLeftSetting = "unassigned.node_left.delayed_timeout"

const defaultNodeLeftDelay = time.Minute

var delayValueRe = regexp.MustCompile(`^(\d+)(ms|s|m|h|d)$`)

// parseDelay reads CrateDB time values like 5m or 300s; ok is false for
// anything else, so a typo doesn't turn into one failed ALTER per table.
func parseDelay(v string) (time.Duration, bool) {
	m := delayValueRe.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	unit := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour}[m[2]]
	return time.Duration(n) * unit, true
}

func shortDelay(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	}
	return d.String()
}

// longestAbsence is the longest time a node was away while obsi ran.
func longestAbsence(abs []store.NodeAbsence) (store.NodeAbsence, bool) {
	var best store.NodeAbsence
	for _, a := range abs {
		if a.Duration() > best.Duration() {
			best = a
		}
	}
	return best, best.Name != ""
}

// suggestDelay is half again the longest absence, rounded up to a minute,
// at least 5m.
func suggestDelay(abs []store.NodeAbsence) time.Duration {
	d := 5 * time.Minute
	if a, ok := longestAbsence(abs); ok {
		want := (a.Duration()*3/2 + time.Minute - 1) / time.Minute * time.Minute
		d = max(d, want)
	}
	return d
}

// nodeLeftDelays groups tables by the shortest delay they have (table or
// partition). mixed counts tables whose older partitions are shorter than
// the table's own value.
type delayGroup struct {
	delay         time.Duration
	tables, mixed int
}

func nodeLeftDelays(tables []cratedb.TableInfo) []delayGroup {
	by := map[time.Duration]*delayGroup{}
	for _, t := range tables {
		d := t.NodeLeftDelayMin
		if d == 0 {
			continue // not read (fallback without information_schema)
		}
		g := by[d]
		if g == nil {
			g = &delayGroup{delay: d}
			by[d] = g
		}
		g.tables++
		if t.PartitionsBelowDelay > 0 {
			g.mixed++
		}
	}
	out := make([]delayGroup, 0, len(by))
	for _, g := range by {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].delay < out[j].delay })
	return out
}

// renderNodeLeft is the Overview line: delays by table, blob tables, the
// longest absence seen, and a warning when a node was away longer than
// some table waits.
func (m OverviewModel) renderNodeLeft() string {
	groups := nodeLeftDelays(m.snap.Tables)
	if len(groups) == 0 && m.snap.BlobTables == 0 {
		return ""
	}
	var parts []string
	for _, g := range groups {
		p := fmt.Sprintf("%s on %d %s", shortDelay(g.delay), g.tables, plural(g.tables, "table", "tables"))
		if g.mixed > 0 {
			p += fmt.Sprintf(" (%d via older partitions)", g.mixed)
		}
		parts = append(parts, p)
	}
	if m.snap.BlobTables > 0 {
		parts = append(parts, fmt.Sprintf("%d blob %s at 1m, fixed", m.snap.BlobTables, plural(m.snap.BlobTables, "table", "tables")))
	}
	shortest := defaultNodeLeftDelay
	if len(groups) > 0 {
		shortest = groups[0].delay
	}
	line := "  Node-left delay: " + m.editor.renderValue(slotNodeLeftDelay, shortDelay(shortest)) + " │ " + strings.Join(parts, " · ")
	if a, ok := longestAbsence(m.snap.NodeAbsences); ok {
		away := fmt.Sprintf(" │ longest node away %s (%s, %s)", formatDuration(a.Duration().Round(time.Second)), a.Name, a.Left.Format("15:04"))
		if a.Duration() > shortest {
			return line + styleHealthYellow.Render(away+" ▲ longer than the delay: replicas were rebuilt")
		}
		line += styleDim.Render(away)
	}
	return line
}

// delayConfirm is the confirm before setting the delay on every table.
type delayConfirm struct {
	value   string
	targets []delayTarget
	blob    int
}

type delayTarget struct {
	schema, name string
	note         string // what changes besides the plain value, if anything
}

func (t delayTarget) stmt(value string) string {
	return fmt.Sprintf(`ALTER TABLE %s.%s SET ("%s" = '%s');`, quoteIdent(t.schema), quoteIdent(t.name), nodeLeftSetting, value)
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// NodeLeftDelayRequest asks the Overview to confirm a delay for all tables.
type NodeLeftDelayRequest struct{ Value string }

// ApplyNodeLeftDelayMsg runs one ALTER TABLE per table. Plain ALTER TABLE,
// not ONLY, so existing and future partitions get the value too.
type ApplyNodeLeftDelayMsg struct {
	Value  string
	Tables [][2]string // schema, name
	Blob   int
}

func (m OverviewModel) openDelayConfirm(value string) OverviewModel {
	c := &delayConfirm{value: value, blob: m.snap.BlobTables}
	want, _ := parseDelay(value)
	for _, t := range m.snap.Tables {
		tg := delayTarget{schema: t.SchemaName, name: t.TableName}
		switch {
		case t.PartitionsBelowDelay > 0:
			tg.note = fmt.Sprintf("%d partitions at %s, overwritten", t.PartitionsBelowDelay, shortDelay(t.NodeLeftDelayMin))
		case want > 0 && t.Settings.NodeLeftDelay > want:
			tg.note = fmt.Sprintf("lowered from %s", shortDelay(t.Settings.NodeLeftDelay))
		}
		c.targets = append(c.targets, tg)
	}
	m.delayConfirm = c
	return m
}

func (m OverviewModel) handleDelayConfirmKey(msg tea.KeyMsg) (OverviewModel, tea.Cmd) {
	c := m.delayConfirm
	switch msg.String() {
	case "y", "enter":
		m.delayConfirm = nil
		apply := ApplyNodeLeftDelayMsg{Value: c.value, Blob: c.blob}
		for _, t := range c.targets {
			apply.Tables = append(apply.Tables, [2]string{t.schema, t.name})
		}
		return m, func() tea.Msg { return apply }
	case "c":
		var b strings.Builder
		for _, t := range c.targets {
			b.WriteString(t.stmt(c.value) + "\n")
		}
		payload := b.String()
		m.delayConfirm = nil
		return m, func() tea.Msg {
			return SetSettingResultMsg{SlotIndex: slotNodeLeftDelay, Note: copyNote(writeClipboard(payload), len(c.targets))}
		}
	case "n", "esc":
		m.delayConfirm = nil
	}
	return m, nil
}

func copyNote(err string, n int) string {
	if err != "" {
		return "copy failed: " + err
	}
	return fmt.Sprintf("copied %d ALTER TABLE statements", n)
}

func (m OverviewModel) renderDelayConfirm() string {
	c := m.delayConfirm
	innerWidth := modalInnerWidth(m.width, 80, 50)
	lines := []string{
		styleModalTitle.Render(fmt.Sprintf("Set %s = %s on %d tables?", nodeLeftSetting, c.value, len(c.targets))),
		"",
		"One ALTER TABLE per table, without ONLY: existing and future partitions get it too.",
		"Replicas then wait this long for a node that left before being rebuilt elsewhere;",
		"a node really gone means running with one copy less for that time.",
	}
	var notes []string
	for _, t := range c.targets {
		if t.note != "" {
			notes = append(notes, fmt.Sprintf("  %s.%s: %s", t.schema, t.name, t.note))
		}
	}
	if len(notes) > 0 {
		lines = append(lines, "", styleHealthYellow.Render("Changes more than the plain value:"))
		lines = append(lines, notes[:min(len(notes), 8)]...)
		if len(notes) > 8 {
			lines = append(lines, styleDim.Render(fmt.Sprintf("  … %d more", len(notes)-8)))
		}
	}
	if c.blob > 0 {
		lines = append(lines, "", styleDim.Render(fmt.Sprintf("Skipped: %d blob tables, SQL can't change it there (they keep 1m).", c.blob)))
	}
	lines = append(lines, "", styleDim.Render("[y] apply   [c] copy the statements   [n] cancel"))
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, m.width, m.height)
}

// syncNodeLeft shows the shortest delay in the slot and keeps the
// suggestion for the edit hint.
func (e *settingsEditor) syncNodeLeft(snap store.StoreSnapshot) {
	shortest := defaultNodeLeftDelay
	if g := nodeLeftDelays(snap.Tables); len(g) > 0 {
		shortest = g[0].delay
	}
	if !(e.inputActive && e.cursor == slotNodeLeftDelay) {
		e.slots[slotNodeLeftDelay].value = shortDelay(shortest)
	}
	e.nodeLeftHint = "suggested " + shortDelay(suggestDelay(snap.NodeAbsences))
	if a, ok := longestAbsence(snap.NodeAbsences); ok {
		e.nodeLeftHint += fmt.Sprintf(": longest node away %s, plus half, at least 5m", formatDuration(a.Duration().Round(time.Second)))
	} else {
		e.nodeLeftHint += ": no node restart seen yet, above your usual restart time"
	}
}

// applyNodeLeftDelay runs the ALTER TABLEs one by one and stops at the first
// error, saying how far it got.
func applyNodeLeftDelay(ctx context.Context, reg *cratedb.Registry, msg ApplyNodeLeftDelayMsg) SetSettingResultMsg {
	res := SetSettingResultMsg{SlotIndex: slotNodeLeftDelay}
	stmt := `ALTER TABLE %s.%s SET ("` + nodeLeftSetting + `" = ?)`
	for i, t := range msg.Tables {
		if _, err := reg.Query(ctx, fmt.Sprintf(stmt, quoteIdent(t[0]), quoteIdent(t[1]))+cratedb.ChangeTag, msg.Value); err != nil {
			res.Error = fmt.Sprintf("set on %d of %d tables, then %s.%s: %v", i, len(msg.Tables), t[0], t[1], err)
			return res
		}
	}
	res.Note = fmt.Sprintf("%s set on %d tables", msg.Value, len(msg.Tables))
	if msg.Blob > 0 {
		res.Note += fmt.Sprintf("; %d blob tables keep 1m", msg.Blob)
	}
	return res
}
