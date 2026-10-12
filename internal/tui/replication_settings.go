package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// replSetting is a cluster setting the subscriber reads, shown and edited
// on the Replication tab.
type replSetting struct {
	path, short, def, meaning string
}

var replSettings = []replSetting{
	{"replication.logical.ops_batch_size", "ops_batch_size", "50000", "operations fetched per poll, per shard"},
	{"replication.logical.reads_poll_duration", "reads_poll_duration", "50ms", "pause between two polls of a shard"},
	{"replication.logical.recovery.chunk_size", "chunk_size", "1mb", "file chunk size of the initial copy"},
	{"replication.logical.recovery.max_concurrent_file_chunks", "max_concurrent_file_chunks", "2", "chunks in flight per shard during the initial copy (1-5)"},
	{"cluster.routing.allocation.node_initial_primaries_recoveries", "node_initial_primaries_recoveries", "4", "shards a node copies at once; also snapshot restores and new tables"},
}

const leasePeriodSetting = "soft_deletes.retention_lease.period"

// defaultLeasePeriod is CrateDB's soft_deletes.retention_lease.period.
const defaultLeasePeriod = 12 * time.Hour

func (m ReplicationModel) settingValue(s replSetting) (string, bool) {
	v, ok := m.repl.Settings[s.path]
	return v, ok && !strings.EqualFold(v, s.def)
}

// renderReplSettings shows the subscriber's settings on one line, the
// non-defaults highlighted. Shown on a subscriber, or anywhere one was
// changed.
func (m ReplicationModel) renderReplSettings() []string {
	var parts []string
	changed := false
	for _, s := range replSettings {
		v, nonDefault := m.settingValue(s)
		if v == "" {
			continue
		}
		part := s.short + " " + v
		if nonDefault {
			part = styleHealthYellow.Render(part + " (default " + s.def + ")")
			changed = true
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 || (!changed && len(m.repl.Subscriptions) == 0) {
		return nil
	}
	lines := []string{sectionTitle("Subscriber settings")}
	for _, l := range wrapText(strings.Join(parts, " · "), max(m.width-4, 40)) {
		lines = append(lines, "  "+l)
	}
	return append(lines, styleDim.Render("  c change (cluster settings, read where the subscription runs)"))
}

// leasePeriodLine is the lease periods of a publication's tables: the
// common one, and the tables that differ, flagged under shortLeasePeriod
// and under reads_poll_duration.
func (m ReplicationModel) leasePeriodLine(p cratedb.Publication) string {
	var odd []string
	known := 0
	poll, _ := parseDelay(m.repl.Settings["replication.logical.reads_poll_duration"])
	for _, t := range p.Tables {
		d, ok := m.repl.LeasePeriods[t]
		if !ok {
			continue
		}
		known++
		if d == defaultLeasePeriod {
			continue
		}
		cell := t + " " + shortDelay(d)
		switch {
		case poll > 0 && d < poll:
			cell = styleHealthRed.Render(cell + " ▲ under reads_poll_duration")
		case d < shortLeasePeriod:
			cell = styleHealthYellow.Render(cell + " ▲ under 1h")
		}
		odd = append(odd, cell)
	}
	switch {
	case known == 0:
		return ""
	case len(odd) == 0:
		return styleDim.Render("    retention lease period 12h (default) on all")
	}
	line := "    retention lease period: " + strings.Join(odd, ", ")
	if known > len(odd) {
		line += styleDim.Render(", others 12h")
	}
	return line
}

// renderPool is the logical_replication thread pool over the nodes: one
// summary line, then the nodes with a queue or rejections.
func (m ReplicationModel) renderPool() []string {
	var nodes, threads, active, queue, rejected int64
	var busy []string
	for _, n := range m.nodes {
		for _, p := range n.ThreadPools {
			if p.Name != "logical_replication" {
				continue
			}
			nodes++
			threads += p.Threads
			active += p.Active
			queue += p.Queue
			rejected += p.Rejected
			if p.Queue > 0 || p.Rejected > 0 {
				cell := fmt.Sprintf("%s queue %d, rejected %d", shortenHostname(n.Name, 24), p.Queue, p.Rejected)
				if p.Rejected > 0 {
					cell = styleHealthRed.Render(cell)
				} else {
					cell = styleHealthYellow.Render(cell)
				}
				busy = append(busy, cell)
			}
		}
	}
	if nodes == 0 {
		return nil
	}
	lines := []string{fmt.Sprintf("  logical_replication thread pool: %d nodes, %d threads, %d active, queue %d, rejected %d (since node start)",
		nodes, threads, active, queue, rejected)}
	if len(busy) > 0 {
		lines = append(lines, "    "+strings.Join(busy, " · "))
	}
	return lines
}

// ReplSettingRequest asks the App to SET or RESET a replication setting.
type ReplSettingRequest struct {
	Path, Value string
	Persistent  bool
}

func runReplSetting(ctx context.Context, reg *cratedb.Registry, req ReplSettingRequest) ReplicationNoticeMsg {
	res := applySetting(ctx, reg, SetSettingMsg{SettingPath: req.Path, Value: req.Value, Persistent: req.Persistent})
	if res.Error != "" {
		return ReplicationNoticeMsg{Error: firstLine(res.Error)}
	}
	note := req.Path + " = " + req.Value
	if res.Note != "" {
		note += "; " + res.Note
	}
	return ReplicationNoticeMsg{Note: note, Ran: true}
}

// replSettingsForm edits the subscriber settings: pick one, type a value,
// or x for the default.
type replSettingsForm struct {
	cursor     int
	editing    bool
	buf        string
	values     map[string]string
	persistent bool
}

func (f *replSettingsForm) key(msg tea.KeyMsg) (done bool, cmd tea.Cmd) {
	s := replSettings[f.cursor]
	if f.editing {
		switch msg.Type {
		case tea.KeyEsc:
			f.editing = false
		case tea.KeyEnter:
			val := strings.TrimSpace(f.buf)
			if val == "" || val == f.values[s.path] {
				f.editing = false
				return false, nil
			}
			req := ReplSettingRequest{Path: s.path, Value: val, Persistent: f.persistent}
			return true, func() tea.Msg { return req }
		case tea.KeyBackspace:
			if f.buf != "" {
				f.buf = f.buf[:len(f.buf)-1]
			}
		case tea.KeyRunes:
			f.buf += string(msg.Runes)
		}
		return false, nil
	}
	switch msg.String() {
	case "esc", "q":
		return true, nil
	case "up", "k":
		f.cursor = max(f.cursor-1, 0)
	case "down", "j":
		f.cursor = min(f.cursor+1, len(replSettings)-1)
	case "enter":
		f.editing, f.buf = true, f.values[s.path]
	case "x":
		apply := ApplyReplicationMsg{Stmt: fmt.Sprintf(`RESET GLOBAL "%s"`, s.path), Done: s.path + " reset to " + s.def}
		return true, func() tea.Msg { return apply }
	}
	return false, nil
}

func (f *replSettingsForm) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 85, 60)
	mode := "PERSISTENT"
	if !f.persistent {
		mode = "TRANSIENT"
	}
	lines := []string{styleModalTitle.Render("Replication settings (SET GLOBAL " + mode + ")"), ""}
	nameW := 0
	for _, s := range replSettings {
		nameW = max(nameW, len(s.short))
	}
	for i, s := range replSettings {
		v := f.values[s.path]
		if f.editing && i == f.cursor {
			v = styleEditInput.Render(f.buf + "▏")
		} else if !strings.EqualFold(v, s.def) {
			v = styleHealthYellow.Render(v)
		}
		row := fmt.Sprintf("%-*s  %s", nameW, s.short, v)
		if i == f.cursor {
			lines = append(lines, stylePickerActive.Render("▸ ")+row)
			lines = append(lines, styleDim.Render(fmt.Sprintf("    %s · default %s", s.meaning, s.def)))
		} else {
			lines = append(lines, "  "+row)
		}
	}
	lines = append(lines, "", styleDim.Render("[↑↓] select   [Enter] edit, again to set   [x] reset to default   [Esc] close"))
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
}

// leaseForm changes soft_deletes.retention_lease.period of one table of a
// publication: pick, type, confirm.
type leaseForm struct {
	pub        string
	tables     []string
	periods    map[string]time.Duration
	cursor     int
	editing    bool
	confirm    bool
	reset      bool
	buf        string
	poll       time.Duration
	maxSilence time.Duration
	silentSub  string
	err        string
}

func (m ReplicationModel) newLeaseForm(p cratedb.Publication) *leaseForm {
	f := &leaseForm{pub: p.Name, tables: p.Tables, periods: m.repl.LeasePeriods}
	f.poll, _ = parseDelay(m.repl.Settings["replication.logical.reads_poll_duration"])
	mine := map[string]bool{}
	for _, t := range p.Tables {
		mine[t] = true
	}
	for _, h := range m.repl.Subscribers {
		pulls := false
		for _, t := range h.On {
			pulls = pulls || mine[t]
		}
		if pulls && h.MaxSilence > f.maxSilence {
			f.maxSilence, f.silentSub = h.MaxSilence, h.Subscriber
		}
	}
	return f
}

func (f *leaseForm) period(t string) string {
	if d, ok := f.periods[t]; ok {
		return shortDelay(d)
	}
	return "?"
}

func (f *leaseForm) stmt() (string, []interface{}) {
	t := quoteFQN(f.tables[f.cursor])
	if f.reset {
		return fmt.Sprintf(`ALTER TABLE %s RESET ("%s")`, t, leasePeriodSetting), nil
	}
	return fmt.Sprintf(`ALTER TABLE %s SET ("%s" = ?)`, t, leasePeriodSetting), []interface{}{strings.TrimSpace(f.buf)}
}

func (f *leaseForm) key(msg tea.KeyMsg) (done bool, cmd tea.Cmd) {
	f.err = ""
	switch {
	case f.confirm:
		switch msg.String() {
		case "y":
			stmt, args := f.stmt()
			t := f.tables[f.cursor]
			done := fmt.Sprintf("%s of %s set to %s", leasePeriodSetting, t, strings.TrimSpace(f.buf))
			if f.reset {
				done = fmt.Sprintf("%s of %s reset to 12h", leasePeriodSetting, t)
			}
			apply := ApplyReplicationMsg{Stmt: stmt, Args: args, Done: done, Periods: true}
			return true, func() tea.Msg { return apply }
		case "n", "esc":
			f.confirm, f.reset = false, false
		}
	case f.editing:
		switch msg.Type {
		case tea.KeyEsc:
			f.editing = false
		case tea.KeyEnter:
			d, ok := parseDelay(f.buf)
			switch {
			case !ok:
				f.err = fmt.Sprintf("%q isn't a time value, e.g. 12h or 30m", strings.TrimSpace(f.buf))
			case d <= 0:
				f.err = "the period must be positive"
			default:
				f.editing, f.confirm = false, true
			}
		case tea.KeyBackspace:
			if f.buf != "" {
				f.buf = f.buf[:len(f.buf)-1]
			}
		case tea.KeyRunes:
			f.buf += string(msg.Runes)
		}
	default:
		switch msg.String() {
		case "esc", "q":
			return true, nil
		case "up", "k":
			f.cursor = max(f.cursor-1, 0)
		case "down", "j":
			f.cursor = min(f.cursor+1, len(f.tables)-1)
		case "enter":
			if len(f.tables) > 0 {
				f.editing, f.buf = true, f.period(f.tables[f.cursor])
			}
		case "x":
			if len(f.tables) > 0 {
				f.reset, f.confirm = true, true
			}
		}
	}
	return false, nil
}

func (f *leaseForm) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 80, 60)
	textW := innerWidth - 4
	lines := []string{styleModalTitle.Render("Retention lease period · publication " + f.pub), ""}
	note := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, textW) {
			lines = append(lines, style.Render(l))
		}
	}
	if f.confirm {
		stmt, args := f.stmt()
		if len(args) > 0 {
			stmt = strings.Replace(stmt, "?", "'"+fmt.Sprint(args[0])+"'", 1)
		}
		lines = append(lines, "Runs:", "  "+styleValue.Render(stmt+";"), "")
		note(lipgloss.NewStyle(), "How long a subscriber may go without pulling before the publisher may drop operations it still needs; it then can't catch up. Meanwhile the table keeps its soft-deleted documents on disk, so a longer period costs space on tables with many updates and deletes.")
		note(styleDim, "ALTER TABLE without ONLY: a partitioned table's existing and future partitions get it too.")
		if d, ok := parseDelay(f.buf); ok && !f.reset {
			if f.poll > 0 && d < f.poll {
				note(styleHealthRed, "Shorter than reads_poll_duration: a lease can expire between two polls.")
			} else if d < shortLeasePeriod {
				note(styleHealthYellow, "Under an hour: a subscriber restart or a short outage can outlast it.")
			}
			if f.maxSilence > d {
				note(styleHealthYellow, fmt.Sprintf("Subscriber %s already went %s without pulling while obsi ran.", f.silentSub, formatDuration(f.maxSilence)))
			}
		}
		lines = append(lines, "", styleDim.Render("[y] apply   [n] back"))
		return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
	}
	for i, t := range f.tables {
		v := f.period(t)
		if f.editing && i == f.cursor {
			v = styleEditInput.Render(f.buf + "▏")
		} else if d, ok := f.periods[t]; ok && d != defaultLeasePeriod {
			v = styleHealthYellow.Render(v)
		}
		row := t + "  " + v
		if i == f.cursor {
			lines = append(lines, stylePickerActive.Render("▸ ")+row)
		} else {
			lines = append(lines, "  "+row)
		}
	}
	lines = append(lines, "")
	if f.maxSilence > 0 {
		note(styleDim, fmt.Sprintf("Longest a subscriber went without pulling while obsi ran: %s (%s).", formatDuration(f.maxSilence), f.silentSub))
	}
	note(styleDim, "? = not read yet. Default 12h.")
	lines = append(lines, "", styleDim.Render("[↑↓] table   [Enter] edit, again to review   [x] reset to 12h   [Esc] close"))
	if f.err != "" {
		lines = append(lines, "", styleHealthRed.Render(f.err))
	}
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
}

// poolNodes is what renderPool needs from the snapshot.
func poolNodes(nodes []store.NodeSnapshot) []store.NodeSnapshot {
	var out []store.NodeSnapshot
	for _, n := range nodes {
		if !n.Gone {
			out = append(out, n)
		}
	}
	return out
}
