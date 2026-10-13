package tui

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// SnapshotsModel is the Snapshots tab: the repositories, running restores
// and every snapshot sys.snapshots lists. The selected repository or
// snapshot opens in place with its details.
type SnapshotsModel struct {
	st        store.SnapshotsState
	nodeNames map[string]string // node ID → name, for the failures
	now       time.Time
	width     int
	height    int
	scroll    int
	maxScroll int
	selected  int // repositories first, then the shown snapshots
	searching bool
	search    string // matched anywhere in repository.name
	repo      string // enter on a repository: only its snapshots

	noticeText  string
	noticeIsErr bool
	noticeAt    time.Time
}

func NewSnapshotsModel(width, height int) SnapshotsModel {
	return SnapshotsModel{width: width, height: height}
}

func (m SnapshotsModel) Refresh(snap store.StoreSnapshot) SnapshotsModel {
	m.st, m.now = snap.Snapshots, time.Now()
	m.nodeNames = map[string]string{}
	for _, n := range snap.Nodes {
		m.nodeNames[n.ID] = n.Name
	}
	m.selected = min(m.selected, max(m.items()-1, 0))
	return m.clampScroll()
}

func (m SnapshotsModel) SetSize(width, height int) SnapshotsModel {
	m.width, m.height = width, height
	return m.clampScroll()
}

func (m SnapshotsModel) inputMode() bool { return m.searching }

func (m SnapshotsModel) notice(msg SnapshotsNoticeMsg) SnapshotsModel {
	m.noticeText, m.noticeIsErr, m.noticeAt = msg.Note, msg.Error != "", time.Now()
	if msg.Error != "" {
		m.noticeText = msg.Error
	}
	return m
}

func (m SnapshotsModel) noticeLine() string {
	if m.noticeText == "" || time.Since(m.noticeAt) > 8*time.Second {
		return ""
	}
	if m.noticeIsErr {
		return "  " + styleHealthRed.Render(m.noticeText)
	}
	return "  " + styleHealthGreen.Render(m.noticeText)
}

// selectedSnapshot is the snapshot under the cursor and its repository.
func (m SnapshotsModel) selectedSnapshot() (cratedb.SnapshotInfo, cratedb.Repository, bool) {
	i := m.selected - len(m.st.Repositories)
	shown := m.shown()
	if i < 0 || i >= len(shown) {
		return cratedb.SnapshotInfo{}, cratedb.Repository{}, false
	}
	s := shown[i]
	for _, r := range m.st.Repositories {
		if r.Name == s.Repository {
			return s, r, true
		}
	}
	return s, cratedb.Repository{Name: s.Repository}, true
}

// shown is the snapshots the filter lets through, newest first.
func (m SnapshotsModel) shown() []cratedb.SnapshotInfo {
	if m.search == "" && m.repo == "" {
		return m.st.Snapshots
	}
	q := strings.ToLower(m.search)
	var out []cratedb.SnapshotInfo
	for _, s := range m.st.Snapshots {
		if (m.repo == "" || s.Repository == m.repo) && strings.Contains(strings.ToLower(s.Repository+"."+s.Name), q) {
			out = append(out, s)
		}
	}
	return out
}

func (m SnapshotsModel) items() int { return len(m.st.Repositories) + len(m.shown()) }

func (m SnapshotsModel) HandleKey(msg tea.KeyMsg) (SnapshotsModel, tea.Cmd) {
	if m.searching {
		switch msg.Type {
		case tea.KeyEsc:
			m.searching, m.search = false, ""
		case tea.KeyEnter:
			m.searching = false
		case tea.KeyBackspace:
			if r := []rune(m.search); len(r) > 0 {
				m.search = string(r[:len(r)-1])
			}
		case tea.KeyRunes:
			m.search += string(msg.Runes)
		}
		m.selected = min(m.selected, max(m.items()-1, 0))
		return m.clampScroll().followSelection(), nil
	}
	page := max(m.height-2, 1)
	switch msg.String() {
	case "/":
		m.searching, m.search = true, ""
		return m.clampScroll(), nil
	case "esc":
		m.search, m.repo = "", ""
		return m.clampScroll(), nil
	case "y":
		s, repo, ok := m.selectedSnapshot()
		if !ok {
			return m.notice(SnapshotsNoticeMsg{Error: "select a snapshot (↓) to copy its details"}), nil
		}
		return m, func() tea.Msg { return SnapshotReportRequest{Snapshot: s, Repository: repo} }
	case "enter":
		// On a repository: show only its snapshots.
		if m.selected < len(m.st.Repositories) {
			m.repo = m.st.Repositories[m.selected].Name
			m = m.clampScroll()
			if len(m.shown()) > 0 {
				m.selected = len(m.st.Repositories)
			}
			return m.followSelection(), nil
		}
		return m, nil
	case "up", "k":
		if m.selected > 0 {
			m.selected--
			return m.clampScroll().followSelection(), nil
		}
		m.scroll--
	case "down", "j":
		if m.selected < m.items()-1 {
			m.selected++
			return m.clampScroll().followSelection(), nil
		}
		m.scroll++
	case "pgup":
		m.scroll -= page
	case "pgdown":
		m.scroll += page
	case "home":
		m.scroll = 0
	case "end":
		m.scroll = m.maxScroll
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m, nil
}

// bodyHeight leaves the last line for notices.
func (m SnapshotsModel) bodyHeight() int { return max(m.height-1, 3) }

func (m SnapshotsModel) clampScroll() SnapshotsModel {
	m.maxScroll = 0
	if n := len(m.render()); n > m.bodyHeight() {
		m.maxScroll = n - (m.bodyHeight() - 1)
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m
}

// followSelection scrolls the selected row, and what it opened, into view.
func (m SnapshotsModel) followSelection() SnapshotsModel {
	lines := m.render()
	for i, l := range lines {
		if !strings.Contains(l, "▸ ") {
			continue
		}
		end := i
		for end+1 < len(lines) && strings.HasPrefix(lines[end+1], detailIndent) {
			end++
		}
		// What's visible from a scroll position: the "more" markers take a
		// line each, as in body.
		last := func(scroll int) int {
			room := m.bodyHeight()
			if scroll > 0 {
				room--
			}
			if scroll+room < len(lines) {
				room--
			}
			return scroll + room - 1
		}
		if i <= m.scroll {
			m.scroll = max(i-1, 0)
		}
		// Keep the row itself (and one line above it) in view.
		for end > last(m.scroll) && m.scroll < i-1 {
			m.scroll++
		}
		break
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m
}

func (m SnapshotsModel) View() string { return m.body() + "\n" + m.noticeLine() }

func (m SnapshotsModel) body() string {
	lines := m.render()
	if len(lines) <= m.bodyHeight() {
		return strings.Join(lines, "\n")
	}
	room := m.bodyHeight()
	if m.scroll > 0 {
		room--
	}
	if m.scroll+room < len(lines) {
		room--
	}
	var out []string
	if m.scroll > 0 {
		out = append(out, styleDim.Render(fmt.Sprintf("  ↑ %d more lines", m.scroll)))
	}
	end := min(m.scroll+room, len(lines))
	out = append(out, lines[m.scroll:end]...)
	if end < len(lines) {
		out = append(out, styleDim.Render(fmt.Sprintf("  ↓ %d more lines", len(lines)-end)))
	}
	return strings.Join(out, "\n")
}

// detailIndent starts the lines a selected row opens.
const detailIndent = "      "

func (m SnapshotsModel) render() []string {
	return strings.Split(strings.Join(m.layout(), "\n"), "\n")
}

func (m SnapshotsModel) layout() []string {
	st := m.st
	var lines []string
	for _, e := range []struct{ what, err string }{{"sys.repositories", st.Err}, {"sys.snapshots", st.ListErr}} {
		if e.err == "" {
			continue
		}
		for _, l := range wrapText(fmt.Sprintf("couldn't read %s: %s", e.what, crateMessage(e.err)), max(m.width-4, 40)) {
			lines = append(lines, "  "+styleHealthRed.Render(l))
		}
	}
	switch {
	case st.UpdatedAt.IsZero():
		return append(lines, styleDim.Render("  waiting for the first snapshots poll"))
	case len(st.Repositories) == 0:
		return append(lines,
			"  "+styleHealthYellow.Render("No snapshot repository on this cluster: nothing is backed up by snapshots."),
			styleDim.Render("  CREATE REPOSITORY adds one (fs, s3, azure, gcs, url); CREATE SNAPSHOT then writes to it."))
	}
	lines = append(lines, m.renderRepositories()...)
	if len(st.Restores) > 0 {
		lines = append(lines, "", sectionTitle("Restores running"))
		for _, r := range st.Restores {
			l := fmt.Sprintf("  %s.%s  %s  %d/%d shards", r.Repository, r.Name, r.State, r.Done, r.Shards)
			if r.Failed > 0 {
				l += ", " + styleHealthRed.Render(fmt.Sprintf("%d failed", r.Failed))
			}
			lines = append(lines, l)
		}
	}
	lines = append(lines, "")
	lines = append(lines, m.renderSnapshotList()...)
	return append(lines, "", styleDim.Render("  ↑↓ select · enter on a repository: its snapshots · / filter · y copy snapshot details · R list snapshots now · ? help"))
}

// newestFinished is the newest snapshot per repository that isn't
// running, and how many each repository lists.
func (m SnapshotsModel) newestFinished() (map[string]cratedb.SnapshotInfo, map[string]int) {
	newest, count := map[string]cratedb.SnapshotInfo{}, map[string]int{}
	for _, s := range m.st.Snapshots {
		count[s.Repository]++
		if _, ok := newest[s.Repository]; !ok && s.State != "IN_PROGRESS" {
			newest[s.Repository] = s
		}
	}
	return newest, count
}

func (m SnapshotsModel) renderRepositories() []string {
	newest, count := m.newestFinished()
	nameW, lastW := len("REPOSITORY"), 0
	for _, r := range m.st.Repositories {
		nameW = max(nameW, len(r.Name))
		if s, ok := newest[r.Name]; ok {
			lastW = max(lastW, len(s.Name))
		}
	}
	nameW, lastW = min(nameW, 30), min(lastW, 30)
	row := fmt.Sprintf("%%s%%-%ds  %%-5s  %%-9s  %%9s  %%s", nameW)
	lines := []string{sectionTitle("Repositories"), styleHeader.Render(fmt.Sprintf(row, "  ", "NAME", "TYPE", "ACCESS", "SNAPSHOTS", "NEWEST"))}
	listed := !m.st.ListedAt.IsZero()
	for i, r := range m.st.Repositories {
		marker := "  "
		if i == m.selected {
			marker = "▸ "
		}
		access := "writable"
		if r.ReadOnly() {
			access = "read-only"
		}
		snaps, last := "?", styleDim.Render("not listed yet")
		if listed {
			snaps = fmt.Sprint(count[r.Name])
			last = styleHealthYellow.Render("none listed")
			if count[r.Name] > 0 {
				last = styleDim.Render("running, none finished")
			}
			if s, ok := newest[r.Name]; ok {
				last = fmt.Sprintf("%-*s  %s", lastW, truncateString(s.Name, lastW), snapshotStateStyle(s.State).Render(fmt.Sprintf("%-11s", s.State)))
				if !s.Finished.IsZero() {
					last += "  " + ageShort(m.now.Sub(s.Finished))
				}
			}
		}
		lines = append(lines, fmt.Sprintf(row, marker, truncateString(r.Name, nameW), r.Type, access, snaps, last))
		if i == m.selected {
			lines = append(lines, m.repositoryDetail(r, listed && count[r.Name] == 0)...)
		}
	}
	return lines
}

func (m SnapshotsModel) repositoryDetail(r cratedb.Repository, empty bool) []string {
	var lines []string
	for _, k := range slices.Sorted(maps.Keys(r.Settings)) {
		lines = append(lines, detailIndent+styleDim.Render(k+" = ")+r.Settings[k])
	}
	if len(r.Settings) == 0 {
		lines = append(lines, detailIndent+styleDim.Render("no settings"))
	}
	if r.Settings["access_key"] == "[xxxxx]" || r.Settings["key"] == "[xxxxx]" || r.Settings["private_key"] == "[xxxxx]" {
		lines = append(lines, detailIndent+styleDim.Render("[xxxxx]: CrateDB doesn't show credentials"))
	}
	if empty {
		for _, l := range wrapText("No snapshots listed: the repository is empty, or CrateDB couldn't read it (sys.snapshots leaves out a repository it can't read, without an error).", max(m.width-10, 40)) {
			lines = append(lines, detailIndent+styleHealthYellow.Render(l))
		}
	}
	return lines
}

func (m SnapshotsModel) renderSnapshotList() []string {
	st := m.st
	title := sectionTitle("Snapshots")
	if !st.ListedAt.IsZero() {
		title += styleDim.Render(fmt.Sprintf("  listed %s, took %s", ageShort(m.now.Sub(st.ListedAt)), formatDuration(st.ListTook.Round(time.Millisecond))))
		if st.ListEvery > 0 {
			title += styleDim.Render(fmt.Sprintf(" · every %s, sooner while one runs", shortDuration(st.ListEvery)))
		}
	}
	lines := []string{title}
	var filter []string
	if m.repo != "" {
		filter = append(filter, "repository "+m.repo)
	}
	switch {
	case m.searching:
		filter = append(filter, m.search+"▏")
	case m.search != "":
		filter = append(filter, fmt.Sprintf("%q", m.search))
	}
	if len(filter) > 0 {
		lines = append(lines, "  filter: "+strings.Join(filter, " · ")+"  "+styleDim.Render("(esc clears)"))
	}
	if st.ListedAt.IsZero() {
		return append(lines, styleDim.Render("  listing snapshots (reads every repository)..."))
	}
	shown := m.shown()
	if len(shown) == 0 {
		if m.search != "" || m.repo != "" {
			return append(lines, styleDim.Render("  no snapshot matches"))
		}
		return append(lines, styleHealthYellow.Render("  no snapshots"))
	}
	repoW, nameW := len("REPOSITORY"), len("NAME")
	for _, s := range shown {
		repoW = max(repoW, len(s.Repository))
		nameW = max(nameW, len(s.Name))
	}
	repoW, nameW = min(repoW, 24), min(nameW, 40)
	row := fmt.Sprintf("%%s%%-16s  %%-%ds  %%-%ds  %%s  %%8s  %%6s  %%6s  %%s", repoW, nameW)
	lines = append(lines, styleHeader.Render(fmt.Sprintf(row, "  ", "STARTED", "REPOSITORY", "NAME", fmt.Sprintf("%-11s", "STATE"), "TOOK", "SHARDS", "TABLES", "FAILURES")))
	base := len(st.Repositories)
	for i, s := range shown {
		marker := "  "
		if base+i == m.selected {
			marker = "▸ "
		}
		started, took := "—", "—"
		if !s.Started.IsZero() {
			started = s.Started.Local().Format("2006-01-02 15:04")
			switch {
			case !s.Finished.IsZero():
				took = snapshotTook(s.Finished.Sub(s.Started))
			case s.State == "IN_PROGRESS":
				took = snapshotTook(m.now.Sub(s.Started)) + "…"
			}
		}
		failures := ""
		if n := len(s.Failures); n > 0 {
			failures = styleHealthRed.Render(fmt.Sprint(n))
		}
		state := snapshotStateStyle(s.State).Render(fmt.Sprintf("%-11s", s.State))
		lines = append(lines, fmt.Sprintf(row, marker, started, truncateString(s.Repository, repoW), truncateString(s.Name, nameW),
			state, took, fmt.Sprint(s.TotalShards), fmt.Sprint(len(s.Tables)), failures))
		if base+i == m.selected {
			lines = append(lines, m.snapshotDetail(s)...)
		}
	}
	return lines
}

func (m SnapshotsModel) snapshotDetail(s cratedb.SnapshotInfo) []string {
	w := max(m.width-len(detailIndent)-2, 40)
	var lines []string
	add := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, w) {
			lines = append(lines, detailIndent+style.Render(l))
		}
	}
	if s.Started.IsZero() && s.State == "FAILED" {
		add(styleHealthYellow, "CrateDB couldn't read this snapshot's details from the repository: missing or unreadable files.")
	}
	var meta []string
	if !s.Finished.IsZero() {
		meta = append(meta, "finished "+s.Finished.Local().Format("2006-01-02 15:04:05"))
	}
	if s.Version != "" {
		meta = append(meta, "written by CrateDB "+s.Version)
	}
	if s.GlobalState {
		meta = append(meta, "with cluster metadata (settings, users, ...)")
	}
	if len(meta) > 0 {
		add(styleDim, strings.Join(meta, " · "))
	}
	if s.Reason != "" {
		add(styleHealthYellow, "reason: "+s.Reason)
	}
	if len(s.Tables) > 0 {
		add(lipgloss.NewStyle(), "tables: "+strings.Join(snapshotTables(s), ", "))
	}
	if len(s.Failures) > 0 {
		add(styleHealthRed, fmt.Sprintf("%d of %d shards failed:", len(s.Failures), s.TotalShards))
		for _, f := range s.Failures {
			add(lipgloss.NewStyle(), "  "+m.failureText(f))
		}
	}
	return lines
}

// snapshotTables names the tables with the partitions the snapshot has of
// each, e.g. doc.events (2 partitions: [a], [b]).
func snapshotTables(s cratedb.SnapshotInfo) []string {
	parts := map[string][]string{}
	for _, p := range s.Partitions {
		parts[p.Table] = append(parts[p.Table], "["+strings.Join(p.Values, ", ")+"]")
	}
	var out []string
	for _, t := range s.Tables {
		p := parts[t]
		switch {
		case len(p) == 0:
			out = append(out, t)
		case len(p) <= 3:
			out = append(out, fmt.Sprintf("%s (%d %s: %s)", t, len(p), plural(len(p), "partition", "partitions"), strings.Join(p, ", ")))
		default:
			out = append(out, fmt.Sprintf("%s (%d partitions)", t, len(p)))
		}
	}
	return out
}

func (m SnapshotsModel) failureText(raw string) string {
	f, ok := cratedb.ParseShardFailure(raw)
	if !ok {
		return raw
	}
	where := f.Table
	if f.Partition != "" {
		where += " partition " + f.Partition
	}
	where += fmt.Sprintf(" shard %d", f.Shard)
	if f.NodeID != "" {
		node := m.nodeNames[f.NodeID]
		if node == "" {
			node = f.NodeID
		}
		where += " on " + node
	}
	return where + ": " + f.Reason
}

// snapshotTook is a snapshot's duration: whole seconds, or ms under one.
func snapshotTook(d time.Duration) string {
	if d < time.Second {
		return formatDuration(d.Round(time.Millisecond))
	}
	return shortDuration(d)
}

// shortDuration is d in whole seconds without zero parts: 10s, 5m45s,
// 15m, 2h, 2h5m (no seconds from an hour on).
func shortDuration(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour && d%time.Minute == 0:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < time.Hour:
		return formatDuration(d)
	case d%time.Hour < time.Minute:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return formatDuration(d)
}

func snapshotStateStyle(state string) lipgloss.Style {
	switch state {
	case "SUCCESS":
		return styleHealthGreen
	case "PARTIAL":
		return styleHealthYellow
	case "FAILED", "INCOMPATIBLE":
		return styleHealthRed
	default:
		return styleDim
	}
}
