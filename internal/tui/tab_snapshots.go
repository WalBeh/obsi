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

// SnapshotsModel is the Snapshots tab: a headline, the repositories each
// with its snapshots below it, running restores, and the selected row's
// details in a pane at the bottom.
type SnapshotsModel struct {
	st        store.SnapshotsState
	nodeNames map[string]string // node ID → name, for the failures
	now       time.Time
	width     int
	height    int
	scroll    int
	maxScroll int
	selected  int // index into rows()
	searching bool
	search    string // matched anywhere in repository.name
	repo      string // enter on a repository: only its snapshots

	noticeText  string
	noticeIsErr bool
	noticeAt    time.Time

	readOnly   bool
	minVersion string    // the oldest node's, for ALTER REPOSITORY (6.4.0)
	modal      replModal // the open dialog, nil without one
}

func NewSnapshotsModel(width, height int) SnapshotsModel {
	return SnapshotsModel{width: width, height: height}
}

func (m SnapshotsModel) Refresh(snap store.StoreSnapshot) SnapshotsModel {
	m.st, m.now = snap.Snapshots, time.Now()
	m.nodeNames = map[string]string{}
	m.minVersion = ""
	for _, n := range snap.Nodes {
		m.nodeNames[n.ID] = n.Name
		if !n.Gone && n.Version != "" && (m.minVersion == "" || cratedb.CompareVersions(n.Version, m.minVersion) < 0) {
			m.minVersion = n.Version
		}
	}
	m.selected = min(m.selected, max(m.items()-1, 0))
	return m.clampScroll()
}

func (m SnapshotsModel) SetSize(width, height int) SnapshotsModel {
	m.width, m.height = width, height
	return m.clampScroll()
}

func (m SnapshotsModel) inputMode() bool { return m.searching || m.modal != nil }

// result shows how a repository statement went; a failed ALTER that left
// changed settings opens the undo.
func (m SnapshotsModel) result(msg RepoResultMsg) SnapshotsModel {
	if msg.Undo {
		m.modal = newRepoUndo(msg)
		return m
	}
	return m.notice(SnapshotsNoticeMsg{Note: msg.Note, Error: msg.Error})
}

// snapRow is a selectable row: a repository, or a snapshot under it.
type snapRow struct {
	repo cratedb.Repository
	snap *cratedb.SnapshotInfo // nil on the repository's own row
}

// rows is the table top to bottom: each repository, then its snapshots
// the filter lets through, newest first. A filter hides repositories
// with nothing to show.
func (m SnapshotsModel) rows() []snapRow {
	byRepo := map[string][]cratedb.SnapshotInfo{}
	for _, s := range m.shown() {
		byRepo[s.Repository] = append(byRepo[s.Repository], s)
	}
	filtering := m.search != "" || m.repo != ""
	var out []snapRow
	known := map[string]bool{}
	add := func(r cratedb.Repository) {
		snaps := byRepo[r.Name]
		if filtering && len(snaps) == 0 && (m.repo != r.Name) && !(m.repo == "" && strings.Contains(strings.ToLower(r.Name), strings.ToLower(m.search))) {
			return
		}
		out = append(out, snapRow{repo: r})
		for i := range snaps {
			out = append(out, snapRow{repo: r, snap: &snaps[i]})
		}
	}
	for _, r := range m.st.Repositories {
		known[r.Name] = true
		add(r)
	}
	// Snapshots of a repository sys.repositories no longer lists.
	for _, s := range m.shown() {
		if !known[s.Repository] {
			known[s.Repository] = true
			add(cratedb.Repository{Name: s.Repository})
		}
	}
	return out
}

// selectedRepo is the repository whose own row is under the cursor.
func (m SnapshotsModel) selectedRepo() (cratedb.Repository, bool) {
	rows := m.rows()
	if m.selected < len(rows) && rows[m.selected].snap == nil {
		return rows[m.selected].repo, true
	}
	return cratedb.Repository{}, false
}

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
	rows := m.rows()
	if m.selected >= len(rows) || rows[m.selected].snap == nil {
		return cratedb.SnapshotInfo{}, cratedb.Repository{}, false
	}
	return *rows[m.selected].snap, rows[m.selected].repo, true
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

func (m SnapshotsModel) items() int { return len(m.rows()) }

func (m SnapshotsModel) HandleKey(msg tea.KeyMsg) (SnapshotsModel, tea.Cmd) {
	if m.modal != nil {
		done, cmd := m.modal.key(msg)
		if done {
			m.modal = nil
		}
		return m, cmd
	}
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
	page := max(m.listHeight()-1, 1)
	switch msg.String() {
	case "/":
		m.searching, m.search = true, ""
		return m.clampScroll(), nil
	case "esc":
		m.search, m.repo = "", ""
		return m.clampScroll(), nil
	case "w", "e", "d":
		repo, ok := m.selectedRepo()
		switch {
		case m.readOnly:
			return m.notice(SnapshotsNoticeMsg{Error: readOnlyRefusal("repositories can't be changed")}), nil
		case !ok:
			return m.notice(SnapshotsNoticeMsg{Error: "select a repository (↑) first"}), nil
		case msg.String() == "e":
			m.modal = newRepoForm(repo, m.guard(repo.Name))
		case msg.String() == "d":
			_, count := m.newestFinished()
			m.modal = &repoDrop{repo: repo, snapshots: count[repo.Name], guard: m.guard(repo.Name)}
		case !cratedb.CanToggleReadOnly(repo.Type):
			return m.notice(SnapshotsNoticeMsg{Error: fmt.Sprintf("a %s repository has no readonly setting", repo.Type)}), nil
		default:
			m.modal = &readOnlyToggle{repo: repo, to: !repo.ReadOnly(), guard: m.guard(repo.Name)}
		}
		return m, nil
	case "y":
		s, repo, ok := m.selectedSnapshot()
		if !ok {
			return m.notice(SnapshotsNoticeMsg{Error: "select a snapshot (↓) to copy its details"}), nil
		}
		return m, func() tea.Msg { return SnapshotReportRequest{Snapshot: s, Repository: repo} }
	case "enter":
		// On a repository: show only its snapshots, the first selected.
		if repo, ok := m.selectedRepo(); ok {
			m.repo, m.selected = repo.Name, 0
			if len(m.shown()) > 0 {
				m.selected = 1
			}
			return m.clampScroll().followSelection(), nil
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

// listHeight is what the table may use: the detail pane, the key line
// and the notice line take the rest.
func (m SnapshotsModel) listHeight() int {
	return max(m.height-len(m.detail())-2, 3)
}

func (m SnapshotsModel) clampScroll() SnapshotsModel {
	m.maxScroll = 0
	if n := len(m.render()); n > m.listHeight() {
		m.maxScroll = n - (m.listHeight() - 1)
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m
}

// followSelection scrolls the selected row into view.
func (m SnapshotsModel) followSelection() SnapshotsModel {
	lines := m.render()
	for i, l := range lines {
		if !strings.Contains(l, "▸ ") {
			continue
		}
		// What's visible from a scroll position: the "more" markers take a
		// line each, as in body.
		last := func(scroll int) int {
			room := m.listHeight()
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
		for i > last(m.scroll) {
			m.scroll++
		}
		break
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m
}

func (m SnapshotsModel) View() string {
	if m.modal != nil {
		return m.modal.render(m.width, m.height)
	}
	out := []string{m.body()}
	out = append(out, m.detail()...)
	return strings.Join(append(out, m.keyLine(), m.noticeLine()), "\n")
}

func (m SnapshotsModel) body() string {
	lines := m.render()
	if len(lines) <= m.listHeight() {
		return strings.Join(append(lines, make([]string, m.listHeight()-len(lines))...), "\n")
	}
	room := m.listHeight()
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
		lines = append(lines,
			"  "+styleHealthYellow.Render("No snapshot repository on this cluster: nothing is backed up by snapshots."),
			styleDim.Render("  CREATE REPOSITORY adds one (fs, s3, azure, gcs, url); CREATE SNAPSHOT then writes to it."))
		return append(lines, m.renderRestores()...)
	}
	lines = append(lines, m.headline())
	switch {
	case m.searching:
		lines = append(lines, "  filter: "+m.filterText()+m.search+"▏")
	case m.search != "" || m.repo != "":
		lines = append(lines, "  filter: "+m.filterText()+fmt.Sprintf("%q", m.search)+"  "+styleDim.Render("(esc clears)"))
	}
	lines = append(lines, "")
	lines = append(lines, m.renderTable()...)
	return append(lines, m.renderRestores()...)
}

// filterText is the repository part of the filter line.
func (m SnapshotsModel) filterText() string {
	if m.repo == "" {
		return ""
	}
	return "repository " + m.repo + " · "
}

// renderRestores lists running restores, and apart from them the initial
// copies of subscriptions, which CrateDB runs as restores from an
// internal repository.
func (m SnapshotsModel) renderRestores() []string {
	var restores, copies []string
	for _, r := range m.st.Restores {
		progress := fmt.Sprintf("%s  %d/%d shards", r.State, r.Done, r.Shards)
		if r.Failed > 0 {
			progress += ", " + styleHealthRed.Render(fmt.Sprintf("%d failed", r.Failed))
		}
		if sub, ok := r.Subscription(); ok {
			copies = append(copies, fmt.Sprintf("  subscription %s  %s", sub, progress))
			continue
		}
		restores = append(restores, fmt.Sprintf("  %s.%s  %s", r.Repository, r.Name, progress))
	}
	var lines []string
	if len(restores) > 0 {
		lines = append(append(lines, "", sectionTitle("Restores running")), restores...)
	}
	if len(copies) > 0 {
		lines = append(append(lines, "", sectionTitle("Replication copies running")), copies...)
		lines = append(lines, styleDim.Render("  a subscription's initial copy runs as a restore; the Replication tab (7) says more"))
	}
	return lines
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

// headline answers "did the backups run?" before any row is read, and
// says how fresh the listing is.
func (m SnapshotsModel) headline() string {
	st := m.st
	n := len(st.Repositories)
	head := styleValue.Render(fmt.Sprintf("%d %s", n, plural(n, "repository", "repositories")))
	if st.ListedAt.IsZero() {
		return "  " + head + styleDim.Render(" · listing snapshots (reads every repository)...")
	}
	var newest time.Time
	failed, partial, running := 0, 0, 0
	newestFinished, _ := m.newestFinished()
	for _, s := range newestFinished {
		switch s.State {
		case "FAILED", "INCOMPATIBLE":
			failed++
		case "PARTIAL":
			partial++
		}
	}
	for _, s := range st.Snapshots {
		if s.State == "IN_PROGRESS" {
			running++
		}
		if s.Finished.After(newest) {
			newest = s.Finished
		}
	}
	if newest.IsZero() {
		head += styleDim.Render(" · no finished snapshot")
	} else {
		head += styleDim.Render(" · newest snapshot " + ageShort(m.now.Sub(newest)))
	}
	var marks []string
	if failed > 0 {
		marks = append(marks, styleHealthRed.Render(fmt.Sprintf("✕ %d FAILED", failed)))
	}
	if partial > 0 {
		marks = append(marks, styleHealthYellow.Render(fmt.Sprintf("▲ %d PARTIAL", partial)))
	}
	if len(marks) == 0 && !newest.IsZero() {
		marks = append(marks, styleHealthGreen.Render("● newest per repository succeeded"))
	}
	if running > 0 {
		marks = append(marks, styleDim.Render(fmt.Sprintf("◐ %d running", running)))
	}
	head += "  " + strings.Join(marks, "  ")
	fresh := fmt.Sprintf("listed %s · R lists now", ageShort(m.now.Sub(st.ListedAt)))
	if gap := m.width - lipgloss.Width(head) - len(fresh) - 4; gap > 2 {
		return "  " + head + strings.Repeat(" ", gap) + styleDim.Render(fresh)
	}
	return "  " + head + styleDim.Render(" · "+fresh)
}

// repoSummary is a repository's group-row line: type, access, where its
// snapshots live, and a snapshot throttle when one is set.
func repoSummary(r cratedb.Repository) string {
	parts := []string{r.Type}
	if r.Type != "" {
		access := "writable"
		if r.ReadOnly() {
			access = "read-only"
		}
		parts = append(parts, access)
	}
	for _, k := range []string{"location", "url", "bucket", "container"} {
		if v := r.Settings[k]; v != "" && v != cratedb.MaskedValue {
			if bp := r.Settings["base_path"]; bp != "" {
				v += "/" + strings.TrimPrefix(bp, "/")
			}
			parts = append(parts, v)
			break
		}
	}
	if v := r.Settings["max_snapshot_bytes_per_sec"]; v != "" {
		parts = append(parts, "snapshots throttled to "+v+"/s")
	}
	return strings.Join(slices.DeleteFunc(parts, func(p string) bool { return p == "" }), " · ")
}

// snapMark is a snapshot's state as a mark and a word.
func snapMark(state string) string {
	switch state {
	case "SUCCESS":
		return styleHealthGreen.Render(fmt.Sprintf("%-11s", "● SUCCESS"))
	case "PARTIAL":
		return styleHealthYellow.Render(fmt.Sprintf("%-11s", "▲ PARTIAL"))
	case "FAILED", "INCOMPATIBLE":
		return styleHealthRed.Render(fmt.Sprintf("%-11s", "✕ "+state))
	case "IN_PROGRESS":
		return styleDim.Render(fmt.Sprintf("%-11s", "◐ running"))
	}
	return fmt.Sprintf("%-11s", state)
}

// renderTable is the repositories, each a group row with its snapshots
// below, under one column header.
func (m SnapshotsModel) renderTable() []string {
	rows := m.rows()
	nameW := len("REPOSITORY / SNAPSHOT") - 2
	for _, r := range rows {
		if r.snap != nil {
			nameW = max(nameW, min(len(r.snap.Name), 40))
		}
	}
	row := fmt.Sprintf("    %%-%ds  %%s  %%9s  %%8s  %%6s  %%s", nameW)
	lines := []string{styleColumns.Render(fmt.Sprintf("  %-*s  %-11s  %9s  %8s  %6s  %s", nameW+2, "REPOSITORY / SNAPSHOT", "STATE", "STARTED", "TOOK", "SHARDS", "FAILED"))}
	_, count := m.newestFinished()
	for i, r := range rows {
		marker := "  "
		if i == m.selected {
			marker = "▸ "
		}
		if r.snap == nil {
			lines = append(lines, marker+styleValue.Render(r.repo.Name)+"  "+styleDim.Render(repoSummary(r.repo)))
			switch {
			case m.st.ListedAt.IsZero():
				lines = append(lines, styleDim.Render("    not listed yet"))
			case count[r.repo.Name] == 0 && m.search == "":
				lines = append(lines, styleHealthYellow.Render("    none listed (empty, or CrateDB couldn't read it)"))
			}
			continue
		}
		s := *r.snap
		started, took := "—", "—"
		if !s.Started.IsZero() {
			started = ageShort(m.now.Sub(s.Started))
			switch {
			case !s.Finished.IsZero():
				took = snapshotTook(s.Finished.Sub(s.Started))
			case s.State == "IN_PROGRESS":
				took = snapshotTook(m.now.Sub(s.Started)) + "…"
			}
		}
		failed := ""
		if n := len(s.Failures); n > 0 {
			failed = styleHealthRed.Render(fmt.Sprint(n))
		}
		line := fmt.Sprintf(row, truncateString(s.Name, nameW), snapMark(s.State), started, took, fmt.Sprint(s.TotalShards), failed)
		lines = append(lines, "  "+marker+strings.TrimPrefix(line, "    "))
	}
	return lines
}

// detail is the selected row's details under a divider, at most half the
// tab's height.
func (m SnapshotsModel) detail() []string {
	rows := m.rows()
	if m.selected >= len(rows) || m.st.UpdatedAt.IsZero() {
		return nil
	}
	r := rows[m.selected]
	var lines []string
	if r.snap == nil {
		_, count := m.newestFinished()
		lines = append(lines, "  "+styleValue.Render(r.repo.Name)+"  "+styleDim.Render(fmt.Sprintf("%s · %d %s listed", repoSummary(r.repo), count[r.repo.Name], plural(count[r.repo.Name], "snapshot", "snapshots"))))
		lines = append(lines, m.repositoryDetail(r.repo)...)
	} else {
		s := *r.snap
		head := "  " + styleValue.Render(s.Repository+"."+s.Name) + "  " + snapMark(s.State)
		if !s.Started.IsZero() {
			head += styleDim.Render(" started " + s.Started.Local().Format("2006-01-02 15:04:05"))
		}
		lines = append(lines, head)
		lines = append(lines, m.snapshotDetail(s)...)
	}
	limit := max(m.height/2-1, 4)
	if len(lines) > limit {
		more := len(lines) - limit + 1
		lines = append(lines[:limit-1], styleDim.Render(fmt.Sprintf("  … %d more lines (y copies all of a snapshot)", more)))
	}
	return append([]string{styleDim.Render(strings.Repeat("─", max(m.width-2, 20)))}, lines...)
}

// keyLine is the keys that matter here, and the way to all of them.
func (m SnapshotsModel) keyLine() string {
	keys := "  enter only this repository · / filter · y copy · w e d change · ? all keys"
	if _, _, ok := m.selectedSnapshot(); ok {
		keys = "  y copy this snapshot · / filter · esc clear · ? all keys"
	}
	return styleDim.Render(keys)
}

// repositoryDetail is a repository's settings as sys.repositories gives
// them, on as few lines as fit.
func (m SnapshotsModel) repositoryDetail(r cratedb.Repository) []string {
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(r.Settings)) {
		parts = append(parts, k+" "+r.Settings[k])
	}
	if len(parts) == 0 {
		return []string{"  " + styleDim.Render("no settings")}
	}
	var lines []string
	for _, l := range wrapText(strings.Join(parts, " · "), max(m.width-6, 40)) {
		lines = append(lines, "  "+l)
	}
	if slices.Contains(slices.Collect(maps.Values(r.Settings)), cratedb.MaskedValue) {
		lines = append(lines, "  "+styleDim.Render("[xxxxx]: CrateDB doesn't show credentials"))
	}
	return lines
}

func (m SnapshotsModel) snapshotDetail(s cratedb.SnapshotInfo) []string {
	w := max(m.width-4, 40)
	var lines []string
	add := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, w) {
			lines = append(lines, "  "+style.Render(l))
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
