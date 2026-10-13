package tui

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// ReplicationModel shows logical replication from the side obsi is
// connected to: subscriptions this cluster pulls, publications it serves
// and the subscribers holding leases on them. Publications can be
// created, changed and dropped here.
type ReplicationModel struct {
	repl      store.ReplicationState
	tables    []string // schema.table, candidates for a publication
	replicas  map[string]tableReplicas
	now       time.Time
	width     int
	height    int
	scroll    int
	maxScroll int

	readOnly    bool
	selected    int                  // subscriptions first, then publications
	modal       replModal            // the open dialog, nil without one
	persistent  bool                 // SET GLOBAL PERSISTENT, from set_global_mode
	version     string               // the oldest node's, for cratedb.Quirk
	nodes       []store.NodeSnapshot // for the logical_replication thread pool
	restores    []cratedb.SnapshotRestore
	noticeText  string
	noticeIsErr bool
	noticeAt    time.Time
}

func NewReplicationModel(width, height int) ReplicationModel {
	return ReplicationModel{width: width, height: height}
}

func (m ReplicationModel) Refresh(snap store.StoreSnapshot) ReplicationModel {
	m.repl, m.now = snap.Replication, time.Now()
	m.restores = snap.Snapshots.Restores
	m.tables = m.tables[:0]
	m.replicas = map[string]tableReplicas{}
	for _, t := range snap.Tables {
		fqn := t.SchemaName + "." + t.TableName
		m.tables = append(m.tables, fqn)
		m.replicas[fqn] = tableReplicas{setting: t.Settings.NumberOfReplicas, partitioned: len(t.Settings.PartitionedBy) > 0}
	}
	sort.Strings(m.tables)
	m.nodes = poolNodes(snap.Nodes)
	m.version = ""
	for _, n := range m.nodes {
		if m.version == "" || cratedb.CompareVersions(n.Version, m.version) < 0 {
			m.version = n.Version
		}
	}
	m.selected = min(m.selected, max(m.items()-1, 0))
	return m.clampScroll()
}

func (m ReplicationModel) inputMode() bool {
	return m.modal != nil
}

func (m ReplicationModel) items() int { return len(m.repl.Subscriptions) + len(m.repl.Publications) }

func (m ReplicationModel) selectedSub() (cratedb.Subscription, bool) {
	if m.selected < len(m.repl.Subscriptions) {
		return m.repl.Subscriptions[m.selected], true
	}
	return cratedb.Subscription{}, false
}

func (m ReplicationModel) selectedPub() (cratedb.Publication, bool) {
	if i := m.selected - len(m.repl.Subscriptions); i >= 0 && i < len(m.repl.Publications) {
		return m.repl.Publications[i], true
	}
	return cratedb.Publication{}, false
}

func (m ReplicationModel) setChecks(msg PublicationChecksMsg) ReplicationModel {
	if f, ok := m.modal.(*pubForm); ok && f == msg.form && f.seq == msg.seq && f.phase == pubReview {
		f.checks = &msg
	}
	return m
}

func (m ReplicationModel) notice(msg ReplicationNoticeMsg) ReplicationModel {
	if f, ok := m.modal.(*subForm); ok && f.running {
		f.running = false
		if msg.Error != "" {
			f.err = msg.Error
			return m
		}
		m.modal = nil
	}
	m.noticeText, m.noticeIsErr, m.noticeAt = msg.Note, msg.Error != "", time.Now()
	if msg.Error != "" {
		m.noticeText = msg.Error
	}
	return m
}

func (m ReplicationModel) noticeLine() string {
	if m.noticeText == "" || time.Since(m.noticeAt) > 8*time.Second {
		return ""
	}
	if m.noticeIsErr {
		return "  " + styleHealthRed.Render(m.noticeText)
	}
	return "  " + styleHealthGreen.Render(m.noticeText)
}

func (m ReplicationModel) SetSize(width, height int) ReplicationModel {
	m.width, m.height = width, height
	return m.clampScroll()
}

// clampScroll keeps the scroll position in range of the rendered lines.
// Scrolled to the end, the "↑ more" marker takes a line.
func (m ReplicationModel) clampScroll() ReplicationModel {
	m.maxScroll = 0
	if n := len(m.render()); n > m.bodyHeight() {
		m.maxScroll = n - (m.bodyHeight() - 1)
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m
}

func (m ReplicationModel) HandleKey(msg tea.KeyMsg) (ReplicationModel, tea.Cmd) {
	if m.modal != nil {
		done, cmd := m.modal.key(msg)
		if done {
			m.modal = nil
		}
		return m, cmd
	}
	pub, hasPub := m.selectedPub()
	sub, hasSub := m.selectedSub()
	page := max(m.height-2, 1)
	switch msg.String() {
	case "n", "e", "d", "s", "c", "l":
		switch {
		case m.readOnly:
			m = m.notice(ReplicationNoticeMsg{Error: readOnlyRefusal("replication can't be changed")})
		case msg.String() == "n":
			m.modal = m.newPubForm(nil)
		case msg.String() == "s":
			m.modal = m.newSubForm()
		case msg.String() == "c":
			m.modal = &replSettingsForm{values: m.repl.Settings, persistent: m.persistent}
		case msg.String() == "l" && hasPub:
			m.modal = m.newLeaseForm(pub)
		case msg.String() == "l":
			m = m.notice(ReplicationNoticeMsg{Error: "select a publication (↑↓) to change its tables' retention lease period"})
		case hasSub && msg.String() == "d":
			d := &subDrop{name: sub.Name}
			for _, t := range sub.Tables {
				d.tables = append(d.tables, t.FQN())
			}
			m.modal = d
		case hasSub:
			m = m.notice(ReplicationNoticeMsg{Error: "a subscription can't be changed (ALTER SUBSCRIPTION isn't implemented in CrateDB): drop it and subscribe again"})
		case !hasPub:
		case msg.String() == "d":
			m.modal = &pubDrop{name: pub.Name, version: m.version}
		case pub.AllTables:
			m = m.notice(ReplicationNoticeMsg{Error: pub.Name + " is FOR ALL TABLES: its tables can't be changed, only the publication dropped"})
		default:
			m.modal = m.newPubForm(&pub)
		}
		return m, nil
	case "y":
		if hasPub {
			return m, func() tea.Msg { return PublicationTicketRequest{Pub: pub} }
		}
		return m, nil
	// Past the first or last item the arrows scroll, so what's above and
	// below the selectable rows stays reachable.
	case "up", "k":
		if m.selected > 0 {
			m.selected--
			return m.followSelection(), nil
		}
		m.scroll--
	case "down", "j":
		if m.selected < m.items()-1 {
			m.selected++
			return m.followSelection(), nil
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

// replModal is a dialog of the tab: it takes every key until done.
type replModal interface {
	key(tea.KeyMsg) (done bool, cmd tea.Cmd)
	render(width, height int) string
}

// followSelection scrolls the selected subscription or publication into
// view.
func (m ReplicationModel) followSelection() ReplicationModel {
	for i, l := range m.render() {
		if strings.Contains(l, "▸ ") {
			switch {
			case i < m.scroll+1:
				m.scroll = max(i-1, 0)
			case i >= m.scroll+m.bodyHeight()-1:
				m.scroll = i - m.bodyHeight() + 2
			}
			break
		}
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m
}

// bodyHeight leaves the last line for notices.
func (m ReplicationModel) bodyHeight() int { return max(m.height-1, 3) }

func (m ReplicationModel) View() string {
	if m.modal != nil {
		return m.modal.render(m.width, m.height)
	}
	return m.body() + "\n" + m.noticeLine()
}

func (m ReplicationModel) body() string {
	lines := m.render()
	if len(lines) <= m.bodyHeight() {
		return strings.Join(lines, "\n")
	}
	// The markers take a line each when shown, as in the explain window.
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

// render lays out the whole tab, one entry per screen line; View cuts the
// visible part.
func (m ReplicationModel) render() []string {
	// Headers render their underline as a second line.
	return strings.Split(strings.Join(m.layout(), "\n"), "\n")
}

func (m ReplicationModel) layout() []string {
	r := m.repl
	var lines []string
	for _, p := range replParts {
		e := r.Errs[p.key]
		if e == "" {
			continue
		}
		msg := crateMessage(e)
		if strings.Contains(msg, "MissingPrivilegeException") && strings.Contains(p.what, "sys.") && r.User != "" {
			msg += " · GRANT DQL ON SCHEMA sys TO " + r.User
		}
		for _, l := range wrapText(fmt.Sprintf("couldn't read %s: %s", p.what, msg), max(m.width-4, 40)) {
			lines = append(lines, "  "+styleHealthRed.Render(l))
		}
	}
	owner := "another user"
	if r.User != "" {
		owner = "a user other than " + r.User
	}
	for _, rs := range hiddenCopies(r, m.restores) {
		sub, _ := rs.Subscription()
		text := fmt.Sprintf("▲ Subscription %s is copying its tables now (%d/%d shards, from sys.snapshot_restore). It isn't listed because %s owns it, and CrateDB shows a subscription only to its owner.",
			sub, rs.Done, rs.Shards, owner)
		for _, w := range wrapText(text, max(m.width-6, 40)) {
			lines = append(lines, "  "+styleHealthYellow.Render(w))
		}
	}
	switch {
	case r.UpdatedAt.IsZero():
		lines = append(lines, styleDim.Render("  waiting for the first replication poll"))
		return lines
	case len(r.Subscriptions) == 0 && len(r.Publications) == 0 && len(r.Subscribers) == 0:
		none := []string{"  " + styleValue.Render("No publications or subscriptions on this cluster.")}
		if r.User != "" {
			none = []string{"  " + styleValue.Render("No publications or subscriptions owned by "+r.User+".")}
			for _, l := range wrapText(ownerNote(r.User), max(m.width-4, 40)) {
				none = append(none, "  "+styleHealthYellow.Render(l))
			}
		}
		return append(append(lines, none...), "", styleDim.Render("  n publish · s subscribe · ? all keys"))
	}
	lines = append(lines, m.headline()...)
	if len(r.Subscriptions) > 0 {
		lines = append(lines, "")
		lines = append(lines, m.renderSubscriptions()...)
	}
	if len(r.Publications) > 0 || len(r.Subscribers) > 0 {
		lines = append(lines, "")
		lines = append(lines, m.renderPublications()...)
	}
	if s := m.renderReplSettings(); len(s) > 0 {
		lines = append(lines, "")
		lines = append(lines, s...)
	}
	if p := m.renderPool(); len(p) > 0 {
		lines = append(lines, "")
		lines = append(lines, p...)
	}
	return append(lines, "", m.keyLine())
}

// hiddenCopies are initial copies of subscriptions pg_subscription
// doesn't list: another user owns them. The copy is a restore, and
// sys.snapshot_restore shows it to every user; once it ends, obsi can't
// tell the subscription exists. Nothing until pg_subscription was read:
// before that every copy would look like another user's.
func hiddenCopies(r store.ReplicationState, restores []cratedb.SnapshotRestore) []cratedb.SnapshotRestore {
	if r.UpdatedAt.IsZero() || r.Errs["subscriptions"] != "" {
		return nil
	}
	listed := map[string]bool{}
	for _, s := range r.Subscriptions {
		listed[s.Name] = true
	}
	var out []cratedb.SnapshotRestore
	for _, rs := range restores {
		if sub, ok := rs.Subscription(); ok && !listed[sub] {
			out = append(out, rs)
		}
	}
	return out
}

// ownerNote explains why other users' replication doesn't show.
func ownerNote(user string) string {
	return fmt.Sprintf("CrateDB lists publications and subscriptions only to the user who created them, superusers included: obsi connects as %s, so replication set up by another user (e.g. admin on CrateDB Cloud) doesn't show here. Connect as that user to see it.", user)
}

var crateMessageRe = regexp.MustCompile(`"message"\s*:\s*"((?:[^"\\]|\\.)*)"`)

// crateMessage is CrateDB's own message from an error that carries its
// JSON body, e.g. MissingPrivilegeException[...], else the first line.
func crateMessage(err string) string {
	if m := crateMessageRe.FindStringSubmatch(err); m != nil {
		return strings.Join(strings.Fields(strings.NewReplacer(`\"`, `"`, `\n`, " ").Replace(m[1])), " ")
	}
	return firstLine(err)
}

// replParts names the parts of a replication poll for their errors.
var replParts = []struct{ key, what string }{
	{"subscriptions", "subscriptions (pg_subscription)"},
	{"publications", "publications (pg_publication)"},
	{"shards", "subscribed shards (sys.shards), showing the last poll"},
	{"partitions", "partition replica settings (information_schema.table_partitions)"},
	{"leases", "subscriber leases (sys.shards), showing the last poll"},
	{"settings", "replication settings (sys.cluster)"},
}

// stuckPreparing is how long a table may sit initializing before the tab
// points at the subscriber's log. The copy starts within seconds normally.
const stuckPreparing = 5 * time.Minute

// subActivity is true once some subscribed table changed while obsi
// watched: until then a sparkline column is empty bars.
func (m ReplicationModel) subActivity() bool {
	for _, r := range m.repl.Replicas {
		for _, v := range r.History {
			if v > 0 {
				return true
			}
		}
	}
	return false
}

// renderSubscriptions is one table for all subscriptions: each a group
// row with its tables below, one column header for all.
func (m ReplicationModel) renderSubscriptions() []string {
	nameW := len("SUBSCRIPTION / TABLE") - 2
	for _, s := range m.repl.Subscriptions {
		for _, t := range s.Tables {
			nameW = max(nameW, min(len(t.FQN()), 40))
		}
	}
	spark := m.sparks() && m.subActivity()
	sparkHdr := ""
	if spark {
		sparkHdr = fmt.Sprintf("  %-*s", sparkWidth, spanLabel(m.repl.HistorySpan))
	}
	header := fmt.Sprintf("  %-*s  %6s  %8s  %9s%s  %s", nameW+2, "SUBSCRIPTION / TABLE", "SHARDS", "DOCS", "CHANGES/s", sparkHdr, "LAST CHANGE")
	lines := []string{styleColumns.Render(header)}
	for i, s := range m.repl.Subscriptions {
		marker := "  "
		if i == m.selected {
			marker = "▸ "
		}
		counts := map[string]int{}
		for _, t := range s.Tables {
			counts[t.State]++
		}
		left := marker + styleValue.Render(s.Name) + styleDim.Render("  ← "+connTarget(s.ConnInfo)+" · "+strings.Join(s.Publications, ", "))
		right := stateCounts(counts)
		if gap := lipgloss.Width(header) - lipgloss.Width(left) - lipgloss.Width(right); gap > 1 && right != "" {
			left += strings.Repeat(" ", gap) + right
		} else if right != "" {
			left += "  " + right
		}
		lines = append(lines, left)
		if len(s.Tables) == 0 {
			lines = append(lines, styleDim.Render("    no tables yet"))
			continue
		}
		for _, t := range s.Tables {
			lines = append(lines, m.subscribedRow(t, nameW, spark)...)
		}
	}
	if t := m.subscriberTotals(); t != "" {
		lines = append(lines, t)
	}
	return lines
}

// subscribedRow is a table under its subscription. The last column is
// the last change while in sync, else its state.
func (m ReplicationModel) subscribedRow(t cratedb.SubscribedTable, nameW int, spark bool) []string {
	r, ok := m.repl.Replicas[t.FQN()]
	shards, docs, rate, last := "—", "—", "—", "—"
	if ok && r.Shards > 0 {
		shards = fmt.Sprintf("%d/%d", r.Started, r.Shards)
		if r.Started == r.Shards {
			docs = formatRecords(r.Docs)
		}
	}
	if r.Rate >= 0 {
		rate = formatRecords(int64(r.Rate + 0.5))
	}
	switch {
	case t.State == "e":
		// The shards are the subscriber's own table, if any; nothing replicates.
		rate, last = "—", subStateMark(t.State)
	case t.State != "r":
		last = subStateMark(t.State)
		if !r.CopySince.IsZero() {
			last += styleDim.Render(" for " + formatDuration(m.now.Sub(r.CopySince)))
		}
	case !r.LastChange.IsZero():
		last = formatAgo(m.now.Sub(r.LastChange))
	}
	sparkCell := ""
	if spark {
		sparkCell = "  " + strings.Repeat(" ", sparkWidth)
		if t.State == "r" {
			sparkCell = "  " + sparkline0(r.History, m.repl.HistoryLen, sparkWidth)
		}
	}
	lines := []string{fmt.Sprintf("    %-*s  %6s  %8s  %9s%s  %s",
		nameW, truncateString(t.FQN(), nameW), shards, docs, rate, sparkCell, last)}
	if r.Copy != nil && t.State == "r" {
		lines = append(lines, m.copyLines(r.Copy)...)
	}
	lines = append(lines, m.replicaLines(t.FQN(), r)...)
	if t.State == "e" && t.Reason != "" {
		for _, l := range wrapText(t.Reason, max(m.width-8, 20)) {
			lines = append(lines, "      "+styleHealthRed.Render(l))
		}
		if q := cratedb.QuirkDeletesLocalTable; cratedb.RelationExists(t.Reason) && q.Affects(m.version) {
			for _, l := range wrapText("Don't create another subscription on this cluster before dropping this one: CrateDB (seen on "+q.SeenOn+") then tries to delete the local "+t.FQN()+" and every subscription here fails.", max(m.width-8, 20)) {
				lines = append(lines, "      "+styleHealthYellow.Render(l))
			}
		}
	}
	if t.State == "i" && !r.CopySince.IsZero() && m.now.Sub(r.CopySince) > stuckPreparing && cratedb.QuirkTrackerDies.Affects(m.version) {
		lines = append(lines, "      "+styleHealthYellow.Render(
			"no copy started yet; if it stays like this, look for MetadataTracker errors in the subscriber's log"))
	}
	return lines
}

// connTarget shows host and mode of a masked connection string,
// crate://lab1:4300?user=*&password=*&mode=sniff -> lab1:4300 · sniff.
func connTarget(conn string) string {
	u, err := url.Parse(conn)
	if err != nil || u.Host == "" {
		return conn
	}
	target := u.Host
	if mode := u.Query().Get("mode"); mode != "" {
		target += " · " + mode
	}
	return target
}

func (m ReplicationModel) renderPublications() []string {
	lines := []string{styleColumns.Render("  PUBLICATION / TABLES")}
	if len(m.repl.Publications) == 0 {
		none := "  none (dropped?) · n: new publication"
		if m.repl.User != "" {
			none = "  none owned by " + m.repl.User + " (others' don't show) · n: new publication"
		}
		lines = append(lines, styleDim.Render(none))
	}
	for i, p := range m.repl.Publications {
		tables := strings.Join(p.Tables, ", ")
		switch {
		case p.AllTables:
			tables = fmt.Sprintf("all tables (%d)", len(p.Tables))
		case len(p.Tables) == 0:
			tables = styleDim.Render("no tables")
		}
		head := "  " + styleValue.Render(p.Name) + "  "
		if i+len(m.repl.Subscriptions) == m.selected {
			head = "▸ " + styleValue.Render(p.Name) + "  "
		}
		wrapped := wrapText(tables, max(m.width-len(p.Name)-6, 20))
		for i, l := range wrapped {
			if i == 0 {
				lines = append(lines, head+l)
				continue
			}
			lines = append(lines, strings.Repeat(" ", len(p.Name)+4)+l)
		}
		if l := m.leasePeriodLine(p); l != "" {
			lines = append(lines, l)
		}
	}
	lines = append(lines, "")
	if len(m.repl.Subscribers) == 0 {
		return append(lines, styleDim.Render("  No subscriber holds a lease on the published tables yet."))
	}
	subs := append([]store.LeaseHolder(nil), m.repl.Subscribers...)
	sort.Slice(subs, func(i, j int) bool { return subs[i].Subscriber < subs[j].Subscriber })
	nameW := 12
	for _, h := range subs {
		nameW = max(nameW, min(len(h.Subscriber), 30))
	}
	// A BEHIND sparkline only once some subscriber was behind.
	showSpark := false
	for _, h := range subs {
		for _, v := range h.History {
			showSpark = showSpark || v > 0
		}
	}
	showSpark = showSpark && m.sparks()
	spark := ""
	if showSpark {
		spark = fmt.Sprintf("  %-*s", sparkWidth, "BEHIND, "+strings.TrimPrefix(spanLabel(m.repl.HistorySpan), "LAST "))
	}
	lines = append(lines, styleColumns.Render(fmt.Sprintf("  %-*s  %6s  %12s%s  %9s  %-10s  %s",
		nameW+2, "SUBSCRIBER", "TABLES", "BEHIND (ops)", spark, "PULLED/s", "LAST SEEN", "RETENTION LEASE")))
	unpublished, unknown := false, false
	for _, h := range subs {
		tables := fmt.Sprintf("%6d", h.Tables)
		if h.Unpublished > 0 {
			tables = styleHealthYellow.Render(fmt.Sprintf("%5d*", h.Tables))
			unpublished = true
		}
		behindCell := behind(h.Behind)
		if h.Unknown > 0 {
			behindCell = styleDim.Render(fmt.Sprintf("%11s?", formatRecords(h.Behind)))
			unknown = true
		}
		spark := ""
		if showSpark {
			spark = "  " + sparkline0(h.History, m.repl.HistoryLen, sparkWidth)
		}
		lines = append(lines, fmt.Sprintf("    %-*s  %s  %12s%s  %s  %-10s  %s",
			nameW, truncateString(h.Subscriber, nameW), tables, behindCell, spark, pulledCell(h),
			formatAgo(m.now.Sub(h.LastSeen)), m.leaseCell(h)))
	}
	if t := m.publisherTotals(); t != "" {
		lines = append(lines, t)
	}
	if unknown {
		lines = append(lines, styleDim.Render("  ? some tables got no change since their initial copy: their leases still say 0, so how far"),
			styleDim.Render("    behind they are isn't known; the next change sets it"))
	}
	if unpublished {
		who := "obsi's user"
		if m.repl.User != "" {
			who = m.repl.User
		}
		text := fmt.Sprintf("* pulls tables no publication visible to %s lists: one another user owns (CrateDB shows publications only to their owner), or one that was dropped. A dropped publication doesn't stop a subscriber, only its new partitions and columns, so it diverges; DROP SUBSCRIPTION on its side stops it.", who)
		for i, l := range wrapText(text, max(m.width-6, 40)) {
			indent := "  "
			if i > 0 {
				indent = "    "
			}
			lines = append(lines, styleHealthYellow.Render(indent+l))
		}
	}
	return lines
}

func behind(ops int64) string {
	if ops == 0 {
		return styleHealthGreen.Render(fmt.Sprintf("%12s", "0"))
	}
	return styleHealthYellow.Render(fmt.Sprintf("%12s", formatRecords(ops)))
}

func (m ReplicationModel) leaseCell(h store.LeaseHolder) string {
	left := h.Expires.Sub(m.now)
	period := " of " + formatDuration(h.Period)
	switch {
	case left <= 0:
		return styleHealthRed.Render("expired " + formatAgo(-left) + period)
	case left < h.Period/2:
		return styleHealthYellow.Render("expires in " + formatDuration(left) + period)
	}
	return styleDim.Render("expires in " + formatDuration(left) + period)
}
