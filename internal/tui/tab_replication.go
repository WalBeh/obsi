package tui

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
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
	noticeText  string
	noticeIsErr bool
	noticeAt    time.Time
}

func NewReplicationModel(width, height int) ReplicationModel {
	return ReplicationModel{width: width, height: height}
}

func (m ReplicationModel) Refresh(snap store.StoreSnapshot) ReplicationModel {
	m.repl, m.now = snap.Replication, time.Now()
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
	if !r.Superuser && r.User != "" {
		for _, l := range wrapText(fmt.Sprintf("obsi connects as %s, not a superuser: CrateDB lists publications and subscriptions only to their owner and superusers, so others' are missing here, and which tables a subscriber pulls are still published can't be told.", r.User), max(m.width-4, 40)) {
			lines = append(lines, "  "+styleHealthYellow.Render(l))
		}
	}
	switch {
	case r.UpdatedAt.IsZero():
		lines = append(lines, styleDim.Render("  waiting for the first replication poll"))
		return lines
	case len(r.Subscriptions) == 0 && len(r.Publications) == 0 && len(r.Subscribers) == 0:
		none := "  No publications or subscriptions on this cluster."
		if !r.Superuser && r.User != "" {
			none = "  No publications or subscriptions owned by " + r.User + "."
		}
		return append(lines,
			none,
			styleDim.Render("  Logical replication shows here: what this cluster pulls (CREATE SUBSCRIPTION)"),
			styleDim.Render("  and what it serves to other clusters (CREATE PUBLICATION)."),
			"", styleDim.Render("  n: new publication · s: new subscription"))
	}
	if len(r.Subscriptions) > 0 {
		lines = append(lines, m.renderSubscriptions()...)
	}
	if len(r.Publications) > 0 || len(r.Subscribers) > 0 {
		if len(lines) > 0 {
			lines = append(lines, "")
		}
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
	return append(lines, "", styleDim.Render("  ↑↓ select · n publication · s subscription · e tables · d drop · l lease period · c settings · y ticket · ? help"))
}

var crateMessageRe = regexp.MustCompile(`"message"\s*:\s*"((?:[^"\\]|\\.)*)"`)

// crateMessage is CrateDB's own message from an error that carries its
// JSON body, e.g. MissingPrivilegeException[...], else the first line.
func crateMessage(err string) string {
	if m := crateMessageRe.FindStringSubmatch(err); m != nil {
		return strings.ReplaceAll(m[1], `\"`, `"`)
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

var subStates = map[string]string{"i": "preparing", "d": "copying", "r": "in sync", "e": "FAILED"}

func (m ReplicationModel) renderSubscriptions() []string {
	lines := []string{sectionTitle("Subscriptions · this cluster pulls")}
	nameW := 20
	for _, s := range m.repl.Subscriptions {
		for _, t := range s.Tables {
			nameW = max(nameW, min(len(t.FQN()), 40))
		}
	}
	for i, s := range m.repl.Subscriptions {
		marker := "  "
		if i == m.selected {
			marker = "▸ "
		}
		lines = append(lines, fmt.Sprintf("%s%s  ← %s   publications %s",
			marker, styleValue.Render(s.Name), connTarget(s.ConnInfo), strings.Join(s.Publications, ", ")))
		if len(s.Tables) == 0 {
			lines = append(lines, styleDim.Render("    no tables yet"))
			continue
		}
		spark := ""
		if m.sparks() {
			spark = fmt.Sprintf("  %-*s", sparkWidth, spanLabel(m.repl.HistorySpan))
		}
		lines = append(lines, styleHeader.Render(fmt.Sprintf("    %-*s  %-9s  %-6s  %8s  %9s%s  %s",
			nameW, "TABLE", "STATE", "SHARDS", "DOCS", "CHANGES/s", spark, "LAST CHANGE")))
		for _, t := range s.Tables {
			lines = append(lines, m.subscribedRow(t, nameW)...)
		}
	}
	if t := m.subscriberTotals(); t != "" {
		lines = append(lines, t)
	}
	return lines
}

func (m ReplicationModel) subscribedRow(t cratedb.SubscribedTable, nameW int) []string {
	r, ok := m.repl.Replicas[t.FQN()]
	state := subStates[t.State]
	if state == "" {
		state = t.State
	}
	stateStyle := styleHealthGreen
	switch t.State {
	case "i", "d":
		stateStyle = styleHealthYellow
	case "e":
		stateStyle = styleHealthRed
	}
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
		rate = "—"
	case !r.CopySince.IsZero():
		last = "for " + formatDuration(m.now.Sub(r.CopySince))
	case !r.LastChange.IsZero():
		last = formatAgo(m.now.Sub(r.LastChange))
	}
	spark := ""
	if m.sparks() {
		spark = "  " + strings.Repeat(" ", sparkWidth)
		if t.State == "r" {
			spark = "  " + sparkline0(r.History, m.repl.HistoryLen, sparkWidth)
		}
	}
	lines := []string{fmt.Sprintf("    %-*s  %s  %-6s  %8s  %9s%s  %s",
		nameW, truncateString(t.FQN(), nameW), stateStyle.Render(fmt.Sprintf("%-9s", state)), shards, docs, rate, spark, last)}
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
// crate://lab1:4300?user=*&password=*&mode=sniff -> crate://lab1:4300 (sniff).
func connTarget(conn string) string {
	u, err := url.Parse(conn)
	if err != nil || u.Host == "" {
		return conn
	}
	target := u.Scheme + "://" + u.Host
	if mode := u.Query().Get("mode"); mode != "" {
		target += " (" + mode + ")"
	}
	return target
}

func (m ReplicationModel) renderPublications() []string {
	lines := []string{sectionTitle("Publications · this cluster serves")}
	if len(m.repl.Publications) == 0 {
		none := "  none (dropped?) · n: new publication"
		if !m.repl.Superuser && m.repl.User != "" {
			none = "  none owned by " + m.repl.User + " · n: new publication"
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
		return append(lines, styleDim.Render("  No subscriber holds a lease on the published tables."))
	}
	subs := append([]store.LeaseHolder(nil), m.repl.Subscribers...)
	sort.Slice(subs, func(i, j int) bool { return subs[i].Subscriber < subs[j].Subscriber })
	nameW := 12
	for _, h := range subs {
		nameW = max(nameW, min(len(h.Subscriber), 30))
	}
	spark := ""
	if m.sparks() {
		spark = fmt.Sprintf("  %-*s", sparkWidth, "BEHIND, "+strings.TrimPrefix(spanLabel(m.repl.HistorySpan), "LAST "))
	}
	lines = append(lines, styleHeader.Render(fmt.Sprintf("    %-*s  %6s  %12s%s  %9s  %-10s  %s",
		nameW, "SUBSCRIBER", "TABLES", "BEHIND (ops)", spark, "PULLED/s", "LAST SEEN", "RETENTION LEASE")))
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
		if m.sparks() {
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
		lines = append(lines, styleHealthYellow.Render("  * pulls tables no publication lists any more: a dropped publication doesn't stop a subscriber"),
			styleHealthYellow.Render("    (only its new partitions); DROP SUBSCRIPTION on its side does"))
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
