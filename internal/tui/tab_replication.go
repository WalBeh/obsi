package tui

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// ReplicationModel shows logical replication from the side obsi is
// connected to: subscriptions this cluster pulls, publications it serves
// and the subscribers holding leases on them.
type ReplicationModel struct {
	repl      store.ReplicationState
	now       time.Time
	width     int
	height    int
	scroll    int
	maxScroll int
}

func NewReplicationModel(width, height int) ReplicationModel {
	return ReplicationModel{width: width, height: height}
}

func (m ReplicationModel) Refresh(snap store.StoreSnapshot) ReplicationModel {
	m.repl, m.now = snap.Replication, time.Now()
	return m.clampScroll()
}

func (m ReplicationModel) SetSize(width, height int) ReplicationModel {
	m.width, m.height = width, height
	return m.clampScroll()
}

// clampScroll keeps the scroll position in range of the rendered lines.
// Scrolled to the end, the "↑ more" marker takes a line.
func (m ReplicationModel) clampScroll() ReplicationModel {
	m.maxScroll = 0
	if n := len(m.render()); n > m.height {
		m.maxScroll = n - (m.height - 1)
	}
	m.scroll = min(max(m.scroll, 0), m.maxScroll)
	return m
}

func (m ReplicationModel) HandleKey(msg tea.KeyMsg) (ReplicationModel, tea.Cmd) {
	page := max(m.height-2, 1)
	switch msg.String() {
	case "up", "k":
		m.scroll--
	case "down", "j":
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

func (m ReplicationModel) View() string {
	lines := m.render()
	if len(lines) <= m.height {
		return strings.Join(lines, "\n")
	}
	// The markers take a line each when shown, as in the explain window.
	room := m.height
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
	if r.Err != "" {
		lines = append(lines, styleHealthRed.Render("  replication poll failed: "+firstLine(r.Err)))
	}
	switch {
	case r.UpdatedAt.IsZero():
		if r.Err == "" {
			lines = append(lines, styleDim.Render("  waiting for the first replication poll"))
		}
		return lines
	case len(r.Subscriptions) == 0 && len(r.Publications) == 0:
		return append(lines,
			"  No publications or subscriptions on this cluster.",
			styleDim.Render("  Logical replication shows here: what this cluster pulls (CREATE SUBSCRIPTION)"),
			styleDim.Render("  and what it serves to other clusters (CREATE PUBLICATION)."))
	}
	if len(r.Subscriptions) > 0 {
		lines = append(lines, m.renderSubscriptions()...)
	}
	if len(r.Publications) > 0 {
		if len(lines) > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, m.renderPublications()...)
	}
	return lines
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
	for _, s := range m.repl.Subscriptions {
		lines = append(lines, fmt.Sprintf("  %s  ← %s   publications %s",
			styleValue.Render(s.Name), connTarget(s.ConnInfo), strings.Join(s.Publications, ", ")))
		if len(s.Tables) == 0 {
			lines = append(lines, styleDim.Render("    no tables yet"))
			continue
		}
		lines = append(lines, styleHeader.Render(fmt.Sprintf("    %-*s  %-9s  %-6s  %8s  %9s  %s",
			nameW, "TABLE", "STATE", "SHARDS", "DOCS", "CHANGES/s", "LAST CHANGE")))
		for _, t := range s.Tables {
			lines = append(lines, m.subscribedRow(t, nameW)...)
		}
	}
	return append(lines, styleDim.Render("  How far behind the publisher a table is shows only there: run obsi against the publisher."))
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
	lines := []string{fmt.Sprintf("    %-*s  %s  %-6s  %8s  %9s  %s",
		nameW, truncateString(t.FQN(), nameW), stateStyle.Render(fmt.Sprintf("%-9s", state)), shards, docs, rate, last)}
	if t.State == "e" && t.Reason != "" {
		for _, l := range wrapText(t.Reason, max(m.width-8, 20)) {
			lines = append(lines, "      "+styleHealthRed.Render(l))
		}
	}
	// CrateDB 6.4.5's metadata tracker can die after a restart (NPE in
	// MetadataTracker.processSubscription); new tables then stay
	// initializing until the subscription is created again.
	if t.State == "i" && !r.CopySince.IsZero() && m.now.Sub(r.CopySince) > stuckPreparing {
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
	for _, p := range m.repl.Publications {
		tables := strings.Join(p.Tables, ", ")
		switch {
		case p.AllTables:
			tables = fmt.Sprintf("all tables (%d)", len(p.Tables))
		case len(p.Tables) == 0:
			tables = styleDim.Render("no tables")
		}
		head := "  " + styleValue.Render(p.Name) + "  "
		wrapped := wrapText(tables, max(m.width-len(p.Name)-6, 20))
		for i, l := range wrapped {
			if i == 0 {
				lines = append(lines, head+l)
				continue
			}
			lines = append(lines, strings.Repeat(" ", len(p.Name)+4)+l)
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
	lines = append(lines, styleHeader.Render(fmt.Sprintf("    %-*s  %6s  %12s  %-10s  %s",
		nameW, "SUBSCRIBER", "TABLES", "BEHIND (ops)", "LAST SEEN", "RETENTION LEASE")))
	for _, h := range subs {
		lines = append(lines, fmt.Sprintf("    %-*s  %6d  %12s  %-10s  %s",
			nameW, truncateString(h.Subscriber, nameW), h.Tables, behind(h.Behind),
			formatAgo(m.now.Sub(h.LastSeen)), m.leaseCell(h)))
	}
	return append(lines, styleDim.Render("  Subscribers are cluster names from their retention leases. A lease that expires lets the"),
		styleDim.Render("  publisher drop operations the subscriber hasn't pulled yet."))
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
