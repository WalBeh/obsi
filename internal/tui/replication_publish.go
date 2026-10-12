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
)

// pubForm creates a publication (name, tables, review) or changes the
// tables of one (tables, review).
type pubForm struct {
	editing bool
	phase   pubPhase
	name    string
	all     bool
	cands   []pubCandidate
	checked map[string]bool
	orig    map[string]bool // editing: the tables before
	cursor  int
	taken   map[string]bool // existing publication names
	checks  *PublicationChecksMsg
	seq     int // of the last checks request; older answers are dropped
	err     string
}

type pubPhase int

const (
	pubName pubPhase = iota
	pubTables
	pubReview
)

type pubCandidate struct {
	fqn  string
	pubs []string // other publications it is in
	sub  string   // subscription it is a replica of on this cluster
}

// PublicationChecksRequest asks the App for the pre-checks of a review.
// form and seq come back with the answer, so a late one from an earlier
// request or a closed form is dropped.
type PublicationChecksRequest struct {
	Tables []string
	form   *pubForm
	seq    int
}

// PublicationChecksMsg carries the pre-checks: who can read the tables and
// their retention lease periods.
type PublicationChecksMsg struct {
	Readers    []string
	ReadersErr string
	Periods    map[string]time.Duration // only tables that set one
	Checked    int                      // tables whose period was read
	form       *pubForm
	seq        int
}

// ApplyPublicationMsg runs publication statements in order.
type ApplyPublicationMsg struct {
	Stmts []string
	Done  string
}

// PublicationTicketRequest asks the App to copy a subscribe ticket.
type PublicationTicketRequest struct{ Pub cratedb.Publication }

// ReplicationNoticeMsg reports back to the Replication tab. Ran means a
// statement ran and the collectors should look again.
type ReplicationNoticeMsg struct {
	Note, Error string
	Ran         bool
	Periods     bool // a lease period changed
}

// shortLeasePeriod flags a soft_deletes.retention_lease.period a
// subscriber's restart or outage can easily outlast.
const shortLeasePeriod = time.Hour

// maxPeriodChecks caps the SHOW CREATE TABLE calls of one review.
const maxPeriodChecks = 50

func (m ReplicationModel) newPubForm(edit *cratedb.Publication) *pubForm {
	f := &pubForm{checked: map[string]bool{}, taken: map[string]bool{}}
	inPubs := map[string][]string{}
	for _, p := range m.repl.Publications {
		f.taken[p.Name] = true
		if edit != nil && p.Name == edit.Name {
			continue
		}
		if p.AllTables {
			continue
		}
		for _, t := range p.Tables {
			inPubs[t] = append(inPubs[t], p.Name)
		}
	}
	replicaOf := map[string]string{}
	for _, s := range m.repl.Subscriptions {
		for _, t := range s.Tables {
			replicaOf[t.FQN()] = s.Name
		}
	}
	tables := m.tables
	if edit != nil {
		// A published table the shards poll hasn't seen yet must stay a
		// candidate, or SET TABLE would drop it unasked.
		known := map[string]bool{}
		for _, t := range tables {
			known[t] = true
		}
		tables = append([]string(nil), tables...)
		for _, t := range edit.Tables {
			if !known[t] {
				tables = append(tables, t)
			}
		}
		sort.Strings(tables)
	}
	for _, t := range tables {
		f.cands = append(f.cands, pubCandidate{fqn: t, pubs: inPubs[t], sub: replicaOf[t]})
	}
	if edit != nil {
		f.editing, f.phase, f.name = true, pubTables, edit.Name
		f.orig = map[string]bool{}
		for _, t := range edit.Tables {
			f.orig[t], f.checked[t] = true, true
		}
	}
	return f
}

func (f *pubForm) chosen() []string {
	if f.all {
		out := make([]string, 0, len(f.cands))
		for _, c := range f.cands {
			out = append(out, c.fqn)
		}
		return out
	}
	var out []string
	for _, c := range f.cands {
		if f.checked[c.fqn] {
			out = append(out, c.fqn)
		}
	}
	return out
}

func quoteFQN(fqn string) string {
	schema, table := cratedb.SplitRegclass(fqn)
	return quoteIdent(schema) + "." + quoteIdent(table)
}

func quoteFQNs(tables []string) string {
	q := make([]string, len(tables))
	for i, t := range tables {
		q[i] = quoteFQN(t)
	}
	return strings.Join(q, ", ")
}

// stmts is what the review runs. CREATE PUBLICATION ... FOR TABLE refuses
// a table that is already in a publication (6.4.5), ALTER PUBLICATION ...
// ADD TABLE takes it, so then the publication is created empty first.
func (f *pubForm) stmts() []string {
	name := quoteIdent(f.name)
	tables := f.chosen()
	if f.editing {
		if len(tables) == 0 {
			var orig []string
			for _, c := range f.cands {
				if f.orig[c.fqn] {
					orig = append(orig, c.fqn)
				}
			}
			return []string{fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s", name, quoteFQNs(orig))}
		}
		return []string{fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", name, quoteFQNs(tables))}
	}
	switch {
	case f.all:
		return []string{fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", name)}
	case len(tables) == 0:
		return []string{"CREATE PUBLICATION " + name}
	case len(f.elsewhere()) > 0:
		return []string{"CREATE PUBLICATION " + name,
			fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", name, quoteFQNs(tables))}
	}
	return []string{fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", name, quoteFQNs(tables))}
}

// elsewhere lists chosen tables that other publications already have.
func (f *pubForm) elsewhere() []pubCandidate {
	var out []pubCandidate
	for _, c := range f.cands {
		if len(c.pubs) > 0 && (f.all || f.checked[c.fqn]) {
			out = append(out, c)
		}
	}
	return out
}

func (f *pubForm) changed() bool {
	if !f.editing {
		return true
	}
	tables := f.chosen()
	if len(tables) != len(f.orig) {
		return true
	}
	for _, t := range tables {
		if !f.orig[t] {
			return true
		}
	}
	return false
}

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// key handles the form's keys; done means it closes.
func (f *pubForm) key(msg tea.KeyMsg) (done bool, cmd tea.Cmd) {
	f.err = ""
	switch f.phase {
	case pubName:
		switch msg.Type {
		case tea.KeyEsc:
			return true, nil
		case tea.KeyEnter:
			f.name = strings.TrimSpace(f.name)
			switch {
			case !identRe.MatchString(f.name):
				f.err = "lowercase letters, digits and _, starting with a letter or _"
			case f.taken[f.name]:
				f.err = "a publication " + f.name + " exists already"
			default:
				f.phase = pubTables
			}
		case tea.KeyBackspace:
			if len(f.name) > 0 {
				f.name = f.name[:len(f.name)-1]
			}
		case tea.KeyRunes, tea.KeySpace:
			f.name += string(msg.Runes)
		}
	case pubTables:
		switch msg.String() {
		case "esc":
			if f.editing {
				return true, nil
			}
			f.phase = pubName
		case "up", "k":
			f.cursor = max(f.cursor-1, 0)
		case "down", "j":
			f.cursor = min(f.cursor+1, max(len(f.cands)-1, 0))
		case " ", "x":
			if !f.all && f.cursor < len(f.cands) {
				t := f.cands[f.cursor].fqn
				f.checked[t] = !f.checked[t]
			}
		case "a":
			if f.editing {
				f.err = "a publication can't switch between FOR TABLE and FOR ALL TABLES; drop it and create it again"
			} else {
				f.all = !f.all
			}
		case "enter":
			if !f.changed() {
				f.err = "nothing changed"
				break
			}
			f.phase, f.checks = pubReview, nil
			f.seq++
			req := PublicationChecksRequest{Tables: f.chosen(), form: f, seq: f.seq}
			return false, func() tea.Msg { return req }
		}
	case pubReview:
		switch msg.String() {
		case "esc", "n":
			f.phase = pubTables
		case "y", "enter":
			done := fmt.Sprintf("publication %s created", f.name)
			if f.editing {
				done = fmt.Sprintf("tables of publication %s changed", f.name)
			}
			apply := ApplyPublicationMsg{Stmts: f.stmts(), Done: done}
			return true, func() tea.Msg { return apply }
		case "c":
			text := strings.Join(f.stmts(), ";\n") + ";\n"
			return false, func() tea.Msg {
				if e := writeClipboard(text); e != "" {
					return ReplicationNoticeMsg{Error: "copy failed: " + e}
				}
				return ReplicationNoticeMsg{Note: "copied the publication SQL"}
			}
		}
	}
	return false, nil
}

func (f *pubForm) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 80, 60)
	title := "New publication"
	if f.editing {
		title = "Tables of publication " + f.name
	}
	lines := []string{styleModalTitle.Render(title), ""}
	switch f.phase {
	case pubName:
		lines = append(lines, "Name: "+styleEditInput.Render(f.name+"▏"), "",
			styleDim.Render("A publication is a named set of tables other clusters can subscribe to."),
			"", styleDim.Render("[Enter] choose tables   [Esc] cancel"))
	case pubTables:
		lines = append(lines, f.renderTables(height-12)...)
	case pubReview:
		lines = append(lines, f.renderReview(innerWidth-4)...)
	}
	if f.err != "" {
		lines = append(lines, "", styleHealthRed.Render(f.err))
	}
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
}

func (f *pubForm) renderTables(room int) []string {
	var lines []string
	if !f.editing {
		box := "[ ]"
		if f.all {
			box = "[x]"
		}
		lines = append(lines, box+" all tables, including ones created later "+styleDim.Render("(a)"), "")
	}
	if len(f.cands) == 0 {
		lines = append(lines, styleDim.Render("no tables on this cluster"))
	}
	room = max(room, 5)
	top := min(max(f.cursor-room/2, 0), max(len(f.cands)-room, 0))
	for i := top; i < len(f.cands) && i < top+room; i++ {
		c := f.cands[i]
		box := "[ ]"
		if f.all || f.checked[c.fqn] {
			box = "[x]"
		}
		line := box + " " + c.fqn
		var notes []string
		if len(c.pubs) > 0 {
			notes = append(notes, "also in "+strings.Join(c.pubs, ", "))
		}
		if c.sub != "" {
			notes = append(notes, "replica of subscription "+c.sub)
		}
		if len(notes) > 0 {
			line += "  " + styleDim.Render(strings.Join(notes, "; "))
		}
		if i == f.cursor {
			line = stylePickerActive.Render("▸ " + line)
		} else {
			line = "  " + line
		}
		lines = append(lines, line)
	}
	if n := len(f.cands) - (top + room); n > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↓ %d more", n)))
	}
	n := len(f.chosen())
	lines = append(lines, "", styleDim.Render(fmt.Sprintf("%d %s chosen · blob tables, views and foreign tables can't be published", n, plural(n, "table", "tables"))),
		"", styleDim.Render("[space] toggle   [Enter] review   [Esc] back"))
	return lines
}

func (f *pubForm) renderReview(width int) []string {
	lines := []string{"Runs:"}
	for _, s := range f.stmts() {
		for _, l := range wrapText(s+";", width-2) {
			lines = append(lines, "  "+styleValue.Render(l))
		}
	}
	lines = append(lines, "")
	note := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, width) {
			lines = append(lines, style.Render(l))
		}
	}
	if f.all {
		note(styleDim, "Every table, including ones created later. CrateDB leaves out blob tables, views and foreign tables.")
	}
	if e := f.elsewhere(); len(e) > 0 && !f.editing && !f.all {
		names := make([]string, len(e))
		for i, c := range e {
			names[i] = c.fqn
		}
		note(styleDim, fmt.Sprintf("%s already in another publication: CREATE PUBLICATION ... FOR TABLE refuses that, ALTER PUBLICATION ... ADD TABLE doesn't, so the publication is created first.", strings.Join(names, ", ")))
	}
	if f.editing {
		var removed []string
		for _, c := range f.cands {
			if f.orig[c.fqn] && !f.checked[c.fqn] {
				removed = append(removed, c.fqn)
			}
		}
		if len(removed) > 0 {
			note(styleHealthYellow, fmt.Sprintf("Removing %s doesn't stop subscribers that already replicate it: they keep receiving changes until DROP SUBSCRIPTION on their side (seen on 6.4.5).", strings.Join(removed, ", ")))
		}
	}
	switch c := f.checks; {
	case c == nil:
		lines = append(lines, styleDim.Render("checking privileges and lease periods..."))
	default:
		if c.ReadersErr != "" {
			note(styleHealthYellow, "Couldn't check who can read the tables: "+c.ReadersErr)
		} else {
			note(lipgloss.NewStyle(), fmt.Sprintf("Users that can read all of them (the subscriber connects as one; tables it can't read are skipped without an error): %s", strings.Join(c.Readers, ", ")))
		}
		var short []string
		for t, p := range c.Periods {
			if p < shortLeasePeriod {
				short = append(short, fmt.Sprintf("%s %s", t, formatDuration(p)))
			}
		}
		sort.Strings(short)
		if len(short) > 0 {
			note(styleHealthYellow, "Short soft_deletes.retention_lease.period: "+strings.Join(short, ", ")+". A subscriber away longer may miss operations.")
		} else {
			note(styleDim, fmt.Sprintf("Retention lease period: no table below %s (default 12h).", formatDuration(shortLeasePeriod)))
		}
		if n := len(f.chosen()); c.Checked < n {
			note(styleDim, fmt.Sprintf("Lease periods read for %d of %d tables.", c.Checked, n))
		}
	}
	lines = append(lines, "", styleDim.Render("[y] run   [c] copy SQL   [Esc] back"))
	return lines
}

// pubDrop confirms DROP PUBLICATION.
type pubDrop struct{ name string }

func (d *pubDrop) key(msg tea.KeyMsg) (done bool, cmd tea.Cmd) {
	switch msg.String() {
	case "y":
		apply := ApplyPublicationMsg{Stmts: []string{"DROP PUBLICATION " + quoteIdent(d.name)}, Done: "publication " + d.name + " dropped"}
		return true, func() tea.Msg { return apply }
	case "n", "esc":
		return true, nil
	}
	return false, nil
}

func (d *pubDrop) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 70, 50)
	lines := []string{
		styleModalTitle.Render("Drop publication " + d.name + "?"), "",
	}
	lines = append(lines, wrapText("Subscribers that already replicate its tables keep receiving changes and keep them read-only (seen on 6.4.5): only DROP SUBSCRIPTION on the subscriber stops that. The tables here are not touched.", innerWidth-4)...)
	lines = append(lines, "", styleDim.Render("[y] drop   [n] cancel"))
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
}

// runPublicationChecks reads who can read the tables (sys.privileges,
// sys.users, sys.roles) and their retention lease periods.
func runPublicationChecks(ctx context.Context, reg *cratedb.Registry, req PublicationChecksRequest) PublicationChecksMsg {
	out := PublicationChecksMsg{Periods: map[string]time.Duration{}, form: req.form, seq: req.seq}
	if readers, err := readers(ctx, reg, req.Tables); err != nil {
		out.ReadersErr = firstLine(err.Error())
	} else {
		out.Readers = readers
	}
	for i, t := range req.Tables {
		if i == maxPeriodChecks {
			break
		}
		resp, err := reg.Query(ctx, "SHOW CREATE TABLE "+quoteFQN(t)+cratedb.QueryTag)
		if err != nil || len(resp.Rows) == 0 {
			continue
		}
		out.Checked++
		if m := leasePeriodRe.FindStringSubmatch(cratedb.ToString(resp.Rows[0][0])); m != nil {
			if ms, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				out.Periods[t] = time.Duration(ms) * time.Millisecond
			}
		}
	}
	return out
}

var leasePeriodRe = regexp.MustCompile(`"soft_deletes\.retention_lease\.period"\s*=\s*'?(\d+)`)

func readers(ctx context.Context, reg *cratedb.Registry, tables []string) ([]string, error) {
	resp, err := reg.Query(ctx, `SELECT grantee, class, ident, state FROM sys.privileges WHERE type = 'DQL'`+cratedb.QueryTag)
	if err != nil {
		return nil, err
	}
	var privs []cratedb.Privilege
	for _, r := range resp.Rows {
		privs = append(privs, cratedb.Privilege{Grantee: cratedb.ToString(r[0]), Class: cratedb.ToString(r[1]),
			Ident: cratedb.ToString(r[2]), Deny: cratedb.ToString(r[3]) == "DENY"})
	}
	var grantees []cratedb.Grantee
	for _, q := range []struct {
		stmt string
		user bool
	}{
		{`SELECT name, superuser, granted_roles['role'] FROM sys.users`, true},
		{`SELECT name, false, granted_roles['role'] FROM sys.roles`, false},
	} {
		resp, err := reg.Query(ctx, q.stmt+cratedb.QueryTag)
		if err != nil {
			return nil, err
		}
		for _, r := range resp.Rows {
			g := cratedb.Grantee{Name: cratedb.ToString(r[0]), Superuser: cratedb.ToBool(r[1]), User: q.user}
			if roles, ok := r[2].([]interface{}); ok {
				for _, role := range roles {
					g.Roles = append(g.Roles, cratedb.ToString(role))
				}
			}
			grantees = append(grantees, g)
		}
	}
	return cratedb.UsersWhoCanRead(tables, privs, grantees), nil
}

// applyPublication runs the statements and stops at the first error.
func applyPublication(ctx context.Context, reg *cratedb.Registry, msg ApplyPublicationMsg) ReplicationNoticeMsg {
	for i, s := range msg.Stmts {
		if _, err := reg.Query(ctx, s+cratedb.ChangeTag); err != nil {
			text := firstLine(err.Error())
			if i > 0 {
				text = fmt.Sprintf("ran %d of %d statements, then: %s", i, len(msg.Stmts), text)
			}
			return ReplicationNoticeMsg{Error: text, Ran: i > 0}
		}
	}
	return ReplicationNoticeMsg{Note: msg.Done, Ran: true}
}

func copyTicket(ctx context.Context, reg *cratedb.Registry, pub cratedb.Publication) ReplicationNoticeMsg {
	text, err := ticket(ctx, reg, pub)
	if err != nil {
		return ReplicationNoticeMsg{Error: "reading sys.nodes failed: " + firstLine(err.Error())}
	}
	if e := writeClipboard(text); e != "" {
		return ReplicationNoticeMsg{Error: "copy failed: " + e}
	}
	return ReplicationNoticeMsg{Note: "copied the subscribe ticket for " + pub.Name}
}

// ticket is what the subscriber side needs: the CREATE SUBSCRIPTION to
// fill in, the tables, and the publisher's nodes with their ports. Never a
// password.
func ticket(ctx context.Context, reg *cratedb.Registry, pub cratedb.Publication) (string, error) {
	resp, err := reg.Query(ctx, `SELECT name, hostname, port['psql'], port['transport'] FROM sys.nodes ORDER BY name`+cratedb.QueryTag)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "-- Subscribe to publication %s of cluster %s (copied by obsi)\n", pub.Name, reg.Status().ClusterName)
	tables := strings.Join(pub.Tables, ", ")
	if pub.AllTables {
		tables = fmt.Sprintf("all tables (%d now): %s", len(pub.Tables), tables)
	}
	fmt.Fprintf(&b, "-- Tables: %s\n", tables)
	b.WriteString("-- Run on the subscriber; none of these tables may exist there.\n")
	b.WriteString("-- <host>: a publisher node the subscriber can reach. mode=pg_tunnel needs only\n")
	b.WriteString("-- its PostgreSQL port; mode=sniff needs transport on every node. Nodes:\n")
	for _, r := range resp.Rows {
		fmt.Fprintf(&b, "--   %s  %s  psql %d  transport %d\n", cratedb.ToString(r[0]), cratedb.ToString(r[1]),
			cratedb.ToInt64(r[2]), cratedb.ToInt64(r[3]))
	}
	b.WriteString("-- <user> needs DQL on the tables; ones it can't read are skipped without an error.\n")
	b.WriteString("-- Add &sslmode=require to insist on TLS.\n")
	fmt.Fprintf(&b, "CREATE SUBSCRIPTION %s CONNECTION 'crate://<host>:5432?user=<user>&password=<password>&mode=pg_tunnel' PUBLICATION %s;\n",
		quoteIdent(pub.Name), quoteIdent(pub.Name))
	return b.String(), nil
}
