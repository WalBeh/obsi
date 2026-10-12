package tui

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// subForm creates a subscription on this cluster. A pasted subscribe
// ticket (y on the publisher's Replication tab) fills in the publication
// and tells which tables will arrive.
type subForm struct {
	field  int
	vals   [subFieldCount]string
	tables []string // from the ticket; nil without one
	review bool
	// running keeps the form open while CREATE SUBSCRIPTION runs, so a
	// refused connection doesn't lose what was typed.
	running bool
	taken   map[string]bool
	// partial: obsi isn't a superuser and sees only its own
	// subscriptions, so taken and failed may miss some.
	partial bool
	local   map[string]bool   // tables on this cluster
	failed  map[string]string // table -> subscription, failed as "Relation already exists"
	err     string
}

const (
	subName = iota
	subHost
	subPort
	subMode
	subSSL
	subUser
	subPassword
	subPubs
	subFieldCount
)

var subLabels = [subFieldCount]string{"Name", "Host", "Port", "Mode", "SSL", "User", "Password", "Publications"}

var (
	subModes    = []string{"pg_tunnel", "sniff"}
	subSSLModes = []string{"prefer", "require", "disable"}
	modePorts   = map[string]string{"pg_tunnel": "5432", "sniff": "4300"}
)

// ApplyReplicationMsg runs one statement of the Replication tab: CREATE
// or DROP SUBSCRIPTION, a lease period change, a RESET GLOBAL. The
// connection string goes in Args, so sys.jobs_log records CONNECTION ? and
// not the password. Periods means a lease period changed and the
// collector's cached periods are stale.
type ApplyReplicationMsg struct {
	Stmt    string
	Args    []interface{}
	Done    string
	Periods bool
	// Timeout is what to say when CrateDB doesn't answer in time.
	Timeout string
}

// subscriptionTimeout covers CrateDB's 30s connect timeout to an
// unreachable publisher.
var subscriptionTimeout = time.Minute

func (m ReplicationModel) newSubForm() *subForm {
	f := &subForm{taken: map[string]bool{}, local: map[string]bool{}, failed: map[string]string{},
		partial: !m.repl.Superuser && m.repl.User != ""}
	f.vals[subMode], f.vals[subSSL], f.vals[subPort] = "pg_tunnel", "prefer", "5432"
	for _, s := range m.repl.Subscriptions {
		f.taken[s.Name] = true
		for _, t := range s.Tables {
			if t.State == "e" && strings.HasPrefix(t.Reason, "Relation already exists") {
				f.failed[t.FQN()] = s.Name
			}
		}
	}
	for _, t := range m.tables {
		f.local[t] = true
	}
	return f
}

var (
	portRe         = regexp.MustCompile(`^\d+$`)
	ticketPubRe    = regexp.MustCompile(`(?i)\bPUBLICATION\s+(.+?)\s*;?\s*$`)
	ticketTablesRe = regexp.MustCompile(`(?m)^-- Tables: (?:all tables \(\d+ now\): )?(.*)$`)
)

// parseTicket reads a subscribe ticket: the publication from its CREATE
// SUBSCRIPTION line, the tables from the "-- Tables:" comment.
func parseTicket(text string) (pubs []string, tables []string, ok bool) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToUpper(line), "CREATE SUBSCRIPTION") {
			continue
		}
		if m := ticketPubRe.FindStringSubmatch(line); m != nil {
			for _, p := range strings.Split(m[1], ",") {
				p = strings.TrimSpace(p)
				if len(p) >= 2 && p[0] == '"' && p[len(p)-1] == '"' {
					p = strings.ReplaceAll(p[1:len(p)-1], `""`, `"`)
				}
				if p != "" {
					pubs = append(pubs, p)
				}
			}
		}
	}
	if m := ticketTablesRe.FindStringSubmatch(text); m != nil {
		for _, t := range strings.Split(m[1], ",") {
			if t = strings.TrimSpace(t); t != "" {
				tables = append(tables, t)
			}
		}
	}
	return pubs, tables, len(pubs) > 0
}

func (f *subForm) pubs() []string {
	var out []string
	for _, p := range strings.Split(f.vals[subPubs], ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// conninfo builds the CONNECTION string with password as given: encoded
// for running, a mask for showing, empty for none.
func (f *subForm) conninfo(password string) string {
	q := "user=" + url.QueryEscape(f.vals[subUser])
	if password != "" {
		q += "&password=" + password
	}
	q += "&mode=" + f.vals[subMode]
	if f.vals[subMode] == "pg_tunnel" {
		q += "&sslmode=" + f.vals[subSSL]
	}
	return fmt.Sprintf("crate://%s:%s?%s", f.vals[subHost], f.vals[subPort], q)
}

func (f *subForm) stmt() string {
	pubs := f.pubs()
	q := make([]string, len(pubs))
	for i, p := range pubs {
		q[i] = quoteIdent(p)
	}
	return fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION ? PUBLICATION %s", quoteIdent(f.vals[subName]), strings.Join(q, ", "))
}

// shown is the statement with the connection string inlined, the password
// replaced by mask.
func (f *subForm) shown(mask string) string {
	pw := ""
	if f.vals[subPassword] != "" {
		pw = mask
	}
	return strings.Replace(f.stmt(), "CONNECTION ?", "CONNECTION '"+f.conninfo(pw)+"'", 1)
}

// tlsHangs: in 6.4.5 a pg_tunnel subscription that tries TLS doesn't
// answer when the subscriber doesn't trust the publisher's certificate.
func (f *subForm) tlsHangs() bool {
	return f.vals[subMode] == "pg_tunnel" && f.vals[subSSL] != "disable"
}

func (f *subForm) timeoutHint() string {
	if f.tlsHangs() {
		return "no answer within a minute. With sslmode " + f.vals[subSSL] + " CrateDB doesn't answer when this cluster doesn't trust the publisher's certificate (its log: PKIX path building failed): trust it, or use sslmode=disable"
	}
	return "no answer within a minute: is " + f.vals[subHost] + ":" + f.vals[subPort] + " reachable from this cluster?"
}

func (f *subForm) validate() string {
	switch {
	case !identRe.MatchString(f.vals[subName]):
		return "name: lowercase letters, digits and _, starting with a letter or _"
	case f.taken[f.vals[subName]]:
		return "a subscription " + f.vals[subName] + " exists already"
	case !validHost(f.vals[subHost]):
		return "host: a hostname or address without crate:// or a port; IPv6 in brackets, e.g. [fd00::1]"
	case !portRe.MatchString(f.vals[subPort]):
		return "port: a number (5432 for pg_tunnel, 4300 for sniff)"
	case f.vals[subUser] == "":
		return "user: the publisher's user the subscription connects as"
	case len(f.pubs()) == 0:
		return "publications: one or more names, comma separated"
	}
	return ""
}

func validHost(h string) bool {
	if h == "" || strings.ContainsAny(h, "/?&@ ") {
		return false
	}
	if strings.HasPrefix(h, "[") {
		return strings.HasSuffix(h, "]") && strings.Count(h, "[") == 1 && strings.Count(h, "]") == 1
	}
	return !strings.Contains(h, ":")
}

// blockers are reasons not to run at all.
func (f *subForm) blockers() []string {
	var out []string
	for t, sub := range f.failed {
		out = append(out, fmt.Sprintf("Subscription %s has %s failed with \"Relation already exists\". In 6.4.5 the next CREATE SUBSCRIPTION makes it try to delete the local %s, and every subscription here fails. Drop %s first (d).", sub, t, t, sub))
	}
	var exist []string
	for _, t := range f.tables {
		if f.local[t] {
			exist = append(exist, t)
		}
	}
	if len(exist) > 0 {
		out = append(out, fmt.Sprintf("These tables exist here, and CrateDB doesn't subscribe over existing tables: %s.", strings.Join(exist, ", ")))
	}
	return out
}

// move goes to the next or previous field, past SSL when it isn't shown.
func (f *subForm) move(step int) {
	f.field = (f.field + step + subFieldCount) % subFieldCount
	if f.field == subSSL && f.vals[subMode] != "pg_tunnel" {
		f.field = (f.field + step + subFieldCount) % subFieldCount
	}
}

func cycle(vals []string, cur string, step int) string {
	for i, v := range vals {
		if v == cur {
			return vals[(i+step+len(vals))%len(vals)]
		}
	}
	return vals[0]
}

// key handles the form's keys; done means it closes.
func (f *subForm) key(msg tea.KeyMsg) (done bool, cmd tea.Cmd) {
	if f.running {
		return false, nil
	}
	f.err = ""
	if f.review {
		switch msg.String() {
		case "esc", "n":
			f.review = false
		case "y", "enter":
			if len(f.blockers()) > 0 {
				f.err = "not run: see above"
				return false, nil
			}
			// CrateDB URL-decodes the parameters.
			pw := ""
			if f.vals[subPassword] != "" {
				pw = url.QueryEscape(f.vals[subPassword])
			}
			apply := ApplyReplicationMsg{Stmt: f.stmt(), Args: []interface{}{f.conninfo(pw)},
				Done: "subscription " + f.vals[subName] + " created", Timeout: f.timeoutHint()}
			f.running = true
			return false, func() tea.Msg { return apply }
		case "c":
			text := f.shown("<password>") + ";\n"
			return false, func() tea.Msg {
				if e := writeClipboard(text); e != "" {
					return ReplicationNoticeMsg{Error: "copy failed: " + e}
				}
				return ReplicationNoticeMsg{Note: "copied the CREATE SUBSCRIPTION, without the password"}
			}
		}
		return false, nil
	}
	if msg.Paste {
		// Terminals send line breaks in a bracketed paste as \r.
		text := strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(string(msg.Runes))
		if pubs, tables, ok := parseTicket(text); ok {
			f.vals[subPubs] = strings.Join(pubs, ", ")
			if f.vals[subName] == "" {
				f.vals[subName] = pubs[0]
			}
			f.tables = tables
			f.field = subHost
			return false, nil
		}
		if f.field != subMode && f.field != subSSL {
			f.vals[f.field] += strings.TrimSpace(strings.ReplaceAll(text, "\n", ""))
		}
		return false, nil
	}
	switch msg.String() {
	case "esc":
		return true, nil
	case "tab", "down":
		f.move(1)
	case "shift+tab", "up":
		f.move(-1)
	case "enter":
		if e := f.validate(); e != "" {
			f.err = e
			return false, nil
		}
		f.review = true
	case "left", "right", " ":
		step := 1
		if msg.String() == "left" {
			step = -1
		}
		switch f.field {
		case subMode:
			old := f.vals[subMode]
			f.vals[subMode] = cycle(subModes, old, step)
			if f.vals[subPort] == modePorts[old] {
				f.vals[subPort] = modePorts[f.vals[subMode]]
			}
		case subSSL:
			f.vals[subSSL] = cycle(subSSLModes, f.vals[subSSL], step)
		default:
			if msg.String() == " " {
				f.vals[f.field] += " "
			}
		}
	case "backspace":
		if v := f.vals[f.field]; v != "" && f.field != subMode && f.field != subSSL {
			f.vals[f.field] = v[:len(v)-1]
		}
	default:
		if msg.Type == tea.KeyRunes && f.field != subMode && f.field != subSSL {
			f.vals[f.field] += string(msg.Runes)
		}
	}
	return false, nil
}

func (f *subForm) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 80, 60)
	textW := innerWidth - 4 // the border's padding is inside its width
	lines := []string{styleModalTitle.Render("New subscription on this cluster"), ""}
	note := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, textW) {
			lines = append(lines, style.Render(l))
		}
	}
	if f.review {
		lines = append(lines, "Runs:")
		for _, l := range wrapText(f.shown("***")+";", textW-2) {
			lines = append(lines, "  "+styleValue.Render(l))
		}
		lines = append(lines, "")
		note(styleDim, "The connection string goes as a bind parameter: sys.jobs_log records CONNECTION ?, not the password.")
		if f.tables != nil {
			note(lipgloss.NewStyle(), "Tables to arrive: "+strings.Join(f.tables, ", ")+". They become read-only replicas here; the initial copy starts right away.")
		} else {
			note(styleDim, "Without a ticket the tables aren't known here; CrateDB refuses if any of them exists on this cluster.")
		}
		if f.vals[subMode] == "sniff" {
			note(styleDim, "sniff connects to the transport port of every publisher node: their published addresses must be reachable from this cluster.")
			note(styleHealthYellow, "sniff doesn't check the password (seen on 6.4.5): the user only has to exist on the publisher. pg_tunnel does.")
		}
		if f.tlsHangs() {
			note(styleHealthYellow, "sslmode "+f.vals[subSSL]+": if the publisher has TLS on 5432, this cluster must trust its certificate; otherwise CREATE SUBSCRIPTION doesn't answer at all (seen on 6.4.5). obsi gives up after a minute.")
		}
		if f.partial {
			note(styleDim, "obsi isn't a superuser and sees only its own subscriptions: the checks below can't see failed tables of others.")
		}
		note(styleDim, "A refused connection or unknown user fails at once; an unreachable host after CrateDB's 30s connect timeout.")
		for _, b := range f.blockers() {
			lines = append(lines, "")
			note(styleHealthRed, b)
		}
		if f.running {
			lines = append(lines, "", styleHealthYellow.Render("running... an unreachable host takes CrateDB 30s to give up"))
		} else {
			lines = append(lines, "", styleDim.Render("[y] run   [c] copy SQL without the password   [Esc] back"))
		}
	} else {
		lines = append(lines, styleDim.Render("Paste a subscribe ticket (y on the publisher's Replication tab) or fill in:"), "")
		for i := 0; i < subFieldCount; i++ {
			if i == subSSL && f.vals[subMode] != "pg_tunnel" {
				continue
			}
			v := f.vals[i]
			switch i {
			case subPassword:
				v = strings.Repeat("•", len(v))
			case subMode, subSSL:
				v = "‹ " + v + " ›"
			}
			label := fmt.Sprintf("%-13s", subLabels[i])
			if i == f.field {
				if i != subMode && i != subSSL {
					v += "▏"
				}
				lines = append(lines, stylePickerActive.Render("▸ "+label)+styleEditInput.Render(v))
			} else {
				lines = append(lines, "  "+label+v)
			}
		}
		if f.tables != nil {
			lines = append(lines, "", styleDim.Render("From the ticket: "+strings.Join(f.tables, ", ")))
		}
		lines = append(lines, "", styleDim.Render("[tab ↑↓] field   [←→ space] mode, SSL   [Enter] review   [Esc] cancel"))
	}
	if f.err != "" {
		lines = append(lines, "", styleHealthRed.Render(f.err))
	}
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
}

// subDrop confirms DROP SUBSCRIPTION by typing its name: the tables turn
// writable and can't be subscribed again without dropping them.
type subDrop struct {
	name, typed string
	tables      []string
}

func (d *subDrop) key(msg tea.KeyMsg) (done bool, cmd tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		return true, nil
	case tea.KeyEnter:
		if d.typed != d.name {
			return false, nil
		}
		apply := ApplyReplicationMsg{Stmt: "DROP SUBSCRIPTION " + quoteIdent(d.name), Done: "subscription " + d.name + " dropped"}
		return true, func() tea.Msg { return apply }
	case tea.KeyBackspace:
		if d.typed != "" {
			d.typed = d.typed[:len(d.typed)-1]
		}
	case tea.KeyRunes:
		d.typed += string(msg.Runes)
	}
	return false, nil
}

func (d *subDrop) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 70, 50)
	lines := []string{styleModalTitle.Render("Drop subscription " + d.name + "?"), ""}
	text := "One-way: replication stops and its tables become regular writable tables here. They can't be subscribed again without dropping them first, and a later subscription copies everything again. The publisher doesn't need to be reachable."
	if len(d.tables) > 0 {
		text = "Tables: " + strings.Join(d.tables, ", ") + ". " + text
	}
	lines = append(lines, wrapText(text, innerWidth-4)...)
	input := styleEditInput.Render(d.typed + "▏")
	lines = append(lines, "", "Type "+d.name+" to confirm: "+input, "", styleDim.Render("[Enter] drop   [Esc] cancel"))
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
}

func applyReplication(ctx context.Context, reg *cratedb.Registry, msg ApplyReplicationMsg) ReplicationNoticeMsg {
	ctx, cancel := context.WithTimeout(ctx, subscriptionTimeout)
	defer cancel()
	if _, err := reg.QueryUnbounded(ctx, msg.Stmt+cratedb.ChangeTag, msg.Args...); err != nil {
		if ctx.Err() == context.DeadlineExceeded && msg.Timeout != "" {
			return ReplicationNoticeMsg{Error: msg.Timeout}
		}
		return ReplicationNoticeMsg{Error: firstLine(err.Error())}
	}
	return ReplicationNoticeMsg{Note: msg.Done, Ran: true, Periods: msg.Periods}
}
