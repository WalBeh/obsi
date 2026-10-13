package tui

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// repoTimeout bounds an ALTER or DROP REPOSITORY. ALTER verifies the
// repository on every node, on the snapshot thread pool; in the lab a
// throttled snapshot in another repository held that up for 39s.
var repoTimeout = 3 * time.Minute

// repoStmt is one statement with its bound values.
type repoStmt struct {
	SQL  string
	Args []interface{}
}

// RepoApplyMsg asks the App to run repository statements in order,
// stopping at the first failure.
type RepoApplyMsg struct {
	Repo  string
	Stmts []repoStmt
	Done  string
	// For an ALTER: the settings before and the keys it touches. When it
	// fails CrateDB keeps the new settings anyway, so the App reads them
	// again and the tab offers to set the old ones back.
	Alter   bool
	Before  map[string]string
	Changed []string
}

// RepoResultMsg is how a RepoApplyMsg went. Undo is set when a failed
// ALTER left settings that differ from Before. Stored says CrateDB stored
// the new settings before failing (verification, timeout): only then do
// masked keys, which read [xxxxx] before and after, count as changed.
type RepoResultMsg struct {
	Note, Error string
	Repo        string
	Before, Now map[string]string
	Changed     []string
	Stored      bool
	Undo        bool
}

// undoKeys are the changed keys a failed ALTER left different.
func (r RepoResultMsg) undoKeys() []string {
	var out []string
	for _, k := range r.Changed {
		if r.Now[k] != r.Before[k] || (r.Stored && r.Now[k] == cratedb.MaskedValue) {
			out = append(out, k)
		}
	}
	return out
}

func applyRepo(ctx context.Context, reg *cratedb.Registry, msg RepoApplyMsg) RepoResultMsg {
	for _, s := range msg.Stmts {
		qctx, cancel := context.WithTimeout(ctx, repoTimeout)
		_, err := reg.QueryUnbounded(qctx, s.SQL+cratedb.ChangeTag, s.Args...)
		timedOut := qctx.Err() == context.DeadlineExceeded
		cancel()
		if err == nil {
			continue
		}
		res := RepoResultMsg{Repo: msg.Repo, Error: crateMessage(err.Error())}
		if timedOut {
			res.Error = fmt.Sprintf("no answer after %s: CrateDB may still be verifying the repository (a running snapshot holds that up); R lists again", shortDuration(repoTimeout))
		}
		if !msg.Alter {
			return res
		}
		resp, qerr := reg.Query(ctx, `SELECT settings FROM sys.repositories WHERE name = ?`+cratedb.QueryTag, msg.Repo)
		if qerr != nil || len(resp.Rows) == 0 {
			return res
		}
		now := map[string]string{}
		if m, ok := resp.Rows[0][0].(map[string]interface{}); ok {
			for k, v := range m {
				now[k] = fmt.Sprint(v)
			}
		}
		res.Before, res.Now, res.Changed = msg.Before, now, msg.Changed
		res.Stored = storedBeforeFailure(err.Error(), timedOut)
		res.Undo = len(res.undoKeys()) > 0
		return res
	}
	return RepoResultMsg{Repo: msg.Repo, Note: msg.Done}
}

// storedBeforeFailure says whether CrateDB stored an ALTER's settings
// before it failed: it verifies after storing, and every failure there is
// a RepositoryVerificationException, whatever its wording. A timeout may
// be one still running.
func storedBeforeFailure(err string, timedOut bool) bool {
	return timedOut || strings.Contains(err, "RepositoryVerificationException")
}

// repoSizeRe is what CrateDB's ByteSizeValue parses: a number with a unit,
// short or long (64m, 64mb), or 0.
var repoSizeRe = regexp.MustCompile(`(?i)^(0|\d+(\.\d+)?(b|k|kb|m|mb|g|gb|t|tb|p|pb))$`)

// checkRepoValue catches values CrateDB would only reject when verifying,
// after it has stored them: a stored max_retries = 'abc' leaves the
// repository listed but its snapshots unreadable.
func checkRepoValue(k cratedb.RepositoryKey, v string) string {
	switch k.Kind {
	case "bool":
		if v != "true" && v != "false" {
			return k.Name + " is true or false"
		}
	case "int":
		if n, err := strconv.Atoi(v); err != nil || n < 0 {
			return k.Name + " is a whole number"
		}
	case "size":
		if !repoSizeRe.MatchString(v) {
			return k.Name + " is a size like 64mb or 512kb"
		}
	case "time":
		if _, ok := parseDelay(v); !ok && v != "0" {
			return k.Name + " is a time like 30s or 2m"
		}
	case "protocol":
		if v != "http" && v != "https" {
			return k.Name + " is http or https"
		}
	}
	return ""
}

// repoGuard is what the last polls say about a repository being in use.
type repoGuard struct {
	minVersion string   // oldest node's
	here       []string // snapshots running in it, restores from it
	elsewhere  []string // snapshots running in other repositories
}

func (m SnapshotsModel) guard(repo string) repoGuard {
	g := repoGuard{minVersion: m.minVersion}
	for _, s := range m.st.Snapshots {
		if s.State != "IN_PROGRESS" {
			continue
		}
		if s.Repository == repo {
			g.here = append(g.here, "snapshot "+s.Name)
		} else {
			g.elsewhere = append(g.elsewhere, s.Repository+"."+s.Name)
		}
	}
	for _, r := range m.st.Restores {
		if r.Repository == repo {
			g.here = append(g.here, "restore of "+r.Name)
		}
	}
	return g
}

// refusal is why obsi won't run ALTER or DROP now, or "".
func (g repoGuard) refusal(what string) string {
	switch {
	case g.minVersion != "" && cratedb.CompareVersions(g.minVersion, "6.4.0") < 0 && what == "ALTER":
		return fmt.Sprintf("ALTER REPOSITORY needs CrateDB 6.4.0 on every node; the oldest here is %s", g.minVersion)
	case len(g.here) > 0:
		return fmt.Sprintf("in use (%s): CrateDB refuses to %s a repository while a snapshot, deletion or restore runs there", strings.Join(g.here, ", "), strings.ToLower(what))
	}
	return ""
}

// elsewhereNote warns that a snapshot running anywhere can hold up the
// verification an ALTER does.
func (g repoGuard) elsewhereNote() string {
	if len(g.elsewhere) == 0 {
		return ""
	}
	return fmt.Sprintf("A snapshot is running (%s): the verification after ALTER waits for the snapshot threads, so it can take minutes.", listMax(g.elsewhere, 2))
}

const failedAlterNote = "If CrateDB can't verify the repository with the new settings, the statement fails but the new settings stay; obsi then offers to set the previous ones back."

func repoValueShown(k cratedb.RepositoryKey, v string) string {
	if k.Secret && v != cratedb.MaskedValue {
		return strings.Repeat("•", min(len(v), 12))
	}
	return v
}

// repoForm edits a repository's settings: pick a key, type a value or x
// to reset it, r to review, y to run.
type repoForm struct {
	repo    cratedb.Repository
	keys    []cratedb.RepositoryKey
	pending map[string]repoChange
	guard   repoGuard
	cursor  int
	editing bool
	review  bool
	buf     string
	err     string
}

type repoChange struct {
	value string
	reset bool
}

func newRepoForm(r cratedb.Repository, g repoGuard) *repoForm {
	f := &repoForm{repo: r, keys: cratedb.RepositoryKeys(r.Type), pending: map[string]repoChange{}, guard: g}
	known := map[string]bool{}
	for _, k := range f.keys {
		known[k.Name] = true
	}
	// Keys set that obsi doesn't know, e.g. from a newer CrateDB.
	for _, k := range slices.Sorted(maps.Keys(r.Settings)) {
		if !known[k] {
			f.keys = append(f.keys, cratedb.RepositoryKey{Name: k, Kind: "text", Secret: r.Settings[k] == cratedb.MaskedValue})
		}
	}
	return f
}

func (f *repoForm) changes() (set, reset []cratedb.RepositoryKey) {
	for _, k := range f.keys {
		c, ok := f.pending[k.Name]
		switch {
		case !ok:
		case c.reset:
			reset = append(reset, k)
		default:
			set = append(set, k)
		}
	}
	return set, reset
}

// stmts is SET then RESET: CrateDB takes one or the other per statement.
func (f *repoForm) stmts() []repoStmt {
	set, reset := f.changes()
	var out []repoStmt
	name := cratedb.QuoteIdent(f.repo.Name)
	if len(set) > 0 {
		var parts []string
		var args []interface{}
		for _, k := range set {
			parts = append(parts, cratedb.QuoteIdent(k.Name)+" = ?")
			args = append(args, f.pending[k.Name].value)
		}
		out = append(out, repoStmt{SQL: fmt.Sprintf("ALTER REPOSITORY %s SET (%s)", name, strings.Join(parts, ", ")), Args: args})
	}
	if len(reset) > 0 {
		var parts []string
		for _, k := range reset {
			parts = append(parts, cratedb.QuoteIdent(k.Name))
		}
		out = append(out, repoStmt{SQL: fmt.Sprintf("ALTER REPOSITORY %s RESET (%s)", name, strings.Join(parts, ", "))})
	}
	return out
}

func (f *repoForm) key(msg tea.KeyMsg) (bool, tea.Cmd) {
	f.err = ""
	if len(f.keys) == 0 {
		return msg.String() == "esc" || msg.String() == "q", nil
	}
	k := f.keys[f.cursor]
	switch {
	case f.editing:
		switch msg.Type {
		case tea.KeyEsc:
			f.editing = false
		case tea.KeyEnter:
			v := strings.TrimSpace(f.buf)
			switch {
			case v == "":
				f.err = "empty: x resets a key to its default"
			case checkRepoValue(k, v) != "":
				f.err = checkRepoValue(k, v)
			case v == f.repo.Settings[k.Name]:
				delete(f.pending, k.Name)
				f.editing = false
			default:
				f.pending[k.Name] = repoChange{value: v}
				f.editing = false
			}
		case tea.KeyBackspace:
			if r := []rune(f.buf); len(r) > 0 {
				f.buf = string(r[:len(r)-1])
			}
		case tea.KeyRunes:
			f.buf += string(msg.Runes)
		}
	case f.review:
		switch msg.String() {
		case "y":
			if f.guard.refusal("ALTER") != "" {
				return false, nil
			}
			var changed []string
			for name := range f.pending {
				changed = append(changed, name)
			}
			slices.Sort(changed)
			apply := RepoApplyMsg{Repo: f.repo.Name, Stmts: f.stmts(), Alter: true, Before: f.repo.Settings, Changed: changed,
				Done: fmt.Sprintf("repository %s changed: %s", f.repo.Name, strings.Join(changed, ", "))}
			return true, func() tea.Msg { return apply }
		case "n", "esc":
			f.review = false
		}
	default:
		switch msg.String() {
		case "esc", "q":
			return true, nil
		case "up", "k":
			f.cursor = max(f.cursor-1, 0)
		case "down", "j":
			f.cursor = min(f.cursor+1, len(f.keys)-1)
		case "enter":
			f.editing, f.buf = true, ""
			if c, ok := f.pending[k.Name]; ok && !c.reset {
				f.buf = c.value
			} else if v := f.repo.Settings[k.Name]; v != cratedb.MaskedValue {
				f.buf = v
			}
		case "x":
			_, isSet := f.repo.Settings[k.Name]
			switch _, ok := f.pending[k.Name]; {
			case ok:
				delete(f.pending, k.Name)
			case k.Required:
				f.err = k.Name + " is required for " + f.repo.Type + " and can't be reset"
			case !isSet:
				f.err = k.Name + " isn't set: it already has the default"
			default:
				f.pending[k.Name] = repoChange{reset: true}
			}
		case "r":
			if len(f.pending) == 0 {
				f.err = "nothing changed yet: enter sets a value, x resets one"
			} else {
				f.review = true
			}
		}
	}
	return false, nil
}

func (f *repoForm) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 85, 60)
	title := styleModalTitle.Render(fmt.Sprintf("ALTER REPOSITORY %s (%s)", f.repo.Name, f.repo.Type))
	if len(f.keys) == 0 {
		return placeModal(lipgloss.JoinVertical(lipgloss.Left, title, "",
			fmt.Sprintf("obsi doesn't know the settings of a %q repository.", f.repo.Type),
			"", styleDim.Render("[Esc] close")), innerWidth, width, height)
	}
	if f.review {
		return placeModal(lipgloss.JoinVertical(lipgloss.Left, f.reviewLines(innerWidth)...), innerWidth, width, height)
	}
	nameW := 0
	for _, k := range f.keys {
		nameW = max(nameW, len(k.Name))
	}
	lines := []string{title, ""}
	for i, k := range f.keys {
		cur, isSet := f.repo.Settings[k.Name]
		val := styleDim.Render("not set")
		switch {
		case isSet:
			val = cur
		case k.Default != "":
			val = styleDim.Render("default " + k.Default)
		}
		if c, ok := f.pending[k.Name]; ok {
			change := "→ " + repoValueShown(k, c.value)
			if c.reset {
				change = "→ reset"
				if k.Default != "" {
					change += " (" + k.Default + ")"
				}
			}
			val += "  " + styleHealthYellow.Render(change)
		}
		if f.editing && i == f.cursor {
			val = styleEditInput.Render(repoValueShown(k, f.buf) + "▏")
		}
		row := fmt.Sprintf("%-*s  %s", nameW, k.Name, val)
		if i != f.cursor {
			lines = append(lines, "  "+row)
			continue
		}
		lines = append(lines, stylePickerActive.Render("▸ ")+row)
		var notes []string
		if k.Required {
			notes = append(notes, "required, can't be reset")
		}
		if k.Secret {
			notes = append(notes, "CrateDB shows it masked; obsi sends it as a bound value, so jobs_log keeps ?")
		}
		if k.Place {
			notes = append(notes, "where the snapshots live")
		}
		if len(notes) > 0 {
			lines = append(lines, styleDim.Render("    "+strings.Join(notes, " · ")))
		}
	}
	if f.err != "" {
		lines = append(lines, "", styleHealthRed.Render(f.err))
	}
	keys := "[↑↓] select   [Enter] new value   [x] reset / drop the change   [r] review   [Esc] close"
	if f.editing {
		keys = "[Enter] keep the value   [Esc] cancel"
	}
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, append(lines, "", styleDim.Render(keys))...), innerWidth, width, height)
}

func (f *repoForm) reviewLines(width int) []string {
	lines := []string{styleModalTitle.Render(fmt.Sprintf("Review: ALTER REPOSITORY %s", f.repo.Name)), ""}
	set, reset := f.changes()
	place := false
	for _, k := range append(set, reset...) {
		before, isSet := f.repo.Settings[k.Name]
		if !isSet {
			before = "not set"
			if k.Default != "" {
				before = "default " + k.Default
			}
		}
		after := repoValueShown(k, f.pending[k.Name].value)
		if f.pending[k.Name].reset {
			after = "reset"
			if k.Default != "" {
				after += " (" + k.Default + ")"
			}
		}
		lines = append(lines, fmt.Sprintf("  %s: %s → %s", k.Name, before, styleHealthYellow.Render(after)))
		place = place || k.Place
	}
	lines = append(lines, "", styleDim.Render("Runs, values bound as parameters:"))
	for _, s := range f.stmts() {
		for _, l := range wrapText(s.SQL, width-2) {
			lines = append(lines, "  "+l)
		}
	}
	lines = append(lines, "")
	add := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, width) {
			lines = append(lines, style.Render(l))
		}
	}
	if r := f.guard.refusal("ALTER"); r != "" {
		add(styleHealthRed, "Can't run now: "+r+".")
		return append(lines, "", styleDim.Render("[n/Esc] back"))
	}
	if place {
		add(styleHealthYellow, "▲ This changes where the snapshots live: the tab will list what is at the new place. The snapshots at the old one stay there.")
	}
	if n := f.guard.elsewhereNote(); n != "" {
		add(styleHealthYellow, "▲ "+n)
	}
	add(styleDim, failedAlterNote)
	return append(lines, "", styleDim.Render("[y] run   [n/Esc] back"))
}

// readOnlyToggle sets readonly after a confirm saying what changes.
type readOnlyToggle struct {
	repo  cratedb.Repository
	to    bool
	guard repoGuard
}

func (t *readOnlyToggle) key(msg tea.KeyMsg) (bool, tea.Cmd) {
	switch msg.String() {
	case "y":
		if t.guard.refusal("ALTER") != "" {
			return false, nil
		}
		v := strconv.FormatBool(t.to)
		mode := "writable"
		if t.to {
			mode = "read-only"
		}
		apply := RepoApplyMsg{Repo: t.repo.Name, Alter: true, Before: t.repo.Settings, Changed: []string{"readonly"},
			Stmts: []repoStmt{{SQL: fmt.Sprintf(`ALTER REPOSITORY %s SET ("readonly" = ?)`, cratedb.QuoteIdent(t.repo.Name)), Args: []interface{}{v}}},
			Done:  fmt.Sprintf("repository %s is %s", t.repo.Name, mode)}
		return true, func() tea.Msg { return apply }
	case "n", "esc", "q":
		return true, nil
	}
	return false, nil
}

func (t *readOnlyToggle) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 70, 60)
	mode := "writable"
	if t.to {
		mode = "read-only"
	}
	lines := []string{styleModalTitle.Render(fmt.Sprintf("Make %s %s?", t.repo.Name, mode)), ""}
	add := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, innerWidth) {
			lines = append(lines, style.Render(l))
		}
	}
	if t.to {
		add(lipgloss.NewStyle(), "CREATE SNAPSHOT and DROP SNAPSHOT into this repository fail from now on, scheduled snapshots included. Listing and RESTORE keep working.")
	} else {
		add(lipgloss.NewStyle(), "CrateDB writes snapshots here again. Only one cluster may write to a repository: if another cluster writes to the same location, the two corrupt each other's snapshots. obsi can't check that.")
	}
	lines = append(lines, "", fmt.Sprintf(`  ALTER REPOSITORY %s SET ("readonly" = %t)`, cratedb.QuoteIdent(t.repo.Name), t.to), "")
	if r := t.guard.refusal("ALTER"); r != "" {
		add(styleHealthRed, "Can't run now: "+r+".")
		return placeModal(lipgloss.JoinVertical(lipgloss.Left, append(lines, "", styleDim.Render("[Esc] close"))...), innerWidth, width, height)
	}
	if n := t.guard.elsewhereNote(); n != "" {
		add(styleHealthYellow, "▲ "+n)
	}
	add(styleDim, failedAlterNote)
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, append(lines, "", styleDim.Render("[y] run   [n/Esc] cancel"))...), innerWidth, width, height)
}

// repoDrop drops a repository after its name is typed.
type repoDrop struct {
	repo      cratedb.Repository
	snapshots int
	guard     repoGuard
	typed     string
}

func (d *repoDrop) key(msg tea.KeyMsg) (bool, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		return true, nil
	case tea.KeyEnter:
		if d.typed != d.repo.Name || d.guard.refusal("DROP") != "" {
			return false, nil
		}
		apply := RepoApplyMsg{Repo: d.repo.Name, Stmts: []repoStmt{{SQL: "DROP REPOSITORY " + cratedb.QuoteIdent(d.repo.Name)}},
			Done: fmt.Sprintf("repository %s dropped; its snapshots are still in storage", d.repo.Name)}
		return true, func() tea.Msg { return apply }
	case tea.KeyBackspace:
		if r := []rune(d.typed); len(r) > 0 {
			d.typed = string(r[:len(r)-1])
		}
	case tea.KeyRunes:
		d.typed += string(msg.Runes)
	}
	return false, nil
}

// recreate is the CREATE REPOSITORY that lists the snapshots again, with
// placeholders for the credentials CrateDB doesn't show.
func recreateRepo(r cratedb.Repository) string {
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(r.Settings)) {
		v := r.Settings[k]
		if v == cratedb.MaskedValue {
			v = "<" + k + ">"
		}
		parts = append(parts, fmt.Sprintf("%s = '%s'", k, strings.ReplaceAll(v, "'", "''")))
	}
	stmt := fmt.Sprintf("CREATE REPOSITORY %s TYPE %s", cratedb.QuoteIdent(r.Name), r.Type)
	if len(parts) > 0 {
		stmt += " WITH (" + strings.Join(parts, ", ") + ")"
	}
	return stmt
}

func (d *repoDrop) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 75, 60)
	lines := []string{styleModalTitle.Render("Drop repository " + d.repo.Name + "?"), ""}
	add := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, innerWidth) {
			lines = append(lines, style.Render(l))
		}
	}
	add(lipgloss.NewStyle(), fmt.Sprintf("DROP REPOSITORY removes only the definition: the %d %s listed stay in storage, and creating the repository again on the same location lists them again:",
		d.snapshots, plural(d.snapshots, "snapshot", "snapshots")))
	lines = append(lines, "")
	for _, l := range wrapText(recreateRepo(d.repo), innerWidth-2) {
		lines = append(lines, "  "+styleDim.Render(l))
	}
	lines = append(lines, "")
	if r := d.guard.refusal("DROP"); r != "" {
		add(styleHealthRed, "Can't run now: "+r+".")
		return placeModal(lipgloss.JoinVertical(lipgloss.Left, append(lines, "", styleDim.Render("[Esc] close"))...), innerWidth, width, height)
	}
	add(lipgloss.NewStyle(), "Type the repository's name to drop it:")
	lines = append(lines, "  "+styleEditInput.Render(d.typed+"▏"), "", styleDim.Render("[Enter] drop   [Esc] cancel"))
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, lines...), innerWidth, width, height)
}

// repoUndo offers to set back what a failed ALTER left. Masked values
// can't be read back, so for those it asks.
type repoUndo struct {
	res    RepoResultMsg
	keys   []string          // changed keys whose value now differs from before
	asked  map[string]string // masked keys: the previous value as typed
	cursor int               // over the masked keys
	typing bool
}

func newRepoUndo(res RepoResultMsg) *repoUndo {
	return &repoUndo{res: res, keys: res.undoKeys(), asked: map[string]string{}}
}

func (u *repoUndo) masked() []string {
	var out []string
	for _, k := range u.keys {
		if u.res.Before[k] == cratedb.MaskedValue {
			out = append(out, k)
		}
	}
	return out
}

// stmts sets back what it can, and names the keys it sends: a masked key
// nobody typed a value for is left out.
func (u *repoUndo) stmts() ([]repoStmt, []string) {
	name := cratedb.QuoteIdent(u.res.Repo)
	var set, reset, sent []string
	var args []interface{}
	for _, k := range u.keys {
		before, wasSet := u.res.Before[k]
		switch {
		case !wasSet:
			reset = append(reset, cratedb.QuoteIdent(k))
		case before == cratedb.MaskedValue:
			v := u.asked[k]
			if v == "" {
				continue
			}
			set = append(set, cratedb.QuoteIdent(k)+" = ?")
			args = append(args, v)
		default:
			set = append(set, cratedb.QuoteIdent(k)+" = ?")
			args = append(args, before)
		}
		sent = append(sent, k)
	}
	var out []repoStmt
	if len(set) > 0 {
		out = append(out, repoStmt{SQL: fmt.Sprintf("ALTER REPOSITORY %s SET (%s)", name, strings.Join(set, ", ")), Args: args})
	}
	if len(reset) > 0 {
		out = append(out, repoStmt{SQL: fmt.Sprintf("ALTER REPOSITORY %s RESET (%s)", name, strings.Join(reset, ", "))})
	}
	return out, sent
}

func (u *repoUndo) key(msg tea.KeyMsg) (bool, tea.Cmd) {
	masked := u.masked()
	if u.typing {
		k := masked[u.cursor]
		switch msg.Type {
		case tea.KeyEsc, tea.KeyEnter:
			u.typing = false
		case tea.KeyBackspace:
			if r := []rune(u.asked[k]); len(r) > 0 {
				u.asked[k] = string(r[:len(r)-1])
			}
		case tea.KeyRunes:
			u.asked[k] += string(msg.Runes)
		}
		return false, nil
	}
	switch msg.String() {
	case "esc", "q":
		return true, nil
	case "up", "k":
		u.cursor = max(u.cursor-1, 0)
	case "down", "j":
		u.cursor = min(u.cursor+1, max(len(masked)-1, 0))
	case "enter":
		if len(masked) > 0 {
			u.typing = true
		}
	case "u":
		stmts, sent := u.stmts()
		if len(stmts) == 0 {
			return false, nil
		}
		apply := RepoApplyMsg{Repo: u.res.Repo, Stmts: stmts, Alter: true, Before: u.res.Now, Changed: sent,
			Done: fmt.Sprintf("repository %s: previous settings set back", u.res.Repo)}
		return true, func() tea.Msg { return apply }
	}
	return false, nil
}

func (u *repoUndo) render(width, height int) string {
	innerWidth := modalInnerWidth(width, 80, 60)
	lines := []string{styleModalTitle.Render("ALTER REPOSITORY " + u.res.Repo + " failed"), ""}
	add := func(style lipgloss.Style, text string) {
		for _, l := range wrapText(text, innerWidth) {
			lines = append(lines, style.Render(l))
		}
	}
	add(styleHealthRed, u.res.Error)
	lines = append(lines, "")
	add(lipgloss.NewStyle(), "CrateDB kept the new settings anyway. sys.repositories now, and what it had before:")
	masked := u.masked()
	for _, k := range u.keys {
		before, wasSet := u.res.Before[k]
		if !wasSet {
			before = "not set"
		}
		now, isSet := u.res.Now[k]
		if !isSet {
			now = "not set"
		}
		row := fmt.Sprintf("  %s: %s, before %s", k, styleHealthYellow.Render(now), before)
		if i := slices.Index(masked, k); i >= 0 {
			v := strings.Repeat("•", min(len(u.asked[k]), 12))
			if u.typing && i == u.cursor {
				v = styleEditInput.Render(v + "▏")
			} else if v == "" {
				v = styleDim.Render("(type it, or it stays as it is)")
			}
			marker := "  "
			if i == u.cursor {
				marker = stylePickerActive.Render("▸ ")
			}
			row = fmt.Sprintf("%s%s: %s, before masked; previous value: %s", marker, k, styleHealthYellow.Render(now), v)
		}
		lines = append(lines, row)
	}
	if len(masked) > 0 {
		lines = append(lines, "")
		add(styleDim, "CrateDB doesn't show credentials, so obsi can't set them back by itself.")
	}
	lines = append(lines, "")
	if stmts, _ := u.stmts(); len(stmts) == 0 {
		add(styleDim, "Nothing obsi can set back without the previous values.")
	} else {
		add(styleDim, "Setting them back verifies again and can fail the same way (e.g. the storage is unreachable); the previous settings stay either way.")
	}
	keys := "[u] set the previous settings back   [Esc] keep them as they are"
	if len(masked) > 0 {
		keys = "[↑↓] select   [Enter] type a previous value   " + keys
	}
	if u.typing {
		keys = "[Enter] done"
	}
	return placeModal(lipgloss.JoinVertical(lipgloss.Left, append(lines, "", styleDim.Render(keys))...), innerWidth, width, height)
}
