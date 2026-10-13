package tui

import (
	"reflect"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func typeModal(m replModal, keys ...string) tea.Cmd {
	var cmd tea.Cmd
	for _, k := range keys {
		var msg tea.KeyMsg
		switch k {
		case "enter":
			msg = tea.KeyMsg{Type: tea.KeyEnter}
		case "esc":
			msg = tea.KeyMsg{Type: tea.KeyEsc}
		case "down":
			msg = tea.KeyMsg{Type: tea.KeyDown}
		case "up":
			msg = tea.KeyMsg{Type: tea.KeyUp}
		case "backspace":
			msg = tea.KeyMsg{Type: tea.KeyBackspace}
		default:
			msg = tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(k)}
		}
		_, cmd = m.key(msg)
	}
	return cmd
}

func TestCheckRepoValue(t *testing.T) {
	key := func(kind string) cratedb.RepositoryKey { return cratedb.RepositoryKey{Name: "k", Kind: kind} }
	for _, c := range []struct {
		kind, v string
		ok      bool
	}{
		{"bool", "true", true}, {"bool", "yes", false},
		{"int", "5", true}, {"int", "abc", false}, {"int", "-1", false},
		{"size", "64mb", true}, {"size", "0", true}, {"size", "lots", false},
		{"time", "30s", true}, {"time", "0", true}, {"time", "soon", false},
		{"protocol", "http", true}, {"protocol", "ftp", false},
		{"text", "anything", true},
	} {
		if got := checkRepoValue(key(c.kind), c.v) == ""; got != c.ok {
			t.Errorf("checkRepoValue(%s, %q) ok = %v, want %v", c.kind, c.v, got, c.ok)
		}
	}
}

func s3Repo() cratedb.Repository {
	return cratedb.Repository{Name: "lab_s3", Type: "s3", Settings: map[string]string{
		"bucket": "b", "endpoint": "s3:9000", "access_key": "[xxxxx]", "secret_key": "[xxxxx]", "max_retries": "5"}}
}

// keyIndex moves the form's cursor to a key.
func keyIndex(t *testing.T, f *repoForm, name string) {
	t.Helper()
	for i, k := range f.keys {
		if k.Name == name {
			f.cursor = i
			return
		}
	}
	t.Fatalf("no key %s", name)
}

func TestRepoForm(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	f := newRepoForm(s3Repo(), repoGuard{minVersion: "6.4.5", elsewhere: []string{"other.nightly"}})
	keyIndex(t, f, "max_retries")
	typeModal(f, "enter", "backspace", "x", "enter")
	if !strings.Contains(f.err, "whole number") || !f.editing {
		t.Fatalf("bad value accepted: err %q", f.err)
	}
	typeModal(f, "backspace", "7", "enter")
	keyIndex(t, f, "secret_key")
	typeModal(f, "enter", "s3cr3t", "enter")
	keyIndex(t, f, "endpoint")
	typeModal(f, "x")
	keyIndex(t, f, "region")
	typeModal(f, "x")
	if !strings.Contains(f.err, "isn't set") {
		t.Errorf("reset of an unset key: err %q", f.err)
	}
	if out := f.render(140, 60); strings.Contains(out, "s3cr3t") || !strings.Contains(out, "→ ••••••") {
		t.Errorf("secret shown in the form:\n%s", out)
	}
	typeModal(f, "r")
	out := f.render(140, 60)
	for _, want := range []string{
		"max_retries: 5 → 7", "secret_key: [xxxxx] → ••••••", "endpoint: s3:9000 → reset",
		`ALTER REPOSITORY "lab_s3" SET ("secret_key" = ?, "max_retries" = ?)`, `ALTER REPOSITORY "lab_s3" RESET ("endpoint")`,
		"changes where the snapshots live", "other.nightly", "the new settings stay",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("review missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "s3cr3t") {
		t.Errorf("secret in the review:\n%s", out)
	}
	apply := typeModal(f, "y")().(RepoApplyMsg)
	if !apply.Alter || !reflect.DeepEqual(apply.Changed, []string{"endpoint", "max_retries", "secret_key"}) || apply.Before["endpoint"] != "s3:9000" {
		t.Errorf("apply = %+v", apply)
	}
	if !reflect.DeepEqual(apply.Stmts[0].Args, []interface{}{"s3cr3t", "7"}) || len(apply.Stmts) != 2 {
		t.Errorf("stmts = %+v", apply.Stmts)
	}
}

func TestRepoFormRefusesWhileInUse(t *testing.T) {
	f := newRepoForm(s3Repo(), repoGuard{here: []string{"snapshot nightly"}})
	keyIndex(t, f, "max_retries")
	typeModal(f, "enter", "backspace", "6", "enter", "r")
	if out := f.render(140, 60); !strings.Contains(out, "Can't run now: in use (snapshot nightly)") || strings.Contains(out, "[y] run") {
		t.Errorf("review:\n%s", out)
	}
	if cmd := typeModal(f, "y"); cmd != nil {
		t.Error("ran while in use")
	}
	f = newRepoForm(s3Repo(), repoGuard{minVersion: "6.3.2"})
	if r := f.guard.refusal("ALTER"); !strings.Contains(r, "6.4.0") {
		t.Errorf("old node: %q", r)
	}
	f = newRepoForm(cratedb.Repository{Name: "r", Type: "fs", Settings: map[string]string{"location": "/a"}}, repoGuard{})
	keyIndex(t, f, "location")
	typeModal(f, "x")
	if !strings.Contains(f.err, "required") {
		t.Errorf("reset of location: err %q", f.err)
	}
}

func TestReadOnlyToggle(t *testing.T) {
	m := snapshotsModel(t) // lab_fs: read-only, a restore running from it
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("w")})
	tg, ok := m.modal.(*readOnlyToggle)
	if !ok || tg.to {
		t.Fatalf("w on read-only lab_fs: %+v", m.modal)
	}
	if out := m.View(); !strings.Contains(out, "Only one cluster may write to a repository") || !strings.Contains(out, "in use (restore of nightly)") {
		t.Errorf("writable warning and in-use refusal:\n%s", out)
	}
	if typeModal(tg, "y") != nil {
		t.Error("ran while a restore runs")
	}
	m.modal, m.selected = nil, 2 // lab_empty, writable and idle
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("w")})
	if out := m.View(); !strings.Contains(out, "CREATE SNAPSHOT and DROP SNAPSHOT into this repository fail") {
		t.Errorf("read-only warning:\n%s", out)
	}
	apply := typeModal(m.modal, "y")().(RepoApplyMsg)
	if apply.Stmts[0].SQL != `ALTER REPOSITORY "lab_empty" SET ("readonly" = ?)` || apply.Stmts[0].Args[0] != "true" {
		t.Errorf("apply = %+v", apply)
	}
	m.modal = nil
	m.st.Repositories = append(m.st.Repositories, cratedb.Repository{Name: "web", Type: "url"})
	m.selected = len(m.st.Repositories) - 1
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("w")})
	if m.modal != nil || !strings.Contains(m.View(), "has no readonly setting") {
		t.Errorf("w on url:\n%s", m.View())
	}
	m.readOnly, m.selected = true, 0
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("e")})
	if m.modal != nil || !strings.Contains(m.View(), "read-only:") {
		t.Errorf("e in read-only mode:\n%s", m.View())
	}
}

func TestRepoDrop(t *testing.T) {
	d := &repoDrop{repo: s3Repo(), snapshots: 3}
	out := d.render(140, 60)
	for _, want := range []string{"the 3 snapshots listed stay in storage",
		`CREATE REPOSITORY "lab_s3" TYPE s3 WITH (access_key = '<access_key>', bucket = 'b'`} {
		if !strings.Contains(strings.Join(strings.Fields(out), " "), want) {
			t.Errorf("drop dialog missing %q:\n%s", want, out)
		}
	}
	if cmd := typeModal(d, "lab_s", "enter"); cmd != nil {
		t.Error("dropped with a partial name")
	}
	apply := typeModal(d, "3", "enter")().(RepoApplyMsg)
	if apply.Stmts[0].SQL != `DROP REPOSITORY "lab_s3"` || apply.Alter {
		t.Errorf("apply = %+v", apply)
	}
}

func TestRepoUndo(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	res := RepoResultMsg{Repo: "lab_s3", Error: "Unable to verify the repository", Stored: true, Undo: true,
		Changed: []string{"endpoint", "region", "secret_key", "max_retries"},
		Before:  map[string]string{"endpoint": "s3:9000", "secret_key": "[xxxxx]", "max_retries": "5"},
		Now:     map[string]string{"endpoint": "s3:9999", "region": "eu", "secret_key": "[xxxxx]", "max_retries": "5"}}
	if got := res.undoKeys(); !reflect.DeepEqual(got, []string{"endpoint", "region", "secret_key"}) {
		t.Errorf("undoKeys = %v (max_retries didn't change)", got)
	}
	if res.Stored = false; len(res.undoKeys()) != 2 {
		t.Errorf("not stored: undoKeys = %v, masked keys don't count", res.undoKeys())
	}
	res.Stored = true
	u := newRepoUndo(res)
	out := u.render(140, 60)
	for _, want := range []string{"CrateDB kept the new settings", "endpoint: s3:9999, before s3:9000", "region: eu, before not set", "secret_key: [xxxxx], before masked"} {
		if !strings.Contains(out, want) {
			t.Errorf("undo missing %q:\n%s", want, out)
		}
	}
	// Without the old secret: endpoint set back, region reset, secret left.
	if s := u.stmts(); len(s) != 2 || s[0].SQL != `ALTER REPOSITORY "lab_s3" SET ("endpoint" = ?)` || s[1].SQL != `ALTER REPOSITORY "lab_s3" RESET ("region")` {
		t.Errorf("stmts = %+v", s)
	}
	typeModal(u, "enter", "old", "enter")
	apply := typeModal(u, "u")().(RepoApplyMsg)
	if s := apply.Stmts[0]; s.SQL != `ALTER REPOSITORY "lab_s3" SET ("endpoint" = ?, "secret_key" = ?)` || !reflect.DeepEqual(s.Args, []interface{}{"s3:9000", "old"}) {
		t.Errorf("undo stmt = %+v", s)
	}
	if !apply.Alter || apply.Before["endpoint"] != "s3:9999" {
		t.Errorf("undo of the undo: %+v", apply)
	}
}
