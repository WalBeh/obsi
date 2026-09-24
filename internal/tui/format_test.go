package tui

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateString(t *testing.T) {
	cases := []struct {
		in   string
		max  int
		want string
	}{
		{"SELECT 1", 10, "SELECT 1"},
		{"abcdefghijkl", 10, "abcdefg..."},
		// Byte-slicing cut these mid-character (or shortened them at all
		// although they fit in max characters).
		{"größe_tabelle", 13, "größe_tabelle"},
		{"SELECT * FROM größe WHERE x", 14, "SELECT * FR..."},
		{"≥4.9s", 10, "≥4.9s"},
		{"表名表名表名表名表名表名", 8, "表名表名表..."},
	}
	for _, c := range cases {
		got := truncateString(c.in, c.max)
		if got != c.want {
			t.Errorf("truncateString(%q, %d) = %q, want %q", c.in, c.max, got, c.want)
		}
		if !utf8.ValidString(got) {
			t.Errorf("truncateString(%q, %d) produced invalid UTF-8", c.in, c.max)
		}
	}
}

func TestWrapTextUTF8(t *testing.T) {
	in := "SELECT größe, öl FROM tabelle WHERE ä = 'ü' AND name = '表名表名表名表名'"
	lines := wrapText(in, 12)
	for _, l := range lines {
		if !utf8.ValidString(l) {
			t.Fatalf("invalid UTF-8 line %q", l)
		}
		if n := utf8.RuneCountInString(l); n > 12 {
			t.Errorf("line %q has %d characters, want <= 12", l, n)
		}
	}
	if got := strings.Join(lines, " "); strings.ReplaceAll(got, " ", "") != strings.ReplaceAll(in, " ", "") {
		t.Errorf("wrapping lost characters:\n%s", strings.Join(lines, "\n"))
	}
	// ASCII behaviour unchanged: break at the last space that fits.
	if got := strings.Join(wrapText("aaa bbb ccc ddd", 10), "|"); got != "aaa bbb|ccc ddd" {
		t.Errorf("ascii wrap = %q", got)
	}
}
