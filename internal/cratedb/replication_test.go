package cratedb

import "testing"

func TestSplitRegclass(t *testing.T) {
	for in, want := range map[string][2]string{
		`doc.t`:             {"doc", "t"},
		`"My Schema"."a.b"`: {"My Schema", "a.b"},
		`s."say ""hi"""`:    {"s", `say "hi"`},
		`t`:                 {"doc", "t"},
	} {
		if s, tb := SplitRegclass(in); s != want[0] || tb != want[1] {
			t.Errorf("SplitRegclass(%q) = %q, %q", in, s, tb)
		}
	}
}
