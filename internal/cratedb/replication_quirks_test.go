package cratedb

import "testing"

func TestQuirkAffects(t *testing.T) {
	open := Quirk{SeenOn: "6.4.5"}
	fixed := Quirk{SeenOn: "6.4.5", FixedIn: "6.4.7"}
	for _, c := range []struct {
		q       Quirk
		version string
		want    bool
	}{
		{open, "6.4.5", true}, {open, "7.0.0", true}, {open, "", true},
		{fixed, "6.4.5", true}, {fixed, "6.4.7", false}, {fixed, "6.10.0", false}, {fixed, "", true},
	} {
		if got := c.q.Affects(c.version); got != c.want {
			t.Errorf("%+v.Affects(%q) = %v", c.q, c.version, got)
		}
	}
}
