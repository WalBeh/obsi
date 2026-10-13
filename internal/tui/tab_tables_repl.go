package tui

import (
	"slices"
	"strings"
)

// replMark is the marker after a table name on the Tables list: ⇠ this
// cluster pulls it (red when it failed), ⇢ it publishes it.
func (m TablesModel) replMark(fqn string) string {
	for _, s := range m.snap.Replication.Subscriptions {
		for _, t := range s.Tables {
			if t.FQN() != fqn {
				continue
			}
			if t.State == "e" {
				return styleHealthRed.Render("⇠")
			}
			return styleHealthYellow.Render("⇠")
		}
	}
	for _, p := range m.snap.Replication.Publications {
		if slices.Contains(p.Tables, fqn) {
			return styleDim.Render("⇢")
		}
	}
	return ""
}

func (m TablesModel) anyReplMark() bool {
	for _, t := range m.snap.Tables {
		if m.replMark(t.SchemaName+"."+t.TableName) != "" {
			return true
		}
	}
	return false
}

// publishedLine says which publications have the table and which
// subscriber clusters hold leases on it.
func (m TablesModel) publishedLine(fqn string) string {
	var pubs, subs []string
	for _, p := range m.snap.Replication.Publications {
		if slices.Contains(p.Tables, fqn) {
			pubs = append(pubs, p.Name)
		}
	}
	for _, h := range m.snap.Replication.Subscribers {
		if slices.Contains(h.On, fqn) {
			subs = append(subs, h.Subscriber)
		}
	}
	if len(pubs) == 0 {
		return ""
	}
	line := "⇢ published in " + strings.Join(pubs, ", ")
	if len(subs) > 0 {
		line += ", pulled by " + strings.Join(subs, ", ")
	} else {
		line += ", no subscriber yet"
	}
	return line
}
