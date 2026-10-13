package tui

import (
	"fmt"
	"strings"
	"time"
)

// renderReplication says on the Overview that there is replication and
// whether it's healthy: what this cluster pulls and serves. Empty without
// replication; the Replication tab has the details.
func (m OverviewModel) renderReplication() string {
	r := m.snap.Replication
	var parts []string
	var subs []string
	tables, failed, copying := 0, 0, 0
	for _, s := range r.Subscriptions {
		subs = append(subs, s.Name)
		for _, t := range s.Tables {
			tables++
			switch t.State {
			case "e":
				failed++
			case "i", "d":
				copying++
			}
		}
	}
	if len(subs) > 0 {
		p := fmt.Sprintf("pulls %d %s (%s), ", tables, plural(tables, "table", "tables"), listMax(subs, 3))
		switch {
		case failed > 0:
			p += styleHealthRed.Render(fmt.Sprintf("%d failed", failed))
		case copying > 0:
			p += styleHealthYellow.Render(fmt.Sprintf("%d copying", copying))
		default:
			p += "all in sync"
		}
		parts = append(parts, p)
	}
	if len(r.Publications) > 0 || len(r.Subscribers) > 0 {
		var pubs []string
		for _, p := range r.Publications {
			pubs = append(pubs, p.Name)
		}
		p := "serves"
		if len(pubs) > 0 {
			p += " " + listMax(pubs, 3)
		}
		var names, stale []string
		var behind int64
		unknown := false
		now := time.Now()
		for _, h := range r.Subscribers {
			names = append(names, h.Subscriber)
			behind += h.Behind
			unknown = unknown || h.Unknown > 0
			if h.Period > 0 && now.After(h.Expires.Add(-h.Period/2)) {
				stale = append(stale, h.Subscriber)
			}
		}
		if len(names) == 0 {
			p += ", no subscriber"
		} else {
			b := formatRecords(behind)
			if unknown {
				b += "?"
			}
			p += " to " + listMax(names, 3) + ", " + b + " behind"
			if len(stale) > 0 {
				p += ", " + styleHealthYellow.Render(listMax(stale, 3)+" stopped pulling")
			}
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		return ""
	}
	return sectionTitle("Replication") + "\n  " + strings.Join(parts, " · ") + styleDim.Render("  (7: details)")
}

// listMax joins up to n names and counts the rest.
func listMax(names []string, n int) string {
	if len(names) <= n {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:n], ", ") + fmt.Sprintf(" +%d", len(names)-n)
}
