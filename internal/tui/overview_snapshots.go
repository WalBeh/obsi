package tui

import (
	"fmt"
	"strings"
	"time"
)

// renderSnapshots says on the Overview whether snapshots are taken and
// how the newest one per repository went; the Snapshots tab has the
// details.
func (m OverviewModel) renderSnapshots() string {
	st := m.snap.Snapshots
	title := sectionTitle("Snapshots")
	if m.snap.Staleness["snapshots"] {
		title += " " + styleStale.Render("(stale)")
	}
	if err := st.Err + st.ListErr; err != "" {
		title += " " + styleHealthRed.Render("(last poll failed: "+truncateString(firstLine(err), 60)+")")
	}
	switch {
	case st.UpdatedAt.IsZero():
		return title + "\n" + styleStale.Render("  (loading...)")
	case len(st.Repositories) == 0:
		return title + "\n" + styleHealthYellow.Render("  no snapshot repository configured")
	case st.ListedAt.IsZero():
		return title + "\n" + styleStale.Render(fmt.Sprintf("  %d %s, listing snapshots...", len(st.Repositories), plural(len(st.Repositories), "repository", "repositories")))
	}
	var parts []string
	running := 0
	newest := map[string]bool{}
	for _, s := range st.Snapshots {
		if s.State == "IN_PROGRESS" {
			running++
			continue
		}
		if newest[s.Repository] {
			continue
		}
		newest[s.Repository] = true
		p := s.Repository + " " + snapshotStateStyle(s.State).Render(s.State)
		if !s.Finished.IsZero() {
			p += " " + ageShort(time.Since(s.Finished))
		}
		parts = append(parts, p)
	}
	for _, r := range st.Repositories {
		if !newest[r.Name] {
			parts = append(parts, r.Name+" "+styleHealthYellow.Render("no snapshots"))
		}
	}
	if len(parts) > 3 {
		parts = append(parts[:3], fmt.Sprintf("+%d", len(parts)-3))
	}
	line := strings.Join(parts, " · ")
	if running > 0 {
		line += fmt.Sprintf(" · %d running", running)
	}
	n := 0
	for _, r := range st.Restores {
		if _, copying := r.Subscription(); !copying {
			n++
		}
	}
	if n > 0 {
		line += " · " + styleHealthYellow.Render(fmt.Sprintf("%d %s", n, plural(n, "restore", "restores")))
	}
	return title + "\n  " + line + styleDim.Render("  (8: details)")
}

// ageShort is how long ago, coarse: 40m ago, 5h ago, 3d ago.
func ageShort(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "<1m ago"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}
