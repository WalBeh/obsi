package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/collector"
)

var (
	styleOverlayBorder = lipgloss.NewStyle().
		BorderTop(true).
		BorderStyle(lipgloss.DoubleBorder()).
		BorderForeground(colorDim)

	styleOverlayErr = lipgloss.NewStyle().
		Foreground(colorRed)

	styleOverlayDelta = lipgloss.NewStyle().
		Foreground(colorGreen)

	styleOverlayDotActive = lipgloss.NewStyle().
		Foreground(colorGreen)

	styleOverlayDotRecent = lipgloss.NewStyle().
		Foreground(colorDim)
)

// queryActivity tracks per-label change detection for visual cues.
type queryActivity struct {
	prevExecCount int64
	delta         int64     // how many new executions since last change
	changedAt     time.Time // when the delta was last detected
}

// QueryLogOverlay renders a bottom-drawer showing query execution stats.
type QueryLogOverlay struct {
	width     int
	height    int // allocated height for the overlay
	stats     []collector.QueryStat
	throttle  collector.ThrottleLevel
	sorted    bool
	sortOrder []string // cached label order after first sort
	activity  map[string]*queryActivity
}

// SetSize updates the available dimensions.
func (o *QueryLogOverlay) SetSize(width, height int) {
	o.width = width
	o.height = height
}

// Refresh updates the overlay data from the tracker.
func (o *QueryLogOverlay) Refresh(tracker *collector.QueryTracker, throttle collector.ThrottleLevel) {
	o.stats = tracker.Snapshot()
	o.throttle = throttle

	// Detect exec count changes for visual activity cues.
	if o.activity == nil {
		o.activity = make(map[string]*queryActivity, len(o.stats))
	}
	now := time.Now()
	for _, s := range o.stats {
		a, ok := o.activity[s.Label]
		if !ok {
			a = &queryActivity{prevExecCount: s.ExecCount}
			o.activity[s.Label] = a
			continue
		}
		if s.ExecCount != a.prevExecCount {
			a.delta = s.ExecCount - a.prevExecCount
			a.changedAt = now
			a.prevExecCount = s.ExecCount
		}
	}

	// Sort only on first load — order is stable (category+label never change).
	if !o.sorted {
		sort.Slice(o.stats, func(i, j int) bool {
			if o.stats[i].Category != o.stats[j].Category {
				return o.stats[i].Category < o.stats[j].Category
			}
			return o.stats[i].Label < o.stats[j].Label
		})
		if len(o.stats) > 0 {
			o.sortOrder = make([]string, len(o.stats))
			for i, s := range o.stats {
				o.sortOrder[i] = s.Label
			}
			o.sorted = true
		}
	} else {
		// Re-order by cached sort order
		byLabel := make(map[string]collector.QueryStat, len(o.stats))
		for _, s := range o.stats {
			byLabel[s.Label] = s
		}
		for i, label := range o.sortOrder {
			o.stats[i] = byLabel[label]
		}
	}
}

// Height returns how many terminal rows the overlay wants.
func (o *QueryLogOverlay) Height() int {
	// title + header + rows + hint line
	rows := len(o.stats) + 3
	if rows > o.height {
		return o.height
	}
	return rows
}

// View renders the overlay content.
func (o *QueryLogOverlay) View() string {
	if o.width < 40 {
		return ""
	}

	mult := collector.ThrottleMultiplier(o.throttle)
	throttleName := collector.ThrottleName(o.throttle)

	// Column widths
	const (
		colLabel    = 26
		colExec     = 7
		colDelta    = 4
		colLastRun  = 12
		colDur      = 9
		colAvgRows  = 9
		colInterval = 14
		colErr      = 6
	)

	title := styleTitle.Render(fmt.Sprintf("── Query Log ── throttle: %s ──", throttleName))

	header := fmt.Sprintf("  . %-*s %*s %-*s %*s %*s %*s %-*s %*s",
		colLabel, "QUERY",
		colExec, "#EXEC",
		colDelta, "",
		colLastRun, "LAST RUN",
		colDur, "DURATION",
		colAvgRows, "AVG ROWS",
		colInterval, "EFF. INTERVAL",
		colErr, "ERRS",
	)
	header = styleTitle.Render(header)

	lines := make([]string, 0, len(o.stats)+3)
	lines = append(lines, title)
	lines = append(lines, header)

	now := time.Now()
	for _, s := range o.stats {
		lastRun := "—"
		if !s.LastExec.IsZero() {
			ago := now.Sub(s.LastExec)
			lastRun = formatAgo(ago)
		}

		dur := "—"
		if s.ExecCount > 0 && !s.LastExec.IsZero() {
			dur = formatDuration(s.LastDur)
		}

		avgRows := "—"
		if s.ExecCount > 0 {
			avgRows = fmt.Sprintf("%.1f", s.AvgRows())
		}

		effInterval := "—"
		if s.Interval > 0 {
			eff := s.Interval * time.Duration(mult)
			effInterval = formatDuration(eff)
			if mult > 1 {
				effInterval += fmt.Sprintf(" (%dx)", mult)
			}
		} else {
			effInterval = "on-demand"
		}

		errStr := fmt.Sprintf("%d", s.ErrCount)
		errStyle := styleDim
		if s.ErrCount > 0 {
			errStyle = styleOverlayErr
		}

		// Recency dot: green < 1s, dim < 5s, space otherwise
		dot := " "
		if a, ok := o.activity[s.Label]; ok && !a.changedAt.IsZero() {
			age := now.Sub(a.changedAt)
			if age < 1*time.Second {
				dot = styleOverlayDotActive.Render("●")
			} else if age < 5*time.Second {
				dot = styleOverlayDotRecent.Render("●")
			}
		}

		// Delta suffix: show +N in green for 1s after change
		delta := fmt.Sprintf("%-*s", colDelta, "")
		if a, ok := o.activity[s.Label]; ok && a.delta > 0 {
			age := now.Sub(a.changedAt)
			if age < 1*time.Second {
				delta = styleOverlayDelta.Render(fmt.Sprintf("+%-*d", colDelta-1, a.delta))
			}
		}

		line := fmt.Sprintf("  %s %-*s %*d %s %*s %*s %*s %-*s %s",
			dot,
			colLabel, s.Label,
			colExec, s.ExecCount,
			delta,
			colLastRun, lastRun,
			colDur, dur,
			colAvgRows, avgRows,
			colInterval, effInterval,
			errStyle.Render(fmt.Sprintf("%*s", colErr, errStr)),
		)
		lines = append(lines, line)
	}

	lines = append(lines, styleDim.Render("  Press L to close"))

	content := strings.Join(lines, "\n")
	return styleOverlayBorder.Width(o.width).Render(content)
}

// formatAgo formats a duration as a human-readable "ago" string.
func formatAgo(d time.Duration) string {
	switch {
	case d < time.Second:
		return "<1s ago"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds ago", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%dm ago", int(d.Hours()), int(d.Minutes())%60)
	}
}
