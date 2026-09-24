package tui

import (
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func alertStyle(a store.Alert) lipgloss.Style {
	switch {
	case !a.Firing():
		return styleDim
	case a.Level == store.AlertCrit:
		return styleHealthRed
	default:
		return styleHealthYellow
	}
}

// newestFiring is the alert shown next to the tabs.
func newestFiring(st store.AlertsState) (store.Alert, bool) {
	for _, a := range st.History {
		if a.Firing() {
			return a, true
		}
	}
	return store.Alert{}, false
}

// alertBanner is the newest firing alert, cut to fit width.
func alertBanner(st store.AlertsState, width int) string {
	a, ok := newestFiring(st)
	if !ok || width < 10 {
		return ""
	}
	text := "⚠ " + a.Message
	if st.Firing > 1 {
		text += fmt.Sprintf(" (+%d, a:alerts)", st.Firing-1)
	}
	return alertStyle(a).Render(truncateString(text, width))
}

func alertRows(st store.AlertsState, width int) []string {
	rows := make([]string, 0, len(st.History))
	for _, a := range st.History {
		state := "firing"
		if !a.Firing() {
			state = "cleared after " + formatDuration(a.Cleared.Sub(a.Raised).Round(time.Second))
		}
		line := fmt.Sprintf("%s  %-22s  %s", a.Raised.Format("15:04:05"), state, a.Message)
		rows = append(rows, alertStyle(a).Render(truncateString(line, width)))
	}
	return rows
}

// renderAlerts draws the alert history as a modal, newest first.
func renderAlerts(st store.AlertsState, scroll, width, height int) string {
	inner := modalInnerWidth(width, 80, 60)
	title := styleModalTitle.Render(fmt.Sprintf("Alerts: %d firing", st.Firing))
	rows := alertRows(st, inner)
	if len(rows) == 0 {
		rows = []string{styleDim.Render("nothing raised since obsi started")}
	}
	visible := max(height-8, 3)
	scroll = min(scroll, max(len(rows)-visible, 0))
	end := min(scroll+visible, len(rows))

	lines := []string{title, ""}
	lines = append(lines, rows[scroll:end]...)
	footer := "[a/esc] close"
	if len(rows) > visible {
		footer = fmt.Sprintf("[↑↓] scroll  %d-%d of %d  ", scroll+1, end, len(rows)) + footer
	}
	lines = append(lines, "", styleDim.Render(footer))
	return placeModal(strings.Join(lines, "\n"), inner, width, height)
}

// bell rings the terminal. Writing BEL straight to the tty is the only way
// past bubbletea's renderer.
func bell() tea.Msg {
	fmt.Fprint(os.Stderr, "\a")
	return nil
}
