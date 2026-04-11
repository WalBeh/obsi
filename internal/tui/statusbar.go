package tui

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// StatusBarModel renders the connection status bar.
type StatusBarModel struct {
	status        cratedb.RegistryStatus
	throttle      collector.ThrottleLevel
	heapWarning   bool
	width         int
}

// NewStatusBarModel creates a new status bar.
func NewStatusBarModel(width int) StatusBarModel {
	return StatusBarModel{width: width}
}

// Refresh updates the status bar with new registry status.
func (m StatusBarModel) Refresh(status cratedb.RegistryStatus, throttle collector.ThrottleLevel, heapWarning bool) StatusBarModel {
	m.status = status
	m.throttle = throttle
	m.heapWarning = heapWarning
	return m
}

// SetWidth updates the status bar width.
func (m StatusBarModel) SetWidth(width int) StatusBarModel {
	m.width = width
	return m
}

// View renders the status bar.
func (m StatusBarModel) View() string {
	s := m.status

	// Connection indicator
	var connIndicator string
	switch {
	case s.Reconnecting:
		connIndicator = styleHealthYellow.Render("↻ RECONNECTING")
	case s.Connected:
		connIndicator = styleStatusConnected.Render("● CONNECTED")
	default:
		connIndicator = styleStatusDisconnected.Render("✗ DISCONNECTED")
	}

	// Connection path detail
	connPath := ""
	if s.PrimaryOK {
		connPath = " via LB"
		if s.DirectReachable {
			connPath += fmt.Sprintf("+direct(%d)", s.HealthyNodes)
		}
	} else if s.DirectReachable {
		connPath = fmt.Sprintf(" via direct(%d/%d)", s.HealthyNodes, s.TotalNodes)
	}
	if s.ActiveNode != "" {
		connPath += " → " + s.ActiveNode
	}

	cluster := ""
	if s.ClusterName != "" {
		cluster = fmt.Sprintf(" │ %s", s.ClusterName)
	}

	nodes := fmt.Sprintf(" │ nodes: %d", s.TotalNodes)

	// Throttle indicator
	throttleStr := ""
	if m.throttle != collector.ThrottleNone {
		throttleStr = " │ " + styleHealthYellow.Render("⚡ "+collector.ThrottleName(m.throttle))
	} else if m.heapWarning {
		throttleStr = " │ " + styleHealthRed.Render("⚠ heap>85% t:throttle")
	}

	left := connIndicator + connPath + cluster + nodes + throttleStr

	help := styleDim.Render("t:throttle  r:reconnect  q:quit")

	// Pad to fill width
	gap := m.width - lipgloss.Width(left) - lipgloss.Width(help) - 2
	if gap < 1 {
		gap = 1
	}
	padding := fmt.Sprintf("%*s", gap, "")

	return styleStatusBar.Width(m.width).Render(left + padding + help)
}
