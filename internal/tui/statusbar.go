package tui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/collector"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func fmtMs(d time.Duration) string {
	ms := float64(d) / float64(time.Millisecond)
	if ms < 1 {
		return fmt.Sprintf("%.1fms", ms)
	}
	if ms < 1000 {
		return fmt.Sprintf("%.0fms", ms)
	}
	return fmt.Sprintf("%.1fs", ms/1000)
}

// StatusBarModel renders the connection status bar.
type StatusBarModel struct {
	status         cratedb.RegistryStatus
	throttle       collector.ThrottleLevel
	heapWarning    bool
	clusterHealth  string // "GREEN", "YELLOW", "RED", or ""
	totalShards    int
	shardQueryDur  time.Duration
	width          int
}

// NewStatusBarModel creates a new status bar.
func NewStatusBarModel(width int) StatusBarModel {
	return StatusBarModel{width: width}
}

// Refresh updates the status bar with new registry status.
func (m StatusBarModel) Refresh(status cratedb.RegistryStatus, throttle collector.ThrottleLevel, heapWarning bool, clusterHealth string, totalShards int, shardQueryDur time.Duration) StatusBarModel {
	m.status = status
	m.throttle = throttle
	m.heapWarning = heapWarning
	m.clusterHealth = clusterHealth
	m.totalShards = totalShards
	m.shardQueryDur = shardQueryDur
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
		name := s.ClusterName
		switch m.clusterHealth {
		case "GREEN":
			name = styleHealthGreen.Render(name)
		case "YELLOW":
			name = styleHealthYellow.Render(name)
		case "RED":
			name = styleHealthRed.Render(name)
		}
		cluster = fmt.Sprintf(" │ %s", name)
	}

	nodes := fmt.Sprintf(" │ nodes: %d", s.TotalNodes)

	shardsStr := ""
	if m.totalShards > 0 {
		shardsStr = fmt.Sprintf(" │ shards: %d", m.totalShards)
		if m.shardQueryDur > 0 {
			shardsStr += fmt.Sprintf(" (%s)", fmtMs(m.shardQueryDur))
		}
	}

	// Throttle indicator
	throttleStr := ""
	switch {
	case m.throttle == collector.ThrottleMax:
		// Pulse effect: alternate between bright yellow and dim every second
		label := "⏸ PAUSED"
		if time.Now().Second()%2 == 0 {
			throttleStr = " │ " + styleHealthYellow.Bold(true).Render(label)
		} else {
			throttleStr = " │ " + styleDim.Render(label)
		}
	case m.throttle != collector.ThrottleNone:
		throttleStr = " │ " + styleHealthYellow.Render("⚡ "+collector.ThrottleName(m.throttle))
	case m.heapWarning:
		throttleStr = " │ " + styleHealthRed.Render("⚠ heap>85% t:throttle")
	}

	// Latency stats
	latencyStr := ""
	if s.Latency.N > 0 {
		latencyStr = fmt.Sprintf(" │ latency %s/%s/%s",
			fmtMs(s.Latency.Avg), fmtMs(s.Latency.P90), fmtMs(s.Latency.Max))
	}

	left := connIndicator + connPath + cluster + nodes + shardsStr + latencyStr + throttleStr

	help := styleDim.Render("t:throttle  r:reconnect  q:quit")

	// Pad to fill width
	gap := m.width - lipgloss.Width(left) - lipgloss.Width(help) - 2
	if gap < 1 {
		gap = 1
	}
	padding := fmt.Sprintf("%*s", gap, "")

	return styleStatusBar.Width(m.width).Render(left + padding + help)
}
