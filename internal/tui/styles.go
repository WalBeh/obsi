package tui

import (
	"fmt"
	"sort"

	"github.com/charmbracelet/lipgloss"
)

var (
	// Colors
	colorPrimary    = lipgloss.Color("#00BFFF") // CrateDB blue
	colorSecondary  = lipgloss.Color("#A0A0A0")
	colorGreen      = lipgloss.Color("#00FF00")
	colorYellow     = lipgloss.Color("#FFD700")
	colorRed        = lipgloss.Color("#FF4444")
	colorDim        = lipgloss.Color("#666666")
	colorBg         = lipgloss.Color("#1A1A2E")
	colorTabActive  = lipgloss.Color("#00BFFF")
	colorTabInactive = lipgloss.Color("#444444")

	// Styles
	styleApp = lipgloss.NewStyle().
			Background(colorBg)

	styleTabActive = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(colorTabActive).
			Padding(0, 2)

	styleTabInactive = lipgloss.NewStyle().
			Foreground(colorSecondary).
			Background(colorTabInactive).
			Padding(0, 2)

	styleStatusBar = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(lipgloss.Color("#333333")).
			Padding(0, 1)

	styleStatusConnected = lipgloss.NewStyle().
			Foreground(colorGreen).
			Bold(true)

	styleStatusDisconnected = lipgloss.NewStyle().
			Foreground(colorRed).
			Bold(true)

	styleTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorPrimary)

	styleHealthGreen = lipgloss.NewStyle().
			Foreground(colorGreen)

	styleHealthYellow = lipgloss.NewStyle().
			Foreground(colorYellow)

	styleHealthYellowBold = styleHealthYellow.
				Bold(true)

	styleHealthRed = lipgloss.NewStyle().
			Foreground(colorRed)

	styleStale = lipgloss.NewStyle().
			Foreground(colorDim).
			Italic(true)

	styleHeader = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorPrimary).
			BorderBottom(true).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(colorDim)

	styleDim = lipgloss.NewStyle().
			Foreground(colorDim)

	styleValue = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF"))

	styleHighValue = lipgloss.NewStyle().
			Foreground(colorRed).
			Bold(true)

	colorOverlayBg = lipgloss.Color("#111111")

	stylePrimary = lipgloss.NewStyle().
			Foreground(colorYellow)

	styleModalBorder = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorYellow).
				Padding(1, 2)

	styleModalTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorYellow)
)

// metricBar renders a horizontal bar like [████████░░░░░░░░░░░░] 48%
// pct should be 0-100. barWidth is the number of characters inside the brackets.
func metricBar(pct float64, barWidth int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}

	filled := int(pct / 100 * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}

	// Color based on threshold
	var barStyle lipgloss.Style
	switch {
	case pct > 80:
		barStyle = styleHighValue
	case pct > 60:
		barStyle = styleHealthYellow
	default:
		barStyle = styleHealthGreen
	}

	bar := barStyle.Render(repeat('█', filled)) + styleDim.Render(repeat('░', barWidth-filled))
	return fmt.Sprintf("[%s]", bar)
}

// truncateString truncates s to max characters, adding "..." if truncated.
func truncateString(s string, max int) string {
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}

func repeat(ch rune, n int) string {
	if n <= 0 {
		return ""
	}
	r := make([]rune, n)
	for i := range r {
		r[i] = ch
	}
	return string(r)
}

// historyStats computes max, avg, and p90 from a history slice.
func historyStats(data []float64) (max, avg, p90 float64) {
	if len(data) == 0 {
		return 0, 0, 0
	}
	var sum float64
	max = data[0]
	for _, v := range data {
		sum += v
		if v > max {
			max = v
		}
	}
	avg = sum / float64(len(data))

	// p90: sort a copy, pick 90th percentile
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * 0.9)
	p90 = sorted[idx]
	return
}

// sectionTitle renders a consistent section header like "── Cluster Settings ──"
func sectionTitle(title string) string {
	return styleTitle.Render(fmt.Sprintf("── %s ──", title))
}

// padNodeName returns a fixed-width node name string with optional master star.
// The star uses ANSI styles so we pad the plain text portion to keep alignment.
func padNodeName(name string, isMaster bool, width int) string {
	if isMaster {
		// "★" takes 1 visible char + 1 space separator
		pad := width - len(name) - 2
		if pad < 0 {
			pad = 0
		}
		return fmt.Sprintf("%s %s%*s", name, stylePrimary.Render("★"), pad, "")
	}
	return fmt.Sprintf("%-*s", width, name)
}

// healthStyle returns the appropriate style for a health status string.
func healthStyle(health string) lipgloss.Style {
	switch health {
	case "GREEN":
		return styleHealthGreen
	case "YELLOW":
		return styleHealthYellow
	case "RED":
		return styleHealthRed
	default:
		return styleDim
	}
}

// sparkline renders a sparkline from a series of values.
func sparkline(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return ""
	}

	blocks := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

	// Use only the last 'width' values
	if len(values) > width {
		values = values[len(values)-width:]
	}

	// Find min/max
	min, max := values[0], values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	result := make([]rune, len(values))
	for i, v := range values {
		if max == min {
			result[i] = blocks[0]
		} else {
			idx := int((v - min) / (max - min) * float64(len(blocks)-1))
			if idx >= len(blocks) {
				idx = len(blocks) - 1
			}
			result[i] = blocks[idx]
		}
	}

	return string(result)
}
