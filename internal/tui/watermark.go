package tui

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// parseWatermarkPct extracts a percentage value from watermark strings like "85%" or "500mb".
// For absolute values, returns -1 (can't compare without knowing total disk).
func parseWatermarkPct(s string) float64 {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "%") {
		v, err := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
		if err == nil {
			return v
		}
	}
	return -1
}

// diskWatermarkIndicator returns a string showing watermark positions on a bar.
// Example: "low:85% high:90% flood:95%"
func diskWatermarkSummary(cs cratedb.ClusterSettings) string {
	return fmt.Sprintf("low: %s │ high: %s │ flood: %s",
		cs.DiskWatermarkLow, cs.DiskWatermarkHigh, cs.DiskWatermarkFlood)
}

// diskWatermarkBar renders a disk usage bar with watermark markers.
// The bar shows ████ for used space, with │ markers at watermark thresholds.
func diskWatermarkBar(usedPct float64, cs cratedb.ClusterSettings, barWidth int) string {
	if usedPct < 0 {
		usedPct = 0
	}
	if usedPct > 100 {
		usedPct = 100
	}

	low := parseWatermarkPct(cs.DiskWatermarkLow)
	high := parseWatermarkPct(cs.DiskWatermarkHigh)
	flood := parseWatermarkPct(cs.DiskWatermarkFlood)

	// Build the bar character by character
	bar := make([]rune, barWidth)
	for i := range bar {
		pctAt := float64(i) / float64(barWidth) * 100
		if pctAt < usedPct {
			bar[i] = '█'
		} else {
			bar[i] = '░'
		}
	}

	// Place watermark markers (overwrite bar chars at marker positions)
	placeMarker := func(pct float64, ch rune) {
		if pct < 0 {
			return
		}
		pos := int(pct / 100 * float64(barWidth))
		if pos >= barWidth {
			pos = barWidth - 1
		}
		if pos >= 0 {
			bar[pos] = ch
		}
	}
	placeMarker(low, '│')
	placeMarker(high, '┃')
	placeMarker(flood, '╋')

	// Color the bar segments
	var result string
	for i, ch := range bar {
		pctAt := float64(i) / float64(barWidth) * 100
		s := string(ch)
		switch {
		case flood > 0 && pctAt >= flood:
			result += styleHighValue.Render(s)
		case high > 0 && pctAt >= high:
			result += styleHealthRed.Render(s)
		case low > 0 && pctAt >= low:
			result += styleHealthYellow.Render(s)
		case pctAt < usedPct:
			result += styleHealthGreen.Render(s)
		default:
			result += styleDim.Render(s)
		}
	}

	return fmt.Sprintf("[%s]", result)
}
