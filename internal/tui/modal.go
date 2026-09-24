package tui

import "github.com/charmbracelet/lipgloss"

// modalInnerWidth is the content width of a modal pct% as wide as the
// screen, never narrower than minOuter. 6 = border (2) + padding (4).
func modalInnerWidth(width, pct, minOuter int) int {
	return max(width*pct/100, minOuter) - 6
}

// placeModal draws content in the modal border, centered over the dimmed
// body. innerWidth 0 sizes the box to its content.
func placeModal(content string, innerWidth, width, height int) string {
	border := styleModalBorder
	if innerWidth > 0 {
		border = border.Width(innerWidth)
	}
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, border.Render(content),
		lipgloss.WithWhitespaceBackground(colorOverlayBg))
}
