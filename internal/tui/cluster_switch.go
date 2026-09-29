package tui

import (
	"github.com/charmbracelet/lipgloss"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// renderClusterSwitch is shown while the endpoint answers as a cluster other
// than the one obsi started with. Everything is paused behind it.
func renderClusterSwitch(st cratedb.RegistryStatus, now cratedb.ClusterIdentity, width, height int) string {
	innerWidth := modalInnerWidth(width, 70, 40)
	was := cratedb.ClusterIdentity{ID: st.ClusterID, Name: st.ClusterName}
	content := lipgloss.JoinVertical(lipgloss.Left,
		styleModalTitle.Render("Different cluster behind the endpoint"),
		"",
		"obsi was watching   "+styleValue.Render(was.String()),
		"the endpoint is now "+styleValue.Render(now.String()),
		"",
		"Polling and writes are paused until you decide. Switching drops",
		"what obsi kept about "+was.String()+". If it comes back first,",
		"obsi carries on by itself.",
		"",
		styleDim.Render("[enter] switch to "+now.String()+"   [q] quit"),
	)
	return placeModal(content, innerWidth, width, height)
}
