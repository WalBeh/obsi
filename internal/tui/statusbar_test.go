package tui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

func TestStatusBarUser(t *testing.T) {
	lipgloss.SetColorProfile(termenv.Ascii)
	m := NewStatusBarModel(200).Refresh(cratedb.RegistryStatus{Connected: true, ClusterName: "shardlab", Username: "crate", TotalNodes: 3}, 0, false, "GREEN", 0, 0)
	if out := m.View(); !strings.Contains(out, "shardlab │ crate │ nodes: 3") {
		t.Errorf("configured user missing:\n%s", out)
	}
	m.user, m.superuser = "admin", true
	if out := m.View(); !strings.Contains(out, "shardlab │ admin (superuser) │ nodes") {
		t.Errorf("CURRENT_USER missing:\n%s", out)
	}
}
