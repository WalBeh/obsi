package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestPartitionValues(t *testing.T) {
	types := map[string]string{"day": "timestamp with time zone", "id": "bigint"}
	day := float64(time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC).UnixMilli())
	if s, _ := partitionValues(map[string]interface{}{"day": day}, types); s != "day=2026-10-08" {
		t.Errorf("date = %q", s)
	}
	hour := day + float64(14*time.Hour/time.Millisecond)
	if s, _ := partitionValues(map[string]interface{}{"day": hour}, types); s != "day=2026-10-08 14:00" {
		t.Errorf("hour = %q", s)
	}
	// 6.4.5 returns timestamp partition values as strings.
	if s, _ := partitionValues(map[string]interface{}{"day": "1790812800000"}, types); s != "day=2026-10-01" {
		t.Errorf("string date = %q", s)
	}
	// A large integer that isn't a timestamp stays a number.
	if s, _ := partitionValues(map[string]interface{}{"id": float64(1759881600000)}, types); s != "id=1759881600000" {
		t.Errorf("bigint = %q", s)
	}
	if s, _ := partitionValues(map[string]interface{}{"b": "x", "a": nil}, nil); s != "a=NULL, b=x" {
		t.Errorf("multi = %q", s)
	}
}

func TestPartitionsWindow(t *testing.T) {
	day := func(d int) float64 { return float64(time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC).UnixMilli()) }
	parts := [][]interface{}{
		{"p6", map[string]interface{}{"day": day(6)}, float64(4), "1", false, float64(300000)},
		{"p8", map[string]interface{}{"day": day(8)}, float64(4), "1", false, float64(300000)},
		{"p1", map[string]interface{}{"day": day(1)}, float64(6), "1", false, float64(60000)},
		{"p7", map[string]interface{}{"day": day(7)}, float64(4), "1", true, float64(300000)},
	}
	shards := [][]interface{}{
		{"p6", float64(34 << 30), float64(118e6), float64(8), float64(1)},
		{"p8", float64(3 << 30), float64(41e6), float64(8), float64(0)},
		{"p1", float64(52 << 30), float64(130e6), float64(12), float64(0)},
	}
	types := map[string]string{"day": "timestamp with time zone"}
	snap := store.StoreSnapshot{
		Tables: []cratedb.TableInfo{{SchemaName: "doc", TableName: "events", Partitions: 4,
			Settings: cratedb.TableSettings{NumberOfShards: 4, PartitionedBy: []string{"day"}, NodeLeftDelay: 5 * time.Minute}}},
		TableHealth: []cratedb.TableHealth{{TableSchema: "doc", TableName: "events", Partition: "p6", Health: "YELLOW", UnderReplicated: 1}},
	}
	m := NewTablesModel(160, 40).Refresh(snap)
	if !strings.Contains(m.View(), "PARTS") {
		t.Error("list lacks PARTS")
	}
	m, cmd := m.HandleKey(tea.KeyMsg{Type: tea.KeyEnter})
	if req, ok := cmd().(PartitionsRequestMsg); !ok || req.Name != "events" {
		t.Fatalf("enter gave %#v", cmd())
	}
	m = m.setPartitions(PartitionsMsg{Schema: "doc", Name: "events", Parts: buildPartitions(parts, shards, types)})
	out := m.View()
	for _, want := range []string{
		"Partitions of doc.events",
		"4 partitions · 18 primaries, 28 copies",
		"1 closed", "1 YELLOW",
		"day=2026-10-08", // newest first
		"6×1",            // shard count changed later
		"closed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("window lacks %q:\n%s", want, out)
		}
	}
	if strings.Index(out, "day=2026-10-08") > strings.Index(out, "day=2026-10-01") {
		t.Error("not newest first")
	}

	m, _ = m.HandleKey(keyRune('j'))
	m, _ = m.HandleKey(keyRune('j')) // 08, 07 (closed), 06
	if d := m.View(); !strings.Contains(d, "day=2026-10-06 · ident p6") || !strings.Contains(d, "1 underreplicated") || !strings.Contains(d, "1 copies not started") {
		t.Errorf("detail for p6:\n%s", d)
	}

	m, _ = m.HandleKey(keyRune('s')) // by size
	if m.partView.parts[0].ident != "p1" {
		t.Errorf("by size first = %s", m.partView.parts[0].ident)
	}
	if txt := m.partitionsText(); !strings.Contains(txt, "day=2026-10-01\t6×1\t130000000 records") {
		t.Errorf("copy text:\n%s", txt)
	}
	m, _ = m.HandleKey(tea.KeyMsg{Type: tea.KeyEsc})
	if m.partView != nil {
		t.Error("esc didn't close")
	}
}
