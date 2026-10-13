package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

func TestSnapshotReport(t *testing.T) {
	start := time.Date(2026, 10, 13, 10, 9, 12, 0, time.UTC)
	s := cratedb.SnapshotInfo{Repository: "offsite", Name: "nightly", State: "PARTIAL", Started: start, Finished: start.Add(5*time.Minute + 45*time.Second),
		Version: "6.4.5", TotalShards: 10, GlobalState: true, Tables: []string{"doc.t1", "doc.parted"},
		Partitions: []cratedb.SnapshotPartition{{Table: "doc.parted", Values: []string{"d1"}}},
		Failures: []string{
			"SnapshotShardFailure{shardId=[t1/W][1], reason='node shutdown', nodeId='n2'}",
			"SnapshotShardFailure{shardId=[.partitioned.parted.041m8c8/I][1], reason='" + strings.Repeat("x", 300) + "', nodeId='n2'}",
			"something CrateDB says differently",
		}}
	// What sys.repositories gives for an Azure repository.
	repo := cratedb.Repository{Name: "offsite", Type: "azure", Settings: map[string]string{"account": "[xxxxx]", "key": "[xxxxx]", "container": "backups"}}
	snap := store.StoreSnapshot{
		Nodes: []store.NodeSnapshot{
			{NodeInfo: cratedb.NodeInfo{ID: "n1", Name: "bak1", Version: "6.4.5"}},
			{NodeInfo: cratedb.NodeInfo{ID: "n2", Name: "bak2", Version: "6.4.4"}},
			{NodeInfo: cratedb.NodeInfo{ID: "n3", Name: "bak3", Version: "6.4.5"}, Gone: true},
		},
		ClusterStatus: &cratedb.ClusterHealth{Health: "YELLOW"},
		TableHealth: []cratedb.TableHealth{
			{TableSchema: "doc", TableName: "t1", Health: "YELLOW"},
			{TableSchema: "doc", TableName: "t1", Health: "RED", Partition: ""},
		},
	}
	got := snapshotReport(s, repo, snap, "shardbak", start.Add(time.Hour))
	for _, want := range []string{
		"```\nSnapshot offsite.nightly: PARTIAL\n",
		"  started   2026-10-13 10:09:12Z\n",
		"  finished  2026-10-13 10:14:57Z (took 5m45s)\n",
		"  written by CrateDB 6.4.5 · 10 shards · with cluster metadata\n",
		"  tables    doc.t1, doc.parted (1 partition: [d1])\n",
		"  3 of 10 shards failed:\n",
		"    doc.t1 shard 1 on bak2: node shutdown\n",
		"    doc.parted partition 041m8c8 shard 1 on bak2: " + strings.Repeat("x", 300) + "\n", // never cut
		"    something CrateDB says differently\n",
		"Repository offsite (azure, writable)\n  account = [xxxxx]\n  container = backups\n  key = [xxxxx]\n",
		"Cluster shardbak: YELLOW, 2 nodes, CrateDB 6.4.4 (bak2), 6.4.5 (bak1)\n",
		"  tables with failed shards, now: doc.parted GREEN, doc.t1 RED\n",
		"Copied 2026-10-13 11:09:12Z by obsi ",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
	if !strings.HasSuffix(got, "\n```") {
		t.Errorf("report not fenced:\n%s", got)
	}
}

func TestSnapshotsYank(t *testing.T) {
	m := snapshotsModel(t)
	m, cmd := m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	if cmd != nil || !strings.Contains(m.View(), "select a snapshot") {
		t.Errorf("y on a repository: cmd %v\n%s", cmd != nil, m.View())
	}
	// Repositories, two running, then nightly.
	m = pressSnap(m, "down", "down", "down", "down", "down", "down")
	_, cmd = m.HandleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	req, ok := cmd().(SnapshotReportRequest)
	if !ok || req.Snapshot.Name != "nightly" || req.Repository.Settings["location"] != "/repo/lab_fs" {
		t.Errorf("request = %+v", req)
	}
	if m := m.notice(SnapshotsNoticeMsg{Note: "copied lab_fs.nightly"}); !strings.Contains(m.View(), "copied lab_fs.nightly") {
		t.Error("notice not shown")
	}
}
