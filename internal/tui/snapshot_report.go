package tui

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

// Version is obsi's version, for reports that leave obsi.
var Version = "dev"

// SnapshotReportRequest asks the App to copy a snapshot's details with
// the cluster's state: the tab doesn't know the cluster name.
type SnapshotReportRequest struct {
	Snapshot   cratedb.SnapshotInfo
	Repository cratedb.Repository
}

// SnapshotsNoticeMsg is a one-line result for the Snapshots tab.
type SnapshotsNoticeMsg struct{ Note, Error string }

// snapshotReport is what y copies: the snapshot with every failure, its
// repository's settings as CrateDB shows them (credentials masked), and
// the cluster now, fenced so it pastes as one block into Slack, GitHub
// or a ticket.
func snapshotReport(s cratedb.SnapshotInfo, repo cratedb.Repository, snap store.StoreSnapshot, cluster string, now time.Time) string {
	utc := func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05Z") }
	names := map[string]string{}
	for _, n := range snap.Nodes {
		names[n.ID] = n.Name
	}
	var b strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	line("```")
	line("Snapshot %s.%s: %s", s.Repository, s.Name, s.State)
	if !s.Started.IsZero() {
		line("  started   %s", utc(s.Started))
	}
	switch {
	case !s.Finished.IsZero():
		line("  finished  %s (took %s)", utc(s.Finished), snapshotTook(s.Finished.Sub(s.Started)))
	case s.State == "IN_PROGRESS":
		line("  running   for %s", snapshotTook(now.Sub(s.Started)))
	case s.State == "FAILED":
		line("  CrateDB couldn't read this snapshot's details from the repository")
	}
	var meta []string
	if s.Version != "" {
		meta = append(meta, "written by CrateDB "+s.Version)
	}
	meta = append(meta, fmt.Sprintf("%d shards", s.TotalShards))
	if s.GlobalState {
		meta = append(meta, "with cluster metadata")
	}
	line("  %s", strings.Join(meta, " · "))
	if s.Reason != "" {
		line("  reason    %s", s.Reason)
	}
	if len(s.Tables) > 0 {
		line("  tables    %s", strings.Join(snapshotTables(s), ", "))
	}
	failed := map[string]bool{}
	if len(s.Failures) > 0 {
		line("  %d of %d shards failed:", len(s.Failures), s.TotalShards)
		for _, raw := range s.Failures {
			f, ok := cratedb.ParseShardFailure(raw)
			if !ok {
				line("    %s", raw)
				continue
			}
			failed[f.Table] = true
			where := f.Table
			if f.Partition != "" {
				where += " partition " + f.Partition
			}
			node := names[f.NodeID]
			if node == "" {
				node = f.NodeID
			}
			if node != "" {
				node = " on " + node
			}
			line("    %s shard %d%s: %s", where, f.Shard, node, f.Reason)
		}
	}

	access := "writable"
	if repo.ReadOnly() {
		access = "read-only"
	}
	line("Repository %s (%s, %s)", repo.Name, repo.Type, access)
	for _, k := range slices.Sorted(maps.Keys(repo.Settings)) {
		line("  %s = %s", k, repo.Settings[k])
	}

	health := "health unknown"
	if snap.ClusterStatus != nil {
		health = snap.ClusterStatus.Health
	}
	versions := map[string][]string{}
	for _, n := range snap.Nodes {
		if !n.Gone {
			versions[n.Version] = append(versions[n.Version], n.Name)
		}
	}
	running := 0
	for _, v := range versions {
		running += len(v)
	}
	cv := "CrateDB ?"
	switch len(versions) {
	case 0:
	case 1:
		for v := range versions {
			cv = "CrateDB " + v
		}
	default:
		var parts []string
		for _, v := range slices.Sorted(maps.Keys(versions)) {
			slices.Sort(versions[v])
			parts = append(parts, v+" ("+strings.Join(versions[v], ", ")+")")
		}
		cv = "CrateDB " + strings.Join(parts, ", ")
	}
	line("Cluster %s: %s, %d %s, %s", cluster, health, running, plural(running, "node", "nodes"), cv)
	if len(failed) > 0 {
		worst := map[string]string{}
		for _, h := range snap.TableHealth {
			t := h.TableSchema + "." + h.TableName
			if failed[t] && healthRank(h.Health) > healthRank(worst[t]) {
				worst[t] = h.Health
			}
		}
		var now []string
		for _, t := range slices.Sorted(maps.Keys(failed)) {
			h := worst[t]
			if h == "" {
				h = "GREEN"
			}
			now = append(now, t+" "+h)
		}
		line("  tables with failed shards, now: %s", strings.Join(now, ", "))
	}
	line("Copied %s by obsi %s", utc(now), Version)
	b.WriteString("```")
	return b.String()
}

func healthRank(h string) int {
	switch h {
	case "RED":
		return 3
	case "YELLOW":
		return 2
	case "GREEN":
		return 1
	}
	return 0
}

// copySnapshotReport builds the report from what the store has now.
func copySnapshotReport(msg SnapshotReportRequest, snap store.StoreSnapshot, cluster string) SnapshotsNoticeMsg {
	text := snapshotReport(msg.Snapshot, msg.Repository, snap, cluster, time.Now())
	if err := writeClipboard(text); err != "" {
		return SnapshotsNoticeMsg{Error: "couldn't copy: " + err}
	}
	return SnapshotsNoticeMsg{Note: fmt.Sprintf("copied %s.%s: snapshot, repository and cluster (%d lines)", msg.Snapshot.Repository, msg.Snapshot.Name, strings.Count(text, "\n")-1)}
}
