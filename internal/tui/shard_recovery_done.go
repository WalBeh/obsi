package tui

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

const (
	finishedLimit = 10
	// A finished copy's record can lag the running list by a poll; give it
	// a few ticks before dropping it.
	finishedAttempts = 5
)

// finishedRecovery is a recovery that left the running list, with what the
// new copy recorded about it in sys.shards once STARTED.
type finishedRecovery struct {
	recovery
	ended     time.Time // when obsi saw it gone
	took      time.Duration
	recovered int64 // bytes copied
	reused    int64 // bytes already on the target
	files     int64 // files copied
	attempts  int
}

// replayed means the copy was rebuilt from operations rather than files:
// CrateDB copied at most the commit point (one tiny file).
func (f finishedRecovery) replayed() bool {
	return f.files <= 1 && f.recovered < 1<<20
}

// rate is the copy's actual throughput, 0 when nothing was copied.
func (f finishedRecovery) rate() int64 {
	if f.took <= 0 || f.replayed() {
		return 0
	}
	return int64(float64(f.recovered) / f.took.Seconds())
}

// FinishedRecoveriesMsg carries what the new copies recorded.
type FinishedRecoveriesMsg struct {
	Done  []finishedRecovery
	Retry []finishedRecovery
}

const finishedQuery = `SELECT routing_state, recovery['stage'], recovery['total_time'],
	recovery['size']['recovered'], recovery['size']['reused'], recovery['files']['recovered']
FROM sys.shards
WHERE schema_name = ? AND table_name = ? AND partition_ident = ? AND id = ? AND node['name'] = ?`

// takePending hands the recoveries that left the running list to a command
// reading their new copies' records.
func (m *ShardsModel) takePending(ctx context.Context, reg *cratedb.Registry) tea.Cmd {
	if len(m.pending) == 0 || reg == nil {
		return nil
	}
	pending := m.pending
	m.pending = nil
	return func() tea.Msg {
		var msg FinishedRecoveriesMsg
		for _, f := range pending {
			s := f.shard
			resp, err := reg.Query(ctx, finishedQuery+cratedb.QueryTag, s.SchemaName, s.TableName, s.PartitionIdent, s.ID, f.to)
			if err != nil || len(resp.Rows) == 0 ||
				cratedb.ToString(resp.Rows[0][0]) != "STARTED" || cratedb.ToString(resp.Rows[0][1]) != "DONE" {
				// Not started yet, or cancelled and gone.
				if f.attempts++; f.attempts < finishedAttempts {
					msg.Retry = append(msg.Retry, f)
				}
				continue
			}
			row := resp.Rows[0]
			f.took = time.Duration(cratedb.ToInt64(row[2])) * time.Millisecond
			f.recovered = cratedb.ToInt64(row[3])
			f.reused = cratedb.ToInt64(row[4])
			f.files = cratedb.ToInt64(row[5])
			msg.Done = append(msg.Done, f)
		}
		return msg
	}
}

func (m ShardsModel) addFinished(msg FinishedRecoveriesMsg) ShardsModel {
	m.pending = append(m.pending, msg.Retry...)
	for _, f := range msg.Done {
		m.finished = append([]finishedRecovery{f}, m.finished...)
	}
	m.finished = m.finished[:min(len(m.finished), finishedLimit)]
	return m
}

// recoveryStatus says whether the throttle is what limits a running copy.
// elapsed only undercounts (obsi may have come late), so "slower" is certain.
func recoveryStatus(r recovery, elapsed time.Duration) (string, bool) {
	fastest := r.fastest()
	switch {
	case r.from == "":
		return "from its own disk, not throttled", false
	case fastest == 0:
		return "", false
	case elapsed > fastest:
		return "slower than the throttle: disk, network or replay", true
	}
	return "within the throttle's time", false
}

func (m ShardsModel) renderFinished() []string {
	if len(m.finished) == 0 {
		return nil
	}
	lines := []string{"", styleTitle.Render("  Finished while the Shards tab was open"),
		styleHeader.Render(fmt.Sprintf("  %-28s %5s %3s %-29s %10s %9s %10s  %s",
			"TABLE", "SHARD", "P/R", "FROM → TO", "COPIED", "TOOK", "RATE", "VERDICT"))}
	for _, f := range m.finished {
		pr := "R"
		if f.shard.Primary {
			pr = "P"
		}
		from := f.from
		if from == "" {
			from = "local disk"
		}
		rate, verdict := "—", ""
		switch {
		case f.replayed():
			verdict = "rebuilt from operations, no files copied"
		case f.share > 0 && f.rate() < f.share*8/10:
			rate = formatBytes(f.rate()) + "/s"
			verdict = styleHealthYellow.Render(fmt.Sprintf("below its %s/s share: disk or network", formatBytes(f.share)))
		default:
			rate = formatBytes(f.rate()) + "/s"
			verdict = "at the throttle"
		}
		if f.reused > 0 {
			verdict += styleDim.Render(fmt.Sprintf(" · %s already there", formatBytes(f.reused)))
		}
		lines = append(lines, fmt.Sprintf("  %-28s %5d  %s  %-29s %10s %9s %10s  %s",
			truncateString(f.shard.SchemaName+"."+f.shard.TableName, 28), f.shard.ID, pr,
			truncateString(from+" → "+f.to, 29), formatBytes(f.recovered),
			formatDuration(f.took), rate, verdict))
	}
	return lines
}
