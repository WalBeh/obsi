package tui

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// partitionsRefresh is how often the open partitions window re-reads. It
// reads on demand only: a table can have thousands of partitions.
const partitionsRefresh = 10 * time.Second

// partition is one row of the partitions window.
type partition struct {
	ident      string
	values     string        // "day=2026-10-08", dates for timestamp columns
	sortKey    []interface{} // raw values, for ordering
	shards     int
	replicas   string
	closed     bool
	delay      time.Duration
	size, docs int64 // primaries
	copies     int
	notStarted int
}

type partSort int

const (
	partByValue partSort = iota
	partBySize
	partByRecords
	partSortCount
)

var partSortNames = [partSortCount]string{"value", "size", "records"}

// partitionsView is the window over one partitioned table.
type partitionsView struct {
	schema, name string
	parts        []partition
	err          string
	fetchedAt    time.Time
	requestedAt  time.Time
	selected     int
	sortBy       partSort
}

// PartitionsRequestMsg asks the App to read a table's partitions.
type PartitionsRequestMsg struct{ Schema, Name string }

// PartitionsMsg carries them back.
type PartitionsMsg struct {
	Schema, Name string
	Parts        []partition
	Err          string
}

const (
	partitionsQuery = `SELECT partition_ident, values, number_of_shards, number_of_replicas, closed,
	settings['unassigned']['node_left']['delayed_timeout']
FROM information_schema.table_partitions
WHERE table_schema = ? AND table_name = ?`

	partitionShardsQuery = `SELECT partition_ident,
	sum(CASE WHEN "primary" THEN size ELSE 0 END),
	sum(CASE WHEN "primary" THEN num_docs ELSE 0 END),
	count(*),
	sum(CASE WHEN routing_state <> 'STARTED' THEN 1 ELSE 0 END)
FROM sys.shards
WHERE schema_name = ? AND table_name = ?
GROUP BY partition_ident`

	partitionTypesQuery = `SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = ? AND table_name = ?`
)

func fetchPartitions(ctx context.Context, reg *cratedb.Registry, req PartitionsRequestMsg) PartitionsMsg {
	out := PartitionsMsg{Schema: req.Schema, Name: req.Name}
	q := func(stmt string) ([][]interface{}, error) {
		resp, err := reg.Query(ctx, stmt+cratedb.QueryTag, req.Schema, req.Name)
		if err != nil {
			return nil, err
		}
		return resp.Rows, nil
	}
	parts, err := q(partitionsQuery)
	if err != nil {
		out.Err = firstLine(err.Error())
		return out
	}
	shards, err := q(partitionShardsQuery)
	if err != nil {
		out.Err = firstLine(err.Error())
		return out
	}
	types := map[string]string{}
	if rows, err := q(partitionTypesQuery); err == nil {
		for _, r := range rows {
			types[cratedb.ToString(r[0])] = cratedb.ToString(r[1])
		}
	}
	out.Parts = buildPartitions(parts, shards, types)
	return out
}

// buildPartitions joins table_partitions with the per-partition shard sums.
func buildPartitions(parts, shards [][]interface{}, types map[string]string) []partition {
	byIdent := map[string][]interface{}{}
	for _, r := range shards {
		byIdent[cratedb.ToString(r[0])] = r
	}
	out := make([]partition, 0, len(parts))
	for _, r := range parts {
		p := partition{
			ident:    cratedb.ToString(r[0]),
			shards:   int(cratedb.ToInt64(r[2])),
			replicas: cratedb.ToString(r[3]),
			closed:   cratedb.ToBool(r[4]),
			delay:    time.Duration(cratedb.ToInt64(r[5])) * time.Millisecond,
		}
		p.values, p.sortKey = partitionValues(r[1], types)
		if s, ok := byIdent[p.ident]; ok {
			p.size, p.docs = cratedb.ToInt64(s[1]), cratedb.ToInt64(s[2])
			p.copies, p.notStarted = int(cratedb.ToInt64(s[3])), int(cratedb.ToInt64(s[4]))
		}
		out = append(out, p)
	}
	return out
}

// partitionValues renders {"day": 1759881600000} as day=2026-10-08. Only
// columns whose type is a timestamp become dates.
func partitionValues(v interface{}, types map[string]string) (string, []interface{}) {
	obj, _ := v.(map[string]interface{})
	cols := make([]string, 0, len(obj))
	for c := range obj {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	var parts []string
	var keys []interface{}
	for _, c := range cols {
		raw := obj[c]
		// information_schema.table_partitions returns timestamp values as
		// strings ("1790812800000" on 6.4.5); numbers sort as numbers.
		if str, ok := raw.(string); ok {
			if f, err := strconv.ParseFloat(str, 64); err == nil {
				raw = f
			}
		}
		keys = append(keys, raw)
		var s string
		switch x := raw.(type) {
		case nil:
			s = "NULL"
		case float64:
			if strings.HasPrefix(types[c], "timestamp") {
				t := time.UnixMilli(int64(x)).UTC()
				s = t.Format("2006-01-02 15:04")
				if t.Hour() == 0 && t.Minute() == 0 {
					s = t.Format("2006-01-02")
				}
			} else {
				s = strconv.FormatFloat(x, 'f', -1, 64)
			}
		default:
			s = fmt.Sprint(x)
		}
		parts = append(parts, c+"="+s)
	}
	return strings.Join(parts, ", "), keys
}

func lessValues(a, b []interface{}) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		x, xok := a[i].(float64)
		y, yok := b[i].(float64)
		switch {
		case xok && yok && x != y:
			return x < y
		case xok && yok:
		default:
			xs, ys := fmt.Sprint(a[i]), fmt.Sprint(b[i])
			if xs != ys {
				return xs < ys
			}
		}
	}
	return len(a) < len(b)
}

func (v *partitionsView) sort() {
	sort.SliceStable(v.parts, func(i, j int) bool {
		a, b := v.parts[i], v.parts[j]
		switch v.sortBy {
		case partBySize:
			return a.size > b.size
		case partByRecords:
			return a.docs > b.docs
		}
		return lessValues(b.sortKey, a.sortKey) // newest first
	})
}

func (m TablesModel) openPartitions() (TablesModel, tea.Cmd) {
	t, ok := m.selectedTable()
	if !ok || len(t.Settings.PartitionedBy) == 0 {
		return m, nil
	}
	m.partView = &partitionsView{schema: t.SchemaName, name: t.TableName, requestedAt: time.Now()}
	req := PartitionsRequestMsg{Schema: t.SchemaName, Name: t.TableName}
	return m, func() tea.Msg { return req }
}

// partitionsDue asks for a re-read while the window is open.
func (m *TablesModel) partitionsDue(now time.Time) (PartitionsRequestMsg, bool) {
	v := m.partView
	if v == nil || now.Sub(v.requestedAt) < partitionsRefresh {
		return PartitionsRequestMsg{}, false
	}
	v.requestedAt = now
	return PartitionsRequestMsg{Schema: v.schema, Name: v.name}, true
}

func (m TablesModel) setPartitions(msg PartitionsMsg) TablesModel {
	v := m.partView
	if v == nil || v.schema != msg.Schema || v.name != msg.Name {
		return m
	}
	var keep string
	if v.selected < len(v.parts) {
		keep = v.parts[v.selected].ident
	}
	v.err = msg.Err
	if msg.Err == "" {
		v.parts, v.fetchedAt = msg.Parts, time.Now()
		v.sort()
	}
	v.selected = 0
	for i, p := range v.parts {
		if p.ident == keep {
			v.selected = i
		}
	}
	return m
}

func (m TablesModel) handlePartitionsKey(msg tea.KeyMsg) (TablesModel, tea.Cmd) {
	v := m.partView
	switch {
	case msg.Type == tea.KeyEsc || msg.Type == tea.KeyEnter:
		m.partView = nil
	case key.Matches(msg, m.keyMap.Up):
		v.selected = max(v.selected-1, 0)
	case key.Matches(msg, m.keyMap.Down):
		v.selected = min(v.selected+1, max(len(v.parts)-1, 0))
	case key.Matches(msg, m.keyMap.SortNext):
		v.sortBy = (v.sortBy + 1) % partSortCount
		v.sort()
		v.selected = 0
	case key.Matches(msg, m.keyMap.Yank):
		text := m.partitionsText()
		n := len(v.parts)
		return m, func() tea.Msg {
			if e := writeClipboard(text); e != "" {
				return TablesNoticeMsg{Error: "copy failed: " + e}
			}
			return TablesNoticeMsg{Note: fmt.Sprintf("copied %d partitions", n)}
		}
	}
	return m, nil
}

func (m TablesModel) tableInfo(schema, name string) cratedb.TableInfo {
	for _, t := range m.snap.Tables {
		if t.SchemaName == schema && t.TableName == name {
			return t
		}
	}
	return cratedb.TableInfo{}
}

func (m TablesModel) partitionHealth(schema, name string) map[string]cratedb.TableHealth {
	out := map[string]cratedb.TableHealth{}
	for _, h := range m.snap.TableHealth {
		if h.TableSchema == schema && h.TableName == name {
			out[h.Partition] = h
		}
	}
	return out
}

func (m TablesModel) renderPartitions() string {
	v := m.partView
	t := m.tableInfo(v.schema, v.name)
	health := m.partitionHealth(v.schema, v.name)
	lines := []string{styleTitle.Render(fmt.Sprintf("Partitions of %s.%s", v.schema, v.name)) +
		styleDim.Render(fmt.Sprintf("  (esc: back  s: sort by %s  y: copy list)", partSortNames[(v.sortBy+1)%partSortCount]))}
	if n := m.noticeLine(); n != "" {
		lines = append(lines, n)
	}
	switch {
	case v.err != "" && len(v.parts) == 0:
		return strings.Join(append(lines, "  "+styleHealthRed.Render("can't read partitions: "+v.err)), "\n")
	case v.fetchedAt.IsZero():
		return strings.Join(append(lines, "  reading partitions…"), "\n")
	case len(v.parts) == 0:
		return strings.Join(append(lines, "  no partitions yet"), "\n")
	}

	var total, largest, smallest int64
	var copies, primaries, yellow, red, closed int
	smallest = -1
	for _, p := range v.parts {
		total += p.size
		largest = max(largest, p.size)
		if !p.closed && (smallest < 0 || p.size < smallest) {
			smallest = p.size
		}
		copies += p.copies
		primaries += p.shards
		if p.closed {
			closed++
		}
		switch health[p.ident].Health {
		case "YELLOW":
			yellow++
		case "RED":
			red++
		}
	}
	summary := fmt.Sprintf("  %d partitions · %d primaries, %d copies · %s · per partition avg %s, %s–%s",
		len(v.parts), primaries, copies, formatBytes(total), formatBytes(total/int64(len(v.parts))), formatBytes(max(smallest, 0)), formatBytes(largest))
	if closed > 0 {
		summary += fmt.Sprintf(" · %d closed", closed)
	}
	if red > 0 {
		summary += " · " + styleHealthRed.Render(fmt.Sprintf("%d RED", red))
	}
	if yellow > 0 {
		summary += " · " + styleHealthYellow.Render(fmt.Sprintf("%d YELLOW", yellow))
	}
	if !v.fetchedAt.IsZero() {
		summary += styleDim.Render(fmt.Sprintf(" · read %s ago", time.Since(v.fetchedAt).Truncate(time.Second)))
	}
	lines = append(lines, summary, "")

	valueWidth := 22
	for _, p := range v.parts {
		valueWidth = max(valueWidth, min(len([]rune(p.values)), 40))
	}
	const barWidth = 24
	lines = append(lines, styleHeader.Render(fmt.Sprintf("  %-*s %7s %10s %10s  %-*s %-7s %6s",
		valueWidth, "PARTITION", "SHARDS", "RECORDS", "SIZE", barWidth, "", "HEALTH", "DELAY")))

	room := max(m.height-len(lines)-4, 3)
	start := min(max(v.selected-room/2, 0), max(len(v.parts)-room, 0))
	end := min(start+room, len(v.parts))
	if start > 0 {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↑ %d more", start)))
	}
	for i := start; i < end; i++ {
		p := v.parts[i]
		marker := "  "
		if i == v.selected {
			marker = "▸ "
		}
		shards := fmt.Sprintf("%7s", fmt.Sprintf("%d×%s", p.shards, p.replicas))
		if t.Settings.NumberOfShards > 0 && p.shards != t.Settings.NumberOfShards {
			shards = styleHealthYellow.Render(shards) // number_of_shards changed for newer partitions
		}
		bar := ""
		if largest > 0 {
			bar = strings.Repeat("█", int(int64(barWidth)*p.size/largest))
		}
		h := health[p.ident].Health
		hs := fmt.Sprintf("%-7s", h)
		switch h {
		case "RED":
			hs = styleHealthRed.Render(hs)
		case "YELLOW":
			hs = styleHealthYellow.Render(hs)
		}
		delay := fmt.Sprintf("%6s", shortDelay(p.delay))
		if p.delay > 0 && t.Settings.NodeLeftDelay > p.delay {
			delay = styleHealthYellow.Render(delay)
		}
		row := fmt.Sprintf("%s%-*s %s %10s %10s  %-*s %s %s", marker,
			valueWidth, truncateString(p.values, valueWidth), shards,
			formatRecords(p.docs), formatBytes(p.size), barWidth, bar, hs, delay)
		if p.closed {
			row = styleDim.Render(row + "  closed")
		}
		lines = append(lines, row)
	}
	if end < len(v.parts) {
		lines = append(lines, styleDim.Render(fmt.Sprintf("  ↓ %d more", len(v.parts)-end)))
	}

	if v.selected < len(v.parts) {
		p := v.parts[v.selected]
		detail := fmt.Sprintf("  %s · ident %s", p.values, p.ident)
		hl := health[p.ident]
		if hl.MissingShards > 0 {
			detail += styleHealthRed.Render(fmt.Sprintf(" · %d missing shards", hl.MissingShards))
		}
		if hl.UnderReplicated > 0 {
			detail += styleHealthYellow.Render(fmt.Sprintf(" · %d underreplicated", hl.UnderReplicated))
		}
		if p.notStarted > 0 {
			detail += styleHealthYellow.Render(fmt.Sprintf(" · %d copies not started", p.notStarted))
		}
		lines = append(lines, "", detail)
	}
	if v.err != "" {
		lines = append(lines, "  "+styleHealthRed.Render("last read failed: "+v.err))
	}
	return strings.Join(lines, "\n")
}

// partitionsText is what y copies: one line per partition.
func (m TablesModel) partitionsText() string {
	v := m.partView
	health := m.partitionHealth(v.schema, v.name)
	var b strings.Builder
	fmt.Fprintf(&b, "partitions of %s.%s\n", v.schema, v.name)
	for _, p := range v.parts {
		closed := ""
		if p.closed {
			closed = " closed"
		}
		fmt.Fprintf(&b, "%s\t%d×%s\t%d records\t%s\t%s\tdelay %s\t%s%s\n",
			p.values, p.shards, p.replicas, p.docs, formatBytes(p.size), health[p.ident].Health, shortDelay(p.delay), p.ident, closed)
	}
	return b.String()
}
