package store

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// AlertLevel is how bad a raised alert is.
type AlertLevel int

const (
	AlertWarn AlertLevel = iota
	AlertCrit
)

const (
	alertHistorySize = 100
	heapAlertPct     = 85.0
	heapClearPct     = 80.0 // lower than heapAlertPct so heap hovering at 85% doesn't flap
)

// Alert is one condition obsi noticed going bad. Cleared stays zero while it
// still holds.
type Alert struct {
	Key     string
	Level   AlertLevel
	Message string
	Raised  time.Time
	Cleared time.Time
}

// Firing reports whether the condition still holds.
func (a Alert) Firing() bool { return a.Cleared.IsZero() }

// AlertsState is the alert history, newest first, plus a counter of every
// alert ever raised so the TUI can spot new ones between ticks.
type AlertsState struct {
	History []Alert
	Firing  int
	Raised  int
}

type alertLog struct {
	history []*Alert // oldest first
	active  map[string]*Alert
	raised  int
}

// sync reconciles what one source sees now with what it raised before: new
// keys are raised, keys no longer present cleared. A changed message (e.g.
// YELLOW -> RED) clears the old alert and raises a new one.
func (l *alertLog) sync(source string, now time.Time, current []Alert) {
	if l.active == nil {
		l.active = make(map[string]*Alert)
	}
	prefix := source + "/"
	seen := make(map[string]bool, len(current))
	for _, c := range current {
		c.Key = prefix + c.Key
		seen[c.Key] = true
		if a, ok := l.active[c.Key]; ok {
			if a.Message == c.Message && a.Level == c.Level {
				continue
			}
			a.Cleared = now
		}
		c.Raised = now
		a := &c
		l.active[c.Key] = a
		l.history = append(l.history, a)
		l.raised++
	}
	for key, a := range l.active {
		if strings.HasPrefix(key, prefix) && !seen[key] {
			a.Cleared = now
			delete(l.active, key)
		}
	}
	if n := len(l.history) - alertHistorySize; n > 0 {
		l.history = l.history[n:]
	}
}

func (l *alertLog) isActive(source, key string) bool {
	_, ok := l.active[source+"/"+key]
	return ok
}

func (l *alertLog) snapshot() AlertsState {
	st := AlertsState{History: make([]Alert, 0, len(l.history)), Firing: len(l.active), Raised: l.raised}
	for i := len(l.history) - 1; i >= 0; i-- {
		st.History = append(st.History, *l.history[i])
	}
	return st
}

// healthAlerts raises one alert per non-GREEN table (partitions folded in, a
// yellow restart can touch hundreds) and one per failing sys.checks entry.
func healthAlerts(checks []cratedb.ClusterCheck, health []cratedb.TableHealth) []Alert {
	type agg struct {
		red                      bool
		parts                    int
		missing, underreplicated int64
	}
	tables := map[string]*agg{}
	for _, h := range health {
		if h.Health == "GREEN" {
			continue
		}
		name := h.TableSchema + "." + h.TableName
		t := tables[name]
		if t == nil {
			t = &agg{}
			tables[name] = t
		}
		t.red = t.red || h.Health == "RED"
		t.parts++
		t.missing += h.MissingShards
		t.underreplicated += h.UnderReplicated
	}
	var out []Alert
	for name, t := range tables {
		a := Alert{Key: "table/" + name, Level: AlertWarn}
		state := "YELLOW"
		if t.red {
			a.Level, state = AlertCrit, "RED"
		}
		a.Message = fmt.Sprintf("table %s %s", name, state)
		if t.parts > 1 {
			a.Message += fmt.Sprintf(" (%d partitions)", t.parts)
		}
		if t.missing > 0 {
			a.Message += fmt.Sprintf(", %d missing shards", t.missing)
		}
		if t.underreplicated > 0 {
			a.Message += fmt.Sprintf(", %d underreplicated shards", t.underreplicated)
		}
		out = append(out, a)
	}
	for _, c := range checks {
		if c.Passed {
			continue
		}
		a := Alert{Key: "check/" + strconv.Itoa(c.ID), Level: AlertWarn, Message: "check failed: " + firstLine(c.Description)}
		if c.Severity >= 3 {
			a.Level = AlertCrit
		}
		out = append(out, a)
	}
	sortAlerts(out)
	return out
}

// nodeAlerts covers nodes that left, heap above heapAlertPct and disk past a
// percentage watermark. A node alert only clears when the node is back:
// the store forgets gone nodes after nodeDisappearanceTimeout.
func (l *alertLog) nodeAlerts(nodes []NodeSnapshot, cs cratedb.ClusterSettings) []Alert {
	var out []Alert
	present := map[string]bool{}
	for _, n := range nodes {
		if n.Gone {
			out = append(out, Alert{Key: "gone/" + n.ID, Level: AlertCrit, Message: "node " + n.Name + " left the cluster"})
			continue
		}
		present[n.ID] = true

		if n.HeapMax > 0 {
			pct := float64(n.HeapUsed) / float64(n.HeapMax) * 100
			limit := heapAlertPct
			if l.isActive("nodes", "heap/"+n.ID) {
				limit = heapClearPct
			}
			if pct > limit {
				out = append(out, Alert{Key: "heap/" + n.ID, Level: AlertWarn, Message: fmt.Sprintf("node %s heap above %.0f%%", n.Name, heapAlertPct)})
			}
		}

		if n.FSTotal > 0 {
			pct := float64(n.FSUsed) / float64(n.FSTotal) * 100
			for _, wm := range []struct {
				name  string
				value string
				level AlertLevel
			}{
				{"flood", cs.DiskWatermarkFlood, AlertCrit},
				{"high", cs.DiskWatermarkHigh, AlertCrit},
				{"low", cs.DiskWatermarkLow, AlertWarn},
			} {
				limit, ok := parsePct(wm.value)
				if ok && pct >= limit {
					out = append(out, Alert{Key: "disk/" + n.ID, Level: wm.level,
						Message: fmt.Sprintf("node %s disk past %s watermark (%s)", n.Name, wm.name, wm.value)})
					break
				}
			}
		}
	}
	for key, a := range l.active {
		id, ok := strings.CutPrefix(key, "nodes/gone/")
		if ok && !present[id] && !containsKey(out, "gone/"+id) {
			out = append(out, Alert{Key: "gone/" + id, Level: a.Level, Message: a.Message})
		}
	}
	sortAlerts(out)
	return out
}

// parsePct reads "85%" style watermarks. Absolute ones ("500mb") can't be
// compared without per-node totals the alert doesn't carry, so they're skipped.
func parsePct(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasSuffix(s, "%") {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
	return v, err == nil
}

func containsKey(alerts []Alert, key string) bool {
	for _, a := range alerts {
		if a.Key == key {
			return true
		}
	}
	return false
}

// sortAlerts keeps history order stable when several raise on the same poll.
func sortAlerts(alerts []Alert) {
	sort.Slice(alerts, func(i, j int) bool { return alerts[i].Key < alerts[j].Key })
}

// ObserveConnection raises an alert while obsi can't reach the cluster.
func (s *Store) ObserveConnection(connected bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cur []Alert
	if !connected {
		cur = []Alert{{Key: "lost", Level: AlertCrit, Message: "connection to the cluster lost"}}
	}
	s.alerts.sync("conn", time.Now(), cur)
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}
