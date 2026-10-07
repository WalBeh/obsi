package tui

import (
	"fmt"
	"sort"
	"time"
)

// historyWindow is how far back finished recoveries count for node peaks
// and moves back.
const historyWindow = time.Hour

// nodePeak is how much recovery traffic a node carried, in and out together
// (that's what max_bytes_per_sec caps): its bytes over the time it had any
// copy running. Adding up per-copy averages would overstate it, a copy's
// rate changes as others start and finish.
type nodePeak struct {
	rate   int64 // bytes/s while busy
	copies int   // most copies running at once
}

// peakMinDuration leaves out copies too short for the timing to mean
// anything: ended is when obsi noticed, up to a poll late.
const peakMinDuration = 30 * time.Second

func nodePeaks(history []finishedRecovery) map[string]nodePeak {
	type span struct {
		start, end time.Time
		bytes      int64
	}
	byNode := map[string][]span{}
	for _, f := range history {
		if f.replayed() || f.took < peakMinDuration {
			continue
		}
		for _, n := range []string{f.from, f.to} {
			if n != "" {
				byNode[n] = append(byNode[n], span{f.ended.Add(-f.took), f.ended, f.recovered})
			}
		}
	}
	peaks := map[string]nodePeak{}
	for n, spans := range byNode {
		sort.Slice(spans, func(i, j int) bool { return spans[i].start.Before(spans[j].start) })
		var bytes int64
		var busy time.Duration
		var curEnd time.Time
		copies := 0
		for i, s := range spans {
			bytes += s.bytes
			switch {
			case i == 0 || s.start.After(curEnd):
				busy += s.end.Sub(s.start)
				curEnd = s.end
			case s.end.After(curEnd):
				busy += s.end.Sub(curEnd)
				curEnd = s.end
			}
			at := 0
			for _, o := range spans {
				if !o.start.After(s.start) && o.end.After(s.start) {
					at++
				}
			}
			copies = max(copies, at)
		}
		if busy > 0 {
			peaks[n] = nodePeak{rate: int64(float64(bytes) / busy.Seconds()), copies: copies}
		}
	}
	return peaks
}

// capped reports a node that carried several copies at once yet stayed well
// under the throttle: then the node itself (disk or network) is the limit.
func capped(p nodePeak, throttle int64) bool {
	return throttle > 0 && p.copies >= 2 && p.rate < throttle*8/10
}

// bottleneck names the node that limits recoveries, if the history shows
// one: the busiest of the capped nodes.
func bottleneck(peaks map[string]nodePeak, throttle int64, setting string) string {
	var name string
	var best nodePeak
	for n, p := range peaks {
		if capped(p, throttle) && (p.copies > best.copies || p.copies == best.copies && p.rate > best.rate) {
			name, best = n, p
		}
	}
	if name == "" {
		return ""
	}
	return fmt.Sprintf("▲ %s moved %s/s with up to %d copies at once, under the %s/s throttle: its disk or network is the limit",
		name, formatBytes(best.rate), best.copies, setting)
}

// movedBack reports a move that returns a copy to a node it left within the
// hour: the balancer going in circles.
func (m ShardsModel) movedBack(r recovery, start time.Time) (time.Duration, bool) {
	if r.left == "" {
		return 0, false
	}
	k := recoveryKey(r.shard)
	for i := len(m.history) - 1; i >= 0; i-- {
		h := m.history[i]
		if recoveryKey(h.shard) == k && h.left == r.to && h.to == r.left && !h.ended.After(start.Add(30*time.Second)) {
			return start.Sub(h.ended), true
		}
	}
	return 0, false
}
