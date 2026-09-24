package collector

import (
	"testing"
	"time"
)

func TestParseSnapshots(t *testing.T) {
	rows := [][]interface{}{
		{"backups", "snap-2", "IN_PROGRESS", float64(1790232600000), nil, nil},
		{"backups", "snap-1", "PARTIAL", float64(1790232500000), float64(1790232560000), float64(2)},
	}
	got := parseSnapshots(rows)
	if len(got) != 2 || !got[0].Finished.IsZero() || got[0].Failures != 0 {
		t.Fatalf("in-progress row = %+v", got[0])
	}
	if got[1].Failures != 2 || got[1].Finished.Sub(got[1].Started) != time.Minute {
		t.Errorf("partial row = %+v", got[1])
	}
}
