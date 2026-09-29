package store

import (
	"fmt"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// ObserveCluster raises an alert while the endpoint answers as a different
// cluster than the one obsi runs against.
func (s *Store) ObserveCluster(st cratedb.RegistryStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cur []Alert
	if f := st.Foreign; f != nil {
		was := cratedb.ClusterIdentity{ID: st.ClusterID, Name: st.ClusterName}
		cur = []Alert{{Key: f.ID, Level: AlertCrit,
			Message: fmt.Sprintf("different cluster behind the endpoint: %s, was %s", f, was)}}
	}
	s.alerts.sync("cluster", time.Now(), cur)
}

// Reset forgets everything about the old cluster after the user switched to
// the one now behind the endpoint. The alert history stays, with every
// firing alert cleared and the switch recorded.
func (s *Store) Reset(from, to string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	alerts := s.alerts
	for _, a := range alerts.active {
		a.Cleared = now
	}
	alerts.active = nil
	alerts.history = append(alerts.history, &Alert{
		Key: "cluster/switch", Level: AlertWarn, Raised: now, Cleared: now,
		Message: fmt.Sprintf("switched to cluster %s (was %s)", to, from),
	})
	alerts.raised++

	fresh := New(s.sparklineSize, nil)
	fresh.staleAfter = s.staleAfter
	fresh.queriesInterval = s.queriesInterval
	fresh.alerts = alerts
	s.data = fresh.data
}
