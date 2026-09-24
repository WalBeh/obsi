package store

import (
	"time"

	"github.com/waltergrande/cratedb-observer/internal/cratedb"
)

// UpdateTables updates table and shard info.
func (s *Store) UpdateTables(tables []cratedb.TableInfo, viewCount int, shards []cratedb.ShardInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables = tables
	s.viewCount = viewCount
	s.shards = shards
	s.lastUpdated["shards"] = time.Now()
}

// UpdateAllocations updates allocation info for non-STARTED shards.
func (s *Store) UpdateAllocations(allocs []cratedb.AllocationInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocations = allocs
}

// UpdateShardsPartial replaces only non-STARTED shards in the existing list,
// keeping STARTED shards from the last full collection intact.
func (s *Store) UpdateShardsPartial(nonStarted []cratedb.ShardInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := make([]cratedb.ShardInfo, 0, len(s.shards))
	for _, sh := range s.shards {
		if sh.RoutingState == "STARTED" {
			kept = append(kept, sh)
		}
	}
	s.shards = append(kept, nonStarted...)
	s.lastUpdated["shards"] = time.Now()
}

// ShardCount returns the total number of shards in the store.
func (s *Store) ShardCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.shards)
}
