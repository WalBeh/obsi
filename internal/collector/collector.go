package collector

import (
	"context"
	"time"

	"github.com/WalBeh/obsi/internal/cratedb"
	"github.com/WalBeh/obsi/internal/store"
)

// Collector defines the interface every data collector implements.
type Collector interface {
	Name() string
	Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error
	Interval() time.Duration
}

// FastPathCollector is an optional interface for collectors that support
// a lightweight, high-frequency collection mode for active monitoring.
type FastPathCollector interface {
	Collector
	CollectFastPath(ctx context.Context, reg *cratedb.Registry, st *store.Store) error
}
