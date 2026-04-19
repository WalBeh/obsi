package collector

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

type ShardsCollector struct {
	interval            time.Duration
	hasUnhealthy        bool
	hasExplanationsCol  bool // false until first successful query with explanations
	triedExplanations   bool // true after first allocations attempt
	tracker             *QueryTracker
}

func NewShardsCollector(cfg config.CollectorConfig, tracker *QueryTracker) *ShardsCollector {
	return &ShardsCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *ShardsCollector) Name() string           { return "shards" }
func (c *ShardsCollector) Interval() time.Duration { return c.interval }

func (c *ShardsCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	// Get shard details
	resp, err := trackedQuery(ctx, c.tracker, QueryShards, reg, `SELECT
		s.id,
		s.schema_name,
		s.table_name,
		s.partition_ident,
		s.num_docs,
		s.primary,
		s.state,
		s.routing_state,
		s.relocating_node IS NOT NULL AS relocating,
		s.size,
		s.node['id'] AS node_id,
		s.node['name'] AS node_name,
		s.recovery['stage'] AS recovery_stage,
		COALESCE(s.recovery['size']['percent'], 0.0) AS recovery_percent,
		s.relocating_node
	FROM sys.shards s
	ORDER BY s.schema_name, s.table_name, s.id`)
	if err != nil {
		return err
	}

	shards := parseShardRows(resp.Rows)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check for non-STARTED shards and conditionally query allocations
	hasNonStarted := false
	for _, s := range shards {
		if s.RoutingState != "STARTED" {
			hasNonStarted = true
			break
		}
	}
	c.hasUnhealthy = hasNonStarted

	if hasNonStarted {
		c.collectAllocations(ctx, reg, st)
	} else {
		st.UpdateAllocations(nil)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Build table list from information_schema (source of truth for all tables)
	infoResp, err := trackedQuery(ctx, c.tracker, QueryTables, reg, `SELECT
		table_schema, table_name,
		number_of_shards, number_of_replicas,
		clustered_by, partitioned_by, column_policy,
		settings['refresh_interval'] AS refresh_interval,
		settings['codec'] AS codec,
		settings['translog']['flush_threshold_size'] AS translog_flush_threshold_size,
		settings['translog']['sync_interval'] AS translog_sync_interval,
		settings['translog']['durability'] AS translog_durability
	FROM information_schema.tables
	WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog', 'blob')
	AND table_type = 'BASE TABLE'
	ORDER BY table_schema, table_name`)
	if err != nil {
		// Fall back to shard-only aggregation
		tables := aggregateTables(shards)
		st.UpdateTables(tables, 0, shards)
		return nil
	}

	// Count views separately for the Overview display
	viewCount := 0
	if viewResp, err := trackedQuery(ctx, c.tracker, QueryViewCount, reg, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog', 'blob')
		AND table_type = 'VIEW'`); err == nil && len(viewResp.Rows) > 0 {
		viewCount = int(cratedb.ToFloat64(viewResp.Rows[0][0]))
	}

	// Aggregate shard data by table key
	shardAgg := aggregateTables(shards)
	shardMap := make(map[string]*cratedb.TableInfo)
	for i := range shardAgg {
		key := shardAgg[i].SchemaName + "." + shardAgg[i].TableName
		shardMap[key] = &shardAgg[i]
	}

	// Build final table list: information_schema as base, enriched with shard data
	tables := make([]cratedb.TableInfo, 0, len(infoResp.Rows))
	for _, row := range infoResp.Rows {
		schema := cratedb.ToString(row[0])
		name := cratedb.ToString(row[1])
		key := schema + "." + name

		ts := cratedb.TableSettings{
			NumberOfShards:         int(cratedb.ToFloat64(row[2])),
			NumberOfReplicas:       cratedb.ToString(row[3]),
			ClusteredBy:            cratedb.ToString(row[4]),
			ColumnPolicy:           cratedb.ToString(row[6]),
			RefreshInterval:        int(cratedb.ToFloat64(row[7])),
			Codec:                  cratedb.ToString(row[8]),
			TranslogFlushThreshold: int64(cratedb.ToFloat64(row[9])),
			TranslogSyncInterval:   int(cratedb.ToFloat64(row[10])),
			TranslogDurability:     cratedb.ToString(row[11]),
		}
		if arr, ok := row[5].([]interface{}); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					ts.PartitionedBy = append(ts.PartitionedBy, s)
				}
			}
		}

		ti := cratedb.TableInfo{
			SchemaName:    schema,
			TableName:     name,
			Settings:      ts,
			ShardsPerNode: make(map[string]int),
		}

		// Enrich with shard aggregation if available
		if sa, ok := shardMap[key]; ok {
			ti.TotalShards = sa.TotalShards
			ti.PrimaryShards = sa.PrimaryShards
			ti.ReplicaShards = sa.ReplicaShards
			ti.TotalRecords = sa.TotalRecords
			ti.TotalSize = sa.TotalSize
			ti.TotalDiskSize = sa.TotalDiskSize
			ti.MinShardSize = sa.MinShardSize
			ti.MaxShardSize = sa.MaxShardSize
			ti.AvgShardSize = sa.AvgShardSize
			ti.ShardsPerNode = sa.ShardsPerNode
		}

		tables = append(tables, ti)
	}

	st.UpdateTables(tables, viewCount, shards)
	return nil
}

// CollectFastPath runs a lightweight query for only non-STARTED shards.
// Called at high frequency (5s) when the user is on the Shards tab.
func (c *ShardsCollector) CollectFastPath(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	if !c.hasUnhealthy {
		return nil
	}

	resp, err := trackedQuery(ctx, c.tracker, QueryShardsFastPath, reg, `SELECT
		s.id,
		s.schema_name,
		s.table_name,
		s.partition_ident,
		s.num_docs,
		s.primary,
		s.state,
		s.routing_state,
		s.relocating_node IS NOT NULL AS relocating,
		s.size,
		s.node['id'] AS node_id,
		s.node['name'] AS node_name,
		s.recovery['stage'] AS recovery_stage,
		COALESCE(s.recovery['size']['percent'], 0.0) AS recovery_percent,
		s.relocating_node
	FROM sys.shards s
	WHERE s.routing_state != 'STARTED'
	ORDER BY s.schema_name, s.table_name, s.id`)
	if err != nil {
		return err
	}

	nonStarted := parseShardRows(resp.Rows)
	st.UpdateShardsPartial(nonStarted)

	if len(nonStarted) > 0 {
		c.collectAllocations(ctx, reg, st)
	} else {
		c.hasUnhealthy = false
		st.UpdateAllocations(nil)
	}

	return nil
}

// collectAllocations queries sys.allocations for non-STARTED shards.
// Fails gracefully on older CrateDB versions that lack sys.allocations
// or the explanations column.
func (c *ShardsCollector) collectAllocations(ctx context.Context, reg *cratedb.Registry, st *store.Store) {
	useExplanations := !c.triedExplanations || c.hasExplanationsCol

	var resp *cratedb.SQLResponse
	var err error
	if useExplanations {
		resp, err = trackedQuery(ctx, c.tracker, QueryAllocations, reg, `SELECT
			table_schema,
			table_name,
			partition_ident,
			shard_id,
			"primary",
			current_state,
			node_id,
			explanations
		FROM sys.allocations
		WHERE current_state != 'STARTED'`)
		if err != nil && strings.Contains(err.Error(), "ColumnUnknownException") {
			slog.Info("sys.allocations has no explanations column, using fallback query")
			c.triedExplanations = true
			c.hasExplanationsCol = false
			useExplanations = false
			err = nil // clear so we fall through to the no-explanations query
		} else {
			c.triedExplanations = true
			if err == nil {
				c.hasExplanationsCol = true
			}
		}
	}

	if !useExplanations && resp == nil {
		resp, err = trackedQuery(ctx, c.tracker, QueryAllocations, reg, `SELECT
			table_schema,
			table_name,
			partition_ident,
			shard_id,
			"primary",
			current_state,
			node_id
		FROM sys.allocations
		WHERE current_state != 'STARTED'`)
	}

	if err != nil {
		slog.Warn("sys.allocations query failed (may require CrateDB 4.2+)", "error", err)
		return
	}

	allocs := make([]cratedb.AllocationInfo, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		explanation := ""
		if useExplanations && len(row) > 7 {
			// explanations can be an array of strings or a single string
			switch v := row[7].(type) {
			case string:
				explanation = v
			case []interface{}:
				for i, e := range v {
					if s, ok := e.(string); ok {
						if i > 0 {
							explanation += "; "
						}
						explanation += s
					}
				}
			}
		}

		alloc := cratedb.AllocationInfo{
			TableSchema:    cratedb.ToString(row[0]),
			TableName:      cratedb.ToString(row[1]),
			PartitionIdent: cratedb.ToString(row[2]),
			ShardID:        int(cratedb.ToFloat64(row[3])),
			Primary:        cratedb.ToBool(row[4]),
			CurrentState:   cratedb.ToString(row[5]),
			NodeID:         cratedb.ToString(row[6]),
			Explanation:    explanation,
		}
		allocs = append(allocs, alloc)
	}

	st.UpdateAllocations(allocs)
}

func parseShardRows(rows [][]interface{}) []cratedb.ShardInfo {
	shards := make([]cratedb.ShardInfo, 0, len(rows))
	for _, row := range rows {
		shard := cratedb.ShardInfo{
			ID:              int(cratedb.ToFloat64(row[0])),
			SchemaName:      cratedb.ToString(row[1]),
			TableName:       cratedb.ToString(row[2]),
			PartitionIdent:  cratedb.ToString(row[3]),
			NumDocs:         cratedb.ToInt64(row[4]),
			Primary:         cratedb.ToBool(row[5]),
			State:           cratedb.ToString(row[6]),
			RoutingState:    cratedb.ToString(row[7]),
			Relocating:      cratedb.ToBool(row[8]),
			Size:            cratedb.ToInt64(row[9]),
			NodeID:          cratedb.ToString(row[10]),
			NodeName:        cratedb.ToString(row[11]),
			RecoveryStage:   cratedb.ToString(row[12]),
			RecoveryPercent: cratedb.ToFloat64(row[13]),
			RelocatingNode:  cratedb.ToString(row[14]),
		}
		shards = append(shards, shard)
	}
	return shards
}

func aggregateTables(shards []cratedb.ShardInfo) []cratedb.TableInfo {
	type tableKey struct {
		Schema string
		Table  string
	}

	tableMap := make(map[tableKey]*cratedb.TableInfo)

	// First pass: aggregate counts and sizes
	for _, s := range shards {
		key := tableKey{Schema: s.SchemaName, Table: s.TableName}
		ti, ok := tableMap[key]
		if !ok {
			ti = &cratedb.TableInfo{
				SchemaName:   s.SchemaName,
				TableName:    s.TableName,
				ShardsPerNode: make(map[string]int),
				MinShardSize: -1, // sentinel
			}
			tableMap[key] = ti
		}

		ti.TotalShards++
		ti.TotalDiskSize += s.Size
		if s.Primary {
			ti.PrimaryShards++
			ti.TotalRecords += s.NumDocs
			ti.TotalSize += s.Size

			// Track min/max across primary shards
			if ti.MinShardSize < 0 || s.Size < ti.MinShardSize {
				ti.MinShardSize = s.Size
			}
			if s.Size > ti.MaxShardSize {
				ti.MaxShardSize = s.Size
			}
		} else {
			ti.ReplicaShards++
		}

		if s.NodeName != "" {
			ti.ShardsPerNode[s.NodeName]++
		}
	}

	tables := make([]cratedb.TableInfo, 0, len(tableMap))
	for _, ti := range tableMap {
		if ti.MinShardSize < 0 {
			ti.MinShardSize = 0
		}
		if ti.PrimaryShards > 0 {
			ti.AvgShardSize = ti.TotalSize / int64(ti.PrimaryShards)
		}
		tables = append(tables, *ti)
	}
	return tables
}
