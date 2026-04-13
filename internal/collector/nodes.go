package collector

import (
	"context"
	"time"

	"github.com/WalBeh/obsi/internal/config"
	"github.com/WalBeh/obsi/internal/cratedb"
	"github.com/WalBeh/obsi/internal/store"
)

type NodesCollector struct {
	interval time.Duration
	tracker  *QueryTracker
}

func NewNodesCollector(cfg config.CollectorConfig, tracker *QueryTracker) *NodesCollector {
	return &NodesCollector{interval: cfg.Interval.Duration, tracker: tracker}
}

func (c *NodesCollector) Name() string           { return "nodes" }
func (c *NodesCollector) Interval() time.Duration { return c.interval }

func (c *NodesCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	// GREATEST(..., 0) clamps byte-size columns: during restore, CrateDB can
	// return negative values that crash its own ByteSizeValue formatter.
	resp, err := trackedQuery(ctx, c.tracker, QueryNodes, reg, `SELECT
		id, name, hostname, rest_url,
		process['cpu']['percent'] AS cpu_percent,
		os['cpu']['system'] AS cpu_system,
		os['cpu']['user'] AS cpu_user,
		GREATEST(heap['used'], 0) AS heap_used,
		GREATEST(heap['max'], 0) AS heap_max,
		GREATEST(heap['free'], 0) AS heap_free,
		GREATEST(fs['total']['size'], 0) AS fs_total,
		GREATEST(fs['total']['used'], 0) AS fs_used,
		GREATEST(fs['total']['available'], 0) AS fs_avail,
		GREATEST(mem['used'], 0) AS mem_used,
		GREATEST(mem['free'], 0) AS mem_free,
		GREATEST(mem['used'] + mem['free'], 0) AS mem_total,
		load['1'] AS load1,
		load['5'] AS load5,
		load['15'] AS load15,
		version['number'] AS version,
		fs['total']['reads'] AS fs_reads,
		fs['total']['writes'] AS fs_writes,
		fs['total']['bytes_read'] AS fs_bytes_read,
		fs['total']['bytes_written'] AS fs_bytes_written,
		id = (SELECT master_node FROM sys.cluster) AS is_master,
		os_info['available_processors'] AS num_cpus,
		os_info['jvm']['version'] AS jvm_version,
		os_info['jvm']['vm_name'] AS jvm_name,
		attributes['zone'] AS zone,
		attributes['node_name'] AS node_role,
		thread_pools
	FROM sys.nodes
	ORDER BY name`)
	if err != nil {
		return err
	}

	// Get node health from registry
	status := reg.Status()
	healthMap := make(map[string]cratedb.NodeHealth)
	for _, nh := range status.Nodes {
		healthMap[nh.NodeID] = nh
	}

	nodes := make([]store.NodeSnapshot, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		info := cratedb.NodeInfo{
			ID:         cratedb.ToString(row[0]),
			Name:       cratedb.ToString(row[1]),
			Hostname:   cratedb.ToString(row[2]),
			RestURL:    cratedb.ToString(row[3]),
			CPUPercent: cratedb.ToInt16(row[4]),
			CPUSystem:  cratedb.ToInt16(row[5]),
			CPUUser:    cratedb.ToInt16(row[6]),
			HeapUsed:   cratedb.ToInt64(row[7]),
			HeapMax:    cratedb.ToInt64(row[8]),
			HeapFree:   cratedb.ToInt64(row[9]),
			FSTotal:    cratedb.ToInt64(row[10]),
			FSUsed:     cratedb.ToInt64(row[11]),
			FSAvail:    cratedb.ToInt64(row[12]),
			MemUsed:    cratedb.ToInt64(row[13]),
			MemFree:    cratedb.ToInt64(row[14]),
			MemTotal:   cratedb.ToInt64(row[15]),
			Load:       [3]float64{cratedb.ToFloat64(row[16]), cratedb.ToFloat64(row[17]), cratedb.ToFloat64(row[18])},
			Version:        cratedb.ToString(row[19]),
			FSReads:        cratedb.ToInt64(row[20]),
			FSWrites:       cratedb.ToInt64(row[21]),
			FSBytesRead:    cratedb.ToInt64(row[22]),
			FSBytesWritten: cratedb.ToInt64(row[23]),
			IsMaster:       cratedb.ToBool(row[24]),
			NumCPUs:        int(cratedb.ToFloat64(row[25])),
			JVMVersion:     cratedb.ToString(row[26]),
			JVMName:        cratedb.ToString(row[27]),
			Zone:           cratedb.ToString(row[28]),
			NodeRole:       cratedb.ToString(row[29]),
			ThreadPools:    parseThreadPools(row[30]),
		}

		snap := store.NodeSnapshot{NodeInfo: info}
		if nh, ok := healthMap[info.ID]; ok {
			snap.DirectReachable = nh.Reachable
			snap.LastLatency = nh.LastLatency
		}

		nodes = append(nodes, snap)
	}

	st.UpdateNodes(nodes)
	return nil
}

// parseThreadPools converts the thread_pools array-of-objects from CrateDB into typed structs.
func parseThreadPools(v interface{}) []cratedb.ThreadPoolStats {
	arr, ok := v.([]interface{})
	if !ok || arr == nil {
		return nil
	}
	pools := make([]cratedb.ThreadPoolStats, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		pools = append(pools, cratedb.ThreadPoolStats{
			Name:      cratedb.ToString(m["name"]),
			Active:    cratedb.ToInt64(m["active"]),
			Queue:     cratedb.ToInt64(m["queue"]),
			Rejected:  cratedb.ToInt64(m["rejected"]),
			Completed: cratedb.ToInt64(m["completed"]),
			Threads:   cratedb.ToInt64(m["threads"]),
			Largest:   cratedb.ToInt64(m["largest"]),
		})
	}
	return pools
}
