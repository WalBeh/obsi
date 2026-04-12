package collector

import (
	"context"
	"time"

	"github.com/waltergrande/cratedb-observer/internal/config"
	"github.com/waltergrande/cratedb-observer/internal/cratedb"
	"github.com/waltergrande/cratedb-observer/internal/store"
)

type NodesCollector struct {
	interval time.Duration
}

func NewNodesCollector(cfg config.CollectorConfig) *NodesCollector {
	return &NodesCollector{interval: cfg.Interval.Duration}
}

func (c *NodesCollector) Name() string           { return "nodes" }
func (c *NodesCollector) Interval() time.Duration { return c.interval }

func (c *NodesCollector) Collect(ctx context.Context, reg *cratedb.Registry, st *store.Store) error {
	// GREATEST(..., 0) clamps byte-size columns: during restore, CrateDB can
	// return negative values that crash its own ByteSizeValue formatter.
	resp, err := reg.Query(ctx, `SELECT
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
			ID:         toString(row[0]),
			Name:       toString(row[1]),
			Hostname:   toString(row[2]),
			RestURL:    toString(row[3]),
			CPUPercent: int16(toFloat64(row[4])),
			CPUSystem:  int16(toFloat64(row[5])),
			CPUUser:    int16(toFloat64(row[6])),
			HeapUsed:   int64(toFloat64(row[7])),
			HeapMax:    int64(toFloat64(row[8])),
			HeapFree:   int64(toFloat64(row[9])),
			FSTotal:    int64(toFloat64(row[10])),
			FSUsed:     int64(toFloat64(row[11])),
			FSAvail:    int64(toFloat64(row[12])),
			MemUsed:    int64(toFloat64(row[13])),
			MemFree:    int64(toFloat64(row[14])),
			MemTotal:   int64(toFloat64(row[15])),
			Load:       [3]float64{toFloat64(row[16]), toFloat64(row[17]), toFloat64(row[18])},
			Version:        toString(row[19]),
			FSReads:        int64(toFloat64(row[20])),
			FSWrites:       int64(toFloat64(row[21])),
			FSBytesRead:    int64(toFloat64(row[22])),
			FSBytesWritten: int64(toFloat64(row[23])),
			IsMaster:       toBool(row[24]),
			NumCPUs:        int(toFloat64(row[25])),
			JVMVersion:     toString(row[26]),
			JVMName:        toString(row[27]),
			Zone:           toString(row[28]),
			NodeRole:       toString(row[29]),
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
			Name:      toString(m["name"]),
			Active:    int64(toFloat64(m["active"])),
			Queue:     int64(toFloat64(m["queue"])),
			Rejected:  int64(toFloat64(m["rejected"])),
			Completed: int64(toFloat64(m["completed"])),
			Threads:   int64(toFloat64(m["threads"])),
			Largest:   int64(toFloat64(m["largest"])),
		})
	}
	return pools
}
