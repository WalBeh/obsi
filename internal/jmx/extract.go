package jmx

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"
)

// ErrNoClusterIdentity is returned when the scrape lacks a
// cloud_clusters_health row. Without cluster identity we cannot run the
// safety check, so the scrape is rejected wholesale.
var ErrNoClusterIdentity = errors.New("cloud_clusters_health not found in scrape")

// ErrClusterMismatch is returned when the scraped cluster_name does not match
// the cluster obsi is connected to. Returning data in this case would
// attribute foreign metrics to obsi's nodes.
var ErrClusterMismatch = errors.New("scrape cluster name does not match obsi connection")

// cratePodRe matches the CrateDB Cloud StatefulSet pod-name convention. The
// pattern is duplicated from internal/tui/hostname.go on purpose: the TUI
// uses it for display shortening, this package uses it to filter out
// non-CrateDB pods (grand-central, backup-metrics, etc.) that share the
// scrape but should not be mapped to obsi nodes.
var cratePodRe = regexp.MustCompile(
	`^crate-(?:data-(?:hot|warm|cold)|master)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-\d+$`,
)

// Extract converts a parsed scrape into per-pod and cluster-level snapshots.
//
// If expectedClusterName is non-empty and does not match the cluster_name
// label on cloud_clusters_health, ErrClusterMismatch is returned and no
// data is attached. Pass "" to skip the check (e.g. for fixture tests).
func Extract(s *Scrape, expectedClusterName string) (*Extracted, error) {
	clusterName, clusterID, ok := findClusterIdentity(s.Samples)
	if !ok {
		return nil, ErrNoClusterIdentity
	}
	if expectedClusterName != "" && clusterName != expectedClusterName {
		return nil, fmt.Errorf("%w: scrape=%q obsi=%q",
			ErrClusterMismatch, clusterName, expectedClusterName)
	}

	out := &Extracted{
		Cluster: ClusterJMX{Name: clusterName, ID: clusterID, Meta: s.Meta},
		Pods:    map[string]*JMXSnapshot{},
	}

	for i := range s.Samples {
		x := &s.Samples[i]
		pod := podLabel(x)
		if pod == "" || !cratePodRe.MatchString(pod) {
			continue
		}
		snap := getOrCreatePod(out.Pods, pod, s.Meta.ScrapedAt)
		dispatch(snap, x)
	}

	for _, p := range out.Pods {
		if p.LastUserActivity.After(out.Cluster.LastUserActivity) {
			out.Cluster.LastUserActivity = p.LastUserActivity
		}
	}
	return out, nil
}

func findClusterIdentity(samples []Sample) (name, id string, ok bool) {
	for _, x := range samples {
		if x.Name == "cloud_clusters_health" {
			return x.Labels["cluster_name"], x.Labels["cluster_id"], x.Labels["cluster_name"] != ""
		}
	}
	return "", "", false
}

// podLabel reads the pod identity from either of the two label names croudng
// uses: cAdvisor exposes "pod", everything else uses "pod_name".
func podLabel(x *Sample) string {
	if v := x.Labels["pod"]; v != "" {
		return v
	}
	return x.Labels["pod_name"]
}

func getOrCreatePod(pods map[string]*JMXSnapshot, name string, scrapedAt time.Time) *JMXSnapshot {
	if p, ok := pods[name]; ok {
		return p
	}
	p := &JMXSnapshot{Pod: name, ScrapedAt: scrapedAt}
	pods[name] = p
	return p
}

// dispatch routes one sample into the matching slot of its pod snapshot.
// Unknown metric names are ignored — the scrape may carry extras we don't
// surface yet, and that's intentional.
func dispatch(p *JMXSnapshot, x *Sample) {
	switch x.Name {
	// JVM
	case "jvm_memory_bytes_max":
		if x.Labels["area"] == "heap" {
			p.HeapMax = int64(x.Value)
		}
	case "jvm_memory_pool_bytes_used":
		if p.Pools == nil {
			p.Pools = map[string]int64{}
		}
		p.Pools[x.Labels["pool"]] = int64(x.Value)
	case "jvm_buffer_pool_used_bytes":
		if p.BufferPools == nil {
			p.BufferPools = map[string]int64{}
		}
		p.BufferPools[x.Labels["pool"]] = int64(x.Value)
	case "jvm_gc_collection_seconds_count":
		g := ensureGC(p, x.Labels["gc"])
		g.Count = int64(x.Value)
		p.GC[x.Labels["gc"]] = g
	case "jvm_gc_collection_seconds_sum":
		g := ensureGC(p, x.Labels["gc"])
		g.TotalSeconds = x.Value
		p.GC[x.Labels["gc"]] = g

	// CrateDB
	case "crate_circuitbreakers":
		if p.Breakers == nil {
			p.Breakers = map[string]Breaker{}
		}
		b := p.Breakers[x.Labels["name"]]
		switch x.Labels["property"] {
		case "used":
			b.Used = int64(x.Value)
		case "limit":
			b.Limit = int64(x.Value)
		case "trippedCount":
			b.Tripped = int64(x.Value)
		}
		p.Breakers[x.Labels["name"]] = b
	case "crate_query_total_count",
		"crate_query_failed_count",
		"crate_query_sum_of_durations_millis",
		"crate_query_affected_row_count":
		if p.QueryStats == nil {
			p.QueryStats = map[string]QueryTypeStat{}
		}
		q := p.QueryStats[x.Labels["query"]]
		switch x.Name {
		case "crate_query_total_count":
			q.Total = int64(x.Value)
		case "crate_query_failed_count":
			q.Failed = int64(x.Value)
		case "crate_query_sum_of_durations_millis":
			q.DurationSumMs = int64(x.Value)
		case "crate_query_affected_row_count":
			q.AffectedRows = int64(x.Value)
		}
		p.QueryStats[x.Labels["query"]] = q
	case "crate_connections":
		if p.Connections == nil {
			p.Connections = map[string]ConnStat{}
		}
		c := p.Connections[x.Labels["protocol"]]
		switch x.Labels["property"] {
		case "open":
			c.Open = int64(x.Value)
		case "total":
			c.Total = int64(x.Value)
		case "messagesreceived":
			c.MessagesReceived = int64(x.Value)
		case "messagessent":
			c.MessagesSent = int64(x.Value)
		case "bytesreceived":
			c.BytesReceived = int64(x.Value)
		case "bytessent":
			c.BytesSent = int64(x.Value)
		}
		p.Connections[x.Labels["protocol"]] = c
	case "crate_threadpools":
		if p.ThreadPools == nil {
			p.ThreadPools = map[string]ThreadPoolStat{}
		}
		t := p.ThreadPools[x.Labels["name"]]
		switch x.Labels["property"] {
		case "active":
			t.Active = int64(x.Value)
		case "completed":
			t.Completed = int64(x.Value)
		case "poolSize":
			t.PoolSize = int64(x.Value)
		case "largestPoolSize":
			t.LargestPoolSize = int64(x.Value)
		case "queueSize":
			t.QueueSize = int64(x.Value)
		case "rejected":
			t.Rejected = int64(x.Value)
		}
		p.ThreadPools[x.Labels["name"]] = t

	// cAdvisor — pick the crate container specifically where containers are split
	case "container_cpu_usage_seconds_total":
		if x.Labels["container"] == "crate" {
			p.ContainerCPUSeconds = x.Value
		}
	case "container_memory_usage_bytes":
		if x.Labels["container"] == "crate" {
			p.ContainerMemBytes = int64(x.Value)
		}
	case "container_network_receive_bytes_total":
		p.NetRxBytes += int64(x.Value)
	case "container_network_transmit_bytes_total":
		p.NetTxBytes += int64(x.Value)
	case "container_fs_reads_bytes_total":
		if x.Labels["container"] == "crate" {
			if p.DiskReadBytes == nil {
				p.DiskReadBytes = map[string]int64{}
			}
			p.DiskReadBytes[x.Labels["device"]] = int64(x.Value)
		}
	case "container_fs_writes_bytes_total":
		if x.Labels["container"] == "crate" {
			if p.DiskWriteBytes == nil {
				p.DiskWriteBytes = map[string]int64{}
			}
			p.DiskWriteBytes[x.Labels["device"]] = int64(x.Value)
		}

	// Operator
	case "cratedb_cluster_last_user_activity":
		if t := unixSecondsToTime(x.Value); !t.IsZero() {
			p.LastUserActivity = t
		}
	}
}

func ensureGC(p *JMXSnapshot, name string) GCStat {
	if p.GC == nil {
		p.GC = map[string]GCStat{}
	}
	return p.GC[name]
}

// unixSecondsToTime converts a float Unix-seconds value (as exposed by the
// operator's last-activity gauge) to a time.Time. Non-finite or zero values
// return the zero time.
func unixSecondsToTime(v float64) time.Time {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return time.Time{}
	}
	sec := int64(v)
	nsec := int64((v - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}
