package cratedb

import "testing"

func TestParseShardFailure(t *testing.T) {
	// From the lab: bak2 stopped while a snapshot ran.
	cases := map[string]ShardFailure{
		`SnapshotShardFailure{shardId=[t1/WvwdaDRrRDC-4FAgmTzb9Q][3], reason='node shutdown', nodeId='5eAQXV6cQ120hop_SGC8Cg'}`: {
			Table: "doc.t1", Shard: 3, NodeID: "5eAQXV6cQ120hop_SGC8Cg", Reason: "node shutdown"},
		`SnapshotShardFailure{shardId=[.partitioned.parted.041m8c8/I9FKaUuKTbiO72wnHYDI7Q][1], reason='node shutdown', nodeId='5eAQXV6cQ120hop_SGC8Cg'}`: {
			Table: "doc.parted", Partition: "041m8c8", Shard: 1, NodeID: "5eAQXV6cQ120hop_SGC8Cg", Reason: "node shutdown"},
		`SnapshotShardFailure{shardId=[my.events/x][0], reason='IOException[can't write 'x']', nodeId='null'}`: {
			Table: "my.events", Reason: "IOException[can't write 'x']"},
		`SnapshotShardFailure{shardId=[my..partitioned.ev.04132/x][2], reason='aborted', nodeId='n'}`: {
			Table: "my.ev", Partition: "04132", Shard: 2, NodeID: "n", Reason: "aborted"},
	}
	for in, want := range cases {
		got, ok := ParseShardFailure(in)
		if !ok || got != want {
			t.Errorf("ParseShardFailure(%q) = %+v, %v; want %+v", in, got, ok, want)
		}
	}
	if _, ok := ParseShardFailure("something else"); ok {
		t.Error("parsed an unknown wording")
	}
}
