package cratedb

import (
	"strconv"
	"strings"
)

// What obsi relies on or warns about in CrateDB's logical replication
// that the docs don't cover, with the version it was seen on. It may
// change: a Quirk gets FixedIn once a release is known to behave.

// LeaseSourcePrefix starts the source of a retention lease a subscriber
// holds on a published primary; the rest is the subscriber's cluster
// name. Seen on 6.4.5.
const LeaseSourcePrefix = "logical_replication:"

// LeaseUnpulled: after a table is copied by files, the subscriber's lease
// stays at retaining_seq_no 0 until the first change arrives, so it says
// nothing about where the subscriber is. Seen on 6.4.5.
func LeaseUnpulled(retaining, maxSeqNo int64) bool { return retaining == 0 && maxSeqNo > 0 }

// RelationExists matches pg_subscription_rel's reason for a table that
// existed on the subscriber when its publication gained it.
func RelationExists(reason string) bool { return strings.HasPrefix(reason, "Relation already exists") }

// Quirk is a CrateDB bug obsi warns about or works around.
type Quirk struct {
	SeenOn  string
	FixedIn string // first version known to behave, "" while none is
}

var (
	// QuirkDeletesLocalTable: with a table failed as "Relation already
	// exists", the next CREATE SUBSCRIPTION restarts that subscription's
	// tracking, which tries to delete the subscriber's own table of that
	// name; the subscriber then loses master in a loop and every
	// subscription fails.
	QuirkDeletesLocalTable = Quirk{SeenOn: "6.4.5"}
	// QuirkTrackerDies: after a subscriber restart the metadata tracker
	// can die (NPE in MetadataTracker.processSubscription) and leave new
	// tables initializing.
	QuirkTrackerDies = Quirk{SeenOn: "6.4.5"}
	// QuirkSniffNoPassword: mode=sniff doesn't check the password.
	QuirkSniffNoPassword = Quirk{SeenOn: "6.4.5"}
	// QuirkTLSHangs: pg_tunnel with sslmode prefer or require never
	// answers CREATE SUBSCRIPTION when the subscriber doesn't trust the
	// publisher's certificate.
	QuirkTLSHangs = Quirk{SeenOn: "6.4.5"}
	// QuirkDropKeepsSubscribers: after DROP PUBLICATION its subscribers
	// keep replicating the tables they have (existing shards and
	// partitions, read-only, leases renewed); only new partitions stop.
	// ALTER PUBLICATION ... DROP TABLE does stop the table and makes it
	// writable on the subscriber.
	QuirkDropKeepsSubscribers = Quirk{SeenOn: "6.4.5"}
	// QuirkCreateRefusesPublished: CREATE PUBLICATION ... FOR TABLE
	// refuses a table already in a publication; ALTER PUBLICATION ... ADD
	// TABLE takes it.
	QuirkCreateRefusesPublished = Quirk{SeenOn: "6.4.5"}
)

// Affects says whether a cluster on version may have q: anything before
// FixedIn, and everything while no fix is known or the version isn't.
func (q Quirk) Affects(version string) bool {
	if q.FixedIn == "" || version == "" {
		return true
	}
	return CompareVersions(version, q.FixedIn) < 0
}

// CompareVersions orders dotted versions like 6.4.5 numerically; parts
// that aren't numbers count as 0.
func CompareVersions(a, b string) int {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < max(len(pa), len(pb)); i++ {
		var x, y int
		if i < len(pa) {
			x, _ = strconv.Atoi(pa[i])
		}
		if i < len(pb) {
			y, _ = strconv.Atoi(pb[i])
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}
