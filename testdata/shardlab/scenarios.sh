#!/bin/sh
# Puts the shardlab (127.0.0.1:44200, see compose.yml) into the states the
# Shards tab handles. usage: ./scenarios.sh <scenario>
set -e
cd "$(dirname "$0")"
sql() { ./sql.sh "$1" | jq -c '.rowcount // .error.message'; }
subsql() { SQL_PORT=44210 ./sql.sh "$1" | jq -c '.rowcount // .error.message'; }

case "$1" in
setup)
	# Docker disks are usually past the default 85% low watermark.
	sql "SET GLOBAL PERSISTENT \"cluster.routing.allocation.disk.watermark.flood_stage\" = '99%'"
	sql "SET GLOBAL PERSISTENT \"cluster.routing.allocation.disk.watermark.high\" = '98%'"
	sql "SET GLOBAL PERSISTENT \"cluster.routing.allocation.disk.watermark.low\" = '97%'"
	sql "CREATE TABLE IF NOT EXISTS doc.big (id BIGINT, pad TEXT) CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 1)"
	sql "INSERT INTO doc.big SELECT g, repeat(md5(g::TEXT), 20) FROM generate_series(1, 400000) g"
	sql "CREATE TABLE IF NOT EXISTS doc.events (id INT, day TEXT) PARTITIONED BY (day) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas = 1)"
	sql "INSERT INTO doc.events VALUES (1, 'a'), (2, 'b'), (3, 'c')"
	sql "CREATE TABLE IF NOT EXISTS doc.norep (id INT) CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)"
	sql "INSERT INTO doc.norep SELECT g FROM generate_series(1, 1000) g"
	;;
too-many-replicas) sql "ALTER TABLE doc.events SET (number_of_replicas = 3)" ;;
disable-allocation)
	sql "SET GLOBAL TRANSIENT \"cluster.routing.allocation.enable\" = 'primaries'"
	sql "ALTER TABLE doc.big SET (number_of_replicas = 2)"
	;;
filter) sql "ALTER TABLE doc.norep SET (number_of_replicas = 1, \"routing.allocation.require._name\" = 'lab1')" ;;
stuck) sql "ALTER TABLE doc.big SET (number_of_replicas = 2, \"routing.allocation.exclude._name\" = 'lab2')" ;;
# Slow recoveries down so INITIALIZING and RELOCATING stay visible.
slow) sql "SET GLOBAL TRANSIENT \"indices.recovery.max_bytes_per_sec\" = '500kb'" ;;
move) sql "ALTER TABLE doc.big SET (\"routing.allocation.exclude._name\" = 'lab1')" ;;
recover)
	sql "ALTER TABLE doc.big SET (number_of_replicas = 1)"
	sleep 3
	sql "ALTER TABLE doc.big SET (number_of_replicas = 2)"
	;;
# ~1600 more shard copies, for timing the allocation queries.
many)
	sql "SET GLOBAL PERSISTENT \"cluster.max_shards_per_node\" = 3000"
	sql "CREATE TABLE IF NOT EXISTS doc.many (id INT, p INT) PARTITIONED BY (p) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas = 1)"
	sql "INSERT INTO doc.many SELECT g, g FROM generate_series(1, 400) g"
	;;
# Logical replication to the subscriber on 44210 (compose --profile
# replication): doc.repl and the partitioned doc.repl_p replicate,
# doc.repl_x exists on both sides and fails with "Relation already exists".
replication)
	sql "CREATE TABLE IF NOT EXISTS doc.repl (id BIGINT, pad TEXT) CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)"
	sql "INSERT INTO doc.repl SELECT g, md5(g::TEXT) FROM generate_series(1, 50000) g"
	sql "CREATE TABLE IF NOT EXISTS doc.repl_p (id INT, day TEXT) PARTITIONED BY (day) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)"
	sql "INSERT INTO doc.repl_p VALUES (1, 'a'), (2, 'b'), (3, 'c')"
	sql "CREATE TABLE IF NOT EXISTS doc.repl_x (id INT) WITH (number_of_replicas = 0)"
	sql "CREATE PUBLICATION lab_pub FOR TABLE doc.repl, doc.repl_p"
	subsql "CREATE TABLE IF NOT EXISTS doc.repl_x (id INT) WITH (number_of_replicas = 0)"
	subsql "CREATE SUBSCRIPTION lab_sub CONNECTION 'crate://lab1:4300?user=crate&mode=sniff' PUBLICATION lab_pub"
	sleep 10
	sql "ALTER PUBLICATION lab_pub ADD TABLE doc.repl_x"
	;;
# Writes to doc.repl for a minute, for changes/s and the publisher's lag.
replication-write)
	for _ in $(seq 60); do
		sql "INSERT INTO doc.repl SELECT g, md5(g::TEXT) FROM generate_series(1, 500) g" >/dev/null
		sleep 1
	done
	;;
# With the subscriber stopped, the lease warning fires after a minute.
replication-short-lease) sql "ALTER TABLE doc.repl SET (\"soft_deletes.retention_lease.period\" = '2m')" ;;
sub-down) docker compose -p obsi-shardlab stop sub ;;
sub-up) docker compose -p obsi-shardlab start sub ;;
replication-reset)
	subsql "DROP SUBSCRIPTION IF EXISTS lab_sub"
	subsql "DROP TABLE IF EXISTS doc.repl"
	subsql "DROP TABLE IF EXISTS doc.repl_p"
	subsql "DROP TABLE IF EXISTS doc.repl_x"
	sql "DROP PUBLICATION IF EXISTS lab_pub"
	sql "DROP TABLE IF EXISTS doc.repl"
	sql "DROP TABLE IF EXISTS doc.repl_p"
	sql "DROP TABLE IF EXISTS doc.repl_x"
	;;
node-down) docker compose -p obsi-shardlab stop lab2 ;;
node-up) docker compose -p obsi-shardlab start lab2 ;;
reset)
	sql "RESET GLOBAL \"cluster.routing.allocation.enable\""
	sql "RESET GLOBAL \"indices.recovery.max_bytes_per_sec\""
	sql "ALTER TABLE doc.big SET (number_of_replicas = 1)"
	sql "ALTER TABLE doc.big RESET (\"routing.allocation.exclude._name\")"
	sql "ALTER TABLE doc.events SET (number_of_replicas = 1)"
	sql "ALTER TABLE doc.norep SET (number_of_replicas = 0)"
	sql "ALTER TABLE doc.norep RESET (\"routing.allocation.require._name\")"
	sql "DROP TABLE IF EXISTS doc.many"
	;;
*)
	echo "usage: $0 setup|too-many-replicas|disable-allocation|filter|stuck|slow|move|recover|many|node-down|node-up|reset" >&2
	echo "       $0 replication|replication-write|replication-short-lease|sub-down|sub-up|replication-reset" >&2
	exit 1
	;;
esac
