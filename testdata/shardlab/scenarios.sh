#!/bin/sh
# Puts the shardlab (127.0.0.1:44200, see compose.yml) into the states the
# Shards tab handles. usage: ./scenarios.sh <scenario>
set -e
cd "$(dirname "$0")"
sql() { ./sql.sh "$1" | jq -c '.rowcount // .error.message'; }

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
	exit 1
	;;
esac
