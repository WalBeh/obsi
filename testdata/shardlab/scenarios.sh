#!/bin/sh
# Puts the shardlab (127.0.0.1:44200, see compose.yml) into the states the
# Shards tab handles. usage: ./scenarios.sh <scenario>
set -e
cd "$(dirname "$0")"
sql() { ./sql.sh "$1" | jq -c '.rowcount // .error.message'; }
subsql() { SQL_PORT=44210 ./sql.sh "$1" | jq -c '.rowcount // .error.message'; }
pub2sql() { SQL_PORT=44220 ./sql.sh "$1" | jq -c '.rowcount // .error.message'; }
baksql() { SQL_PORT=44230 ./sql.sh "$1" | jq -c '.rowcount // .error.message'; }

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
# replication): doc.repl and the partitioned doc.repl_p replicate.
replication)
	sql "CREATE TABLE IF NOT EXISTS doc.repl (id BIGINT, pad TEXT) CLUSTERED INTO 3 SHARDS WITH (number_of_replicas = 0)"
	sql "INSERT INTO doc.repl SELECT g, md5(g::TEXT) FROM generate_series(1, 50000) g"
	sql "CREATE TABLE IF NOT EXISTS doc.repl_p (id INT, day TEXT) PARTITIONED BY (day) CLUSTERED INTO 1 SHARDS WITH (number_of_replicas = 0)"
	sql "INSERT INTO doc.repl_p VALUES (1, 'a'), (2, 'b'), (3, 'c')"
	sql "CREATE PUBLICATION lab_pub FOR TABLE doc.repl, doc.repl_p"
	subsql "CREATE SUBSCRIPTION lab_sub CONNECTION 'crate://lab1:4300?user=crate&mode=sniff' PUBLICATION lab_pub"
	;;
# doc.repl_x exists on both sides and fails with "Relation already exists".
# Don't create another subscription afterwards: 6.4.5 then tries to delete
# the subscriber's doc.repl_x and the subscriber breaks for good (see
# docs/shardlab.md); replication-reset and recreating the container help.
replication-fail)
	sql "CREATE TABLE IF NOT EXISTS doc.repl_x (id INT) WITH (number_of_replicas = 0)"
	subsql "CREATE TABLE IF NOT EXISTS doc.repl_x (id INT) WITH (number_of_replicas = 0)"
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
# pub2 (compose --profile replication-auth) checks passwords and has TLS
# on 5432 with a self-signed certificate: user repl/right may read
# doc.authed, not doc.secret, both in auth_pub. Subscribe from 44210 with
# crate://pub2:5432?user=repl&password=right&mode=pg_tunnel&sslmode=disable
# (prefer and require hang, the certificate isn't trusted), or through the
# balancer: crate://lb:5432?user=crate&mode=pg_tunnel&sslmode=disable.
replication-auth)
	pub2sql "CREATE USER repl WITH (password = 'right')"
	pub2sql "CREATE TABLE IF NOT EXISTS doc.authed (id INT) WITH (number_of_replicas = 0)"
	pub2sql "CREATE TABLE IF NOT EXISTS doc.secret (id INT) WITH (number_of_replicas = 0)"
	pub2sql "INSERT INTO doc.authed SELECT g FROM generate_series(1, 100) g"
	pub2sql "INSERT INTO doc.secret SELECT g FROM generate_series(1, 50) g"
	pub2sql "GRANT DQL ON TABLE doc.authed TO repl"
	pub2sql "CREATE PUBLICATION auth_pub FOR TABLE doc.authed, doc.secret"
	;;
replication-auth-reset)
	pub2sql "DROP PUBLICATION IF EXISTS auth_pub"
	pub2sql "DROP TABLE IF EXISTS doc.authed"
	pub2sql "DROP TABLE IF EXISTS doc.secret"
	pub2sql "DROP USER IF EXISTS repl"
	;;
# A monitoring user that isn't a superuser, on the lab and the subscriber:
# run obsi as mon (go test -tags shardlab -run LimitedUserlab). Without
# DQL on sys it can't read sys.shards or sys.cluster; with it, it still
# sees only the publications and subscriptions it owns.
limited-user)
	sql "CREATE USER mon"
	subsql "CREATE USER mon"
	;;
limited-user-sys)
	sql "GRANT DQL ON SCHEMA sys TO mon"
	subsql "GRANT DQL ON SCHEMA sys TO mon"
	;;
limited-user-reset)
	sql "DROP USER IF EXISTS mon"
	subsql "DROP USER IF EXISTS mon"
	;;
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
# Snapshots on the backup cluster (profile backup, 44230): an fs and an s3
# repository with a finished snapshot each, and a throttled one for slow
# snapshots. backup-running leaves a snapshot running for minutes;
# backup-partial stops bak2 while one runs, so its shards fail (PARTIAL),
# then starts it again; backup-restore restores doc.parted as
# doc.restored_parted, throttled so sys.snapshot_restore shows it a while.
backup)
	curl -s -o /dev/null --aws-sigv4 "aws:amz:us-east-1:s3" --user obsilab:obsilab-secret -X PUT http://127.0.0.1:44290/lab-bucket
	baksql "CREATE REPOSITORY lab_fs TYPE fs WITH (location = '/repo/lab_fs', compress = true)"
	baksql "CREATE REPOSITORY lab_s3 TYPE s3 WITH (bucket = 'lab-bucket', endpoint = 's3:9000', protocol = 'http', use_path_style_access = true, access_key = 'obsilab', secret_key = 'obsilab-secret')"
	baksql "CREATE REPOSITORY lab_slow TYPE fs WITH (location = '/repo/lab_slow', max_snapshot_bytes_per_sec = '100kb')"
	baksql "CREATE TABLE IF NOT EXISTS doc.t1 (id INT, pad TEXT) CLUSTERED INTO 4 SHARDS WITH (number_of_replicas = 0)"
	baksql "INSERT INTO doc.t1 SELECT g, repeat(md5(g::TEXT), 10) FROM generate_series(1, 20000) g"
	baksql "CREATE TABLE IF NOT EXISTS doc.parted (id INT, day TEXT, pad TEXT) PARTITIONED BY (day) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas = 0)"
	baksql "INSERT INTO doc.parted SELECT g, 'd' || (g % 3)::TEXT, repeat(md5(g::TEXT), 20) FROM generate_series(1, 30000) g"
	baksql "REFRESH TABLE doc.t1, doc.parted"
	baksql "CREATE SNAPSHOT lab_fs.nightly1 ALL WITH (wait_for_completion = true)"
	baksql "CREATE SNAPSHOT lab_s3.t1_only TABLE doc.t1 WITH (wait_for_completion = true)"
	baksql "CREATE SNAPSHOT lab_s3.one_day TABLE doc.parted PARTITION (day = 'd1') WITH (wait_for_completion = true)"
	;;
backup-running) baksql "CREATE SNAPSHOT lab_slow.running_$(date +%H%M%S) ALL" ;;
backup-partial)
	baksql "CREATE SNAPSHOT lab_slow.partial_$(date +%H%M%S) ALL"
	sleep 5
	docker stop obsi-shardlab-bak2-1 >/dev/null
	sleep 15
	docker start obsi-shardlab-bak2-1 >/dev/null
	;;
backup-restore)
	baksql "ALTER REPOSITORY lab_fs SET (max_restore_bytes_per_sec = '200kb')"
	baksql "RESTORE SNAPSHOT lab_fs.nightly1 TABLE doc.parted WITH (table_rename_pattern = '(.+)', table_rename_replacement = 'restored_\$1')"
	;;
# Recreates the backup cluster and its storage: dropping snapshots over
# SQL waits behind a running snapshot in the same repository, and a
# throttled one runs for minutes. Touches only bak1, bak2 and s3.
backup-reset)
	docker compose -p obsi-shardlab --profile backup rm -sf bak1 bak2 s3 >/dev/null 2>&1
	docker volume rm obsi-shardlab_bakrepo >/dev/null 2>&1 || true
	docker compose -p obsi-shardlab --profile backup up -d bak1 bak2 s3 >/dev/null 2>&1
	until [ "$(SQL_PORT=44230 ./sql.sh "SELECT count(*) FROM sys.nodes" 2>/dev/null | jq -r '.rows[0][0] // 0')" = 2 ]; do sleep 2; done
	;;
*)
	echo "usage: $0 setup|too-many-replicas|disable-allocation|filter|stuck|slow|move|recover|many|node-down|node-up|reset" >&2
	echo "       $0 replication|replication-fail|replication-write|replication-short-lease|sub-down|sub-up|replication-reset" >&2
	echo "       $0 replication-auth|replication-auth-reset|limited-user|limited-user-sys|limited-user-reset" >&2
	echo "       $0 backup|backup-running|backup-partial|backup-restore|backup-reset" >&2
	exit 1
	;;
esac
