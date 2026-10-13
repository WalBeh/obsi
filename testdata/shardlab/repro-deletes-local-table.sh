#!/bin/sh
# Reproduces a CrateDB 6.4.5 logical replication bug on the shardlab
# (publisher 44200) and its subscriber (44210, compose --profile
# replication): with a table failed as "Relation already exists", the next
# CREATE SUBSCRIPTION makes the subscriber try to delete its own table of
# that name. The cluster state update doesn't apply, the node loses and
# re-elects master in a loop, and every subscription on it fails.
#
# It breaks the subscriber for good: afterwards
#   docker compose -p obsi-shardlab --profile replication rm -sfv sub
#   docker compose -p obsi-shardlab --profile replication up -d sub
set -e
cd "$(dirname "$0")"
pub() { ./sql.sh "$1" | jq -c '.rows // .rowcount // .error.message'; }
sub() { SQL_PORT=44210 ./sql.sh "$1" | jq -c '.rows // .rowcount // .error.message'; }

# 1. A subscription whose publication later gains a table the subscriber
#    has as its own.
pub "CREATE TABLE doc.bug_a (id INT) WITH (number_of_replicas = 0)"
pub "CREATE TABLE doc.bug_x (id INT) WITH (number_of_replicas = 0)"
sub "CREATE TABLE doc.bug_x (id INT) WITH (number_of_replicas = 0)"
pub "CREATE PUBLICATION bug_p1 FOR TABLE doc.bug_a"
sub "CREATE SUBSCRIPTION bug_s1 CONNECTION 'crate://lab1:4300?user=crate' PUBLICATION bug_p1"
sleep 10
pub "ALTER PUBLICATION bug_p1 ADD TABLE doc.bug_x"
sleep 10
sub "SELECT srrelid::text, srsubstate, srsubstate_reason FROM pg_catalog.pg_subscription_rel"
# doc.bug_x: e, "Relation already exists"; bug_s1 stops tracking metadata.

# 2. Any second subscription.
pub "CREATE TABLE doc.bug_b (id INT) WITH (number_of_replicas = 0)"
pub "CREATE PUBLICATION bug_p2 FOR TABLE doc.bug_b"
sub "CREATE SUBSCRIPTION bug_s2 CONNECTION 'crate://lab1:4300?user=crate' PUBLICATION bug_p2"
sleep 10
sub "SELECT srrelid::text, srsubstate, left(srsubstate_reason, 100) FROM pg_catalog.pg_subscription_rel"
# Every table: e, "Tracking of metadata failed ... no longer master".
docker logs --since 1m obsi-shardlab-sub-1 2>&1 | grep -E "deleting index|deserializing an incoming cluster state" | head -4
# [bug_x/...] deleting index   <- the subscriber's own table
# unexpected error while deserializing an incoming cluster state
#   Caused by: java.util.NoSuchElementException
#   at org.elasticsearch.cluster.metadata.Metadata$MetadataDiff.apply(Metadata.java:655)
