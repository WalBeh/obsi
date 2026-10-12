# shardlab

A throwaway 3-node CrateDB (6.4.5, same as the clusters this was built
against) for working on the Shards tab, which is all green on a healthy
cluster. It listens on 127.0.0.1:44200-44202 only; don't shard-test on a
real cluster.

```bash
cd testdata/shardlab
docker compose -p obsi-shardlab up -d
./scenarios.sh setup                 # tables, lab-friendly disk watermarks
./scenarios.sh too-many-replicas     # or filter, stuck, disable-allocation, ...
../../obsi http://127.0.0.1:44200    # press 5
./scenarios.sh reset
docker compose -p obsi-shardlab down -v
```

`./scenarios.sh` without arguments lists the scenarios. `slow` throttles
recoveries to 500kb/s so `move` and `recover` stay visible for a while;
`node-down` / `node-up` stop and start lab2; `many` adds ~1600 shard
copies for timing the allocation queries.

Logical replication needs a second cluster: the `replication` profile
adds a single node (cluster `shardsub`) on 127.0.0.1:44210 that subscribes
to the lab.

```bash
docker compose -p obsi-shardlab --profile replication up -d
./scenarios.sh replication           # doc.repl, doc.repl_p in sync; doc.repl_x FAILED
../../obsi http://127.0.0.1:44210    # press 7: the subscriber
../../obsi http://127.0.0.1:44200    # press 7: the publisher
./scenarios.sh replication-write     # a minute of inserts
./scenarios.sh replication-short-lease && ./scenarios.sh sub-down  # lease alert after ~1m
./scenarios.sh sub-up
./scenarios.sh replication-reset
```

`go test -tags shardlab -run Shardlab -v ./internal/tui` prints the Shards
tab for the current lab state without starting the TUI. `-run Changeslab`
runs a few SET GLOBAL / ALTER TABLE / CREATE USER statements and prints the
Queries tab's changes board. `-run Replicationlab` prints the Replication
tab for both sides after `./scenarios.sh replication`.

What the lab showed about CrateDB 6.3 and 6.4.5, and why the tab reads
`sys.allocations` rather than trusting `sys.shards`:

- a copy that is recovering is listed in `sys.shards` as UNASSIGNED
  without a node or progress; `sys.allocations` has it as INITIALIZING
- a lost primary (only copy gone) is `primary = false` in `sys.shards`
- relocation targets don't appear in `sys.shards` at all
- `sys.allocations` returns `decisions` as NULL when `explanation` is
  selected before it

And about logical replication (6.4.5):

- a shard being copied for a subscription is UNASSIGNED in `sys.shards`
  too, so copy progress in bytes isn't available on the subscriber
- after the subscriber restarts (`sub-down` / `sub-up`), its metadata
  tracker can die with a NullPointerException in
  `MetadataTracker.processSubscription` ("client is null"): tables already
  replicating carry on, but new subscriptions and tables added to a
  publication stay initializing (`i`); another restart didn't help,
  dropping and recreating the subscriptions (`replication-reset`,
  `replication`) did
- once a table of a subscription fails, CrateDB stops tracking that
  subscription's metadata, so tables added to its publications later
  never arrive
- an expired retention lease isn't removed right away; a subscriber that
  came back a little after expiry still caught up
