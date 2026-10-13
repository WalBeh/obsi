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
./scenarios.sh replication           # doc.repl, doc.repl_p in sync
./scenarios.sh replication-fail      # doc.repl_x FAILED (read the bug below first)
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
- worse: with a table failed as "Relation already exists", creating any
  other subscription restarts that tracking, which then tries to delete
  the subscriber's own table of that name. The cluster state update fails
  to apply (`NoSuchElementException` in `Metadata$MetadataDiff.apply`),
  the node loses and re-elects master in a loop and every subscription
  fails. Reproduced on a fresh subscriber; only a new container helped.
  `./repro-deletes-local-table.sh` runs it (and breaks the subscriber)
- an expired retention lease isn't removed right away; a subscriber that
  came back a little after expiry still caught up
- `DROP PUBLICATION` and `ALTER PUBLICATION ... DROP TABLE` don't stop a
  subscriber that already replicates the tables: changes keep arriving
  and the leases keep being renewed until `DROP SUBSCRIPTION`
- `CREATE PUBLICATION ... FOR TABLE t` refuses a table that's already in
  a publication, `ALTER PUBLICATION ... ADD TABLE t` accepts it
- `mode=pg_tunnel` (port 5432) works like `sniff` (4300); one subscriber
  can have subscriptions in both modes
- `CREATE SUBSCRIPTION` itself tests the connection and leaves nothing
  behind when it fails: a closed port and a missing publication fail at
  once, an unknown user too (pg_tunnel: "trust authentication failed",
  sniff: "subscribing user ... was not found"), an unreachable host after
  a 30s connect timeout
- `CONNECTION ?` with the connection string as a bind parameter works;
  `sys.jobs_log` then shows the `?`
- after `DROP SUBSCRIPTION` the table is writable at once and the
  publisher's leases are gone within a second
- after a table is copied by files (flushed on the publisher), the
  subscriber's lease stays at `retaining_seq_no` 0 until the first change
  arrives, then jumps to the shard's max: the lease says nothing about an
  unchanged copied table
- `ALTER TABLE ... SET ("soft_deletes.retention_lease.period" = ...)` works
  on published tables; `SHOW CREATE TABLE` shows it in ms, partitions
  don't expose it

- a finished initial copy leaves its bytes and time in the subscriber's
  `sys.shards.recovery` (`size.recovered`, `total_time`) per primary, the
  same bytes as the shard's size; nothing while it runs. In the lab a
  ~340MB copy ran at ~125MB/s per shard with the default 2 × 1mb chunks,
  ~48MB/s with 64kb chunks
- `docker pause` on the subscriber (instead of `sub-down`, which can hit
  the tracker NPE) makes BEHIND grow on the publisher; `unpause` catches up

The `replication-auth` profile adds `pub2` (127.0.0.1:44220, password
auth, TLS on 5432 with a self-signed certificate) and `lb`, a TCP balancer
over the lab's PostgreSQL ports; `./scenarios.sh replication-auth` sets up
a user and a publication there, `limited-user` a monitoring user `mon`
that isn't a superuser.

- with a wrong password, `mode=pg_tunnel&sslmode=disable` fails at once
  ("password authentication failed"); `mode=sniff` accepts it and
  replicates: over transport only the user name counts
- `sslmode=require` and the default `prefer` against a certificate the
  subscriber doesn't trust don't answer at all (its log: PKIX path
  building failed); nothing is created. `disable` works
- a table the subscriber's user can't read is left out without an error;
  it doesn't even appear in `pg_subscription_rel`
- `pg_tunnel` through the balancer works, copy and changes
- a user that isn't a superuser sees only the publications and
  subscriptions it owns, whatever it's granted; without DQL on `sys` it
  can't read `sys.shards` or `sys.cluster` either
- with ~1,700 shards the lease scan over `sys.shards` took ~6ms, the
  catalog queries under 1ms, `SHOW CREATE TABLE` 0.2ms

`-run Throughputlab` writes to doc.repl and pauses the subscriber while
polling both sides; `-run Copylab` subscribes a ~300MB table twice, with
the default chunk_size and 64kb; `-run LimitedUserlab` polls both sides
as `mon`; `-run Replicaslab` gives `doc.repl_p` a replica on the
publisher and shows the subscriber's flag, then fixes it with `'0-1'`.

- `number_of_replicas` is one of the settings CrateDB doesn't replicate:
  a value set on the subscriber holds for existing tables and partitions,
  but a new partition arrives with the publisher's count and the
  subscriber's table-level value follows it; a range such as `'0-1'` on
  the publisher arrives as a range and expands to what each cluster has

`-run Settingslab` changes a replication setting on the subscriber and a
lease period on the lab the way the Replication tab does.

`-run Publishlab` and `-run Subscribelab` run the publisher and the
subscriber setup against the lab.
