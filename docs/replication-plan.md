# Logical replication in obsi — plan

obsi stays connected to one cluster. Every replication statement runs on
exactly one side (`CREATE/ALTER/DROP PUBLICATION` on the publisher,
`CREATE/DROP SUBSCRIPTION` on the subscriber), so obsi gets a publisher
part and a subscriber part and never needs a second connection. The
subscriber can't see a publication before it subscribes; a "subscribe
ticket" copied on the publisher side (`y`) carries what it needs for the
pre-checks.

## What the lab showed (CrateDB 6.4.5)

Publisher: shardlab on 44200. Subscriber: `obsi-sub`, single node on 44220,
on the `obsi-shardlab_default` network, subscribing via `crate://lab1:4300`.

- `pg_subscription_rel.srsubstate`: `i` initializing, `d` restoring
  (copying), `r` monitoring (in sync), `e` failed with
  `srsubstate_reason`. One row per table, also for partitioned tables.
- `pg_subscription.subconninfo` masks user and password
  (`crate://lab1:4300?user=*&password=*&mode=sniff`); `subenabled` is
  always true.
- The initial copy is a shard recovery of type `SNAPSHOT` in `sys.shards`.
  A freshly written, unflushed publisher table copies 0 docs and then
  catches up by replaying operations.
- Subscriber: `seq_no_stats['max_seq_no']` rises with every change
  received. The subscriber can't know the publisher's `max_seq_no`, so it
  can't compute lag.
- Publisher: each subscriber holds a retention lease per primary in
  `sys.shards.retention_leases['leases']`, id
  `logical_replication:<subscriber cluster>:[table][shard]`, source
  `logical_replication:<subscriber cluster>`. `max_seq_no - retaining_seq_no`
  is the lag in operations, `timestamp` is the last renewal. Leases expire
  after the table's `soft_deletes.retention_lease.period` (default 12h).
- `ALTER SUBSCRIPTION … ENABLE|DISABLE` parses but fails with
  "Cannot create Plan … not supported": no pause/resume.
- `CREATE SUBSCRIPTION` fails right away for an existing local table
  (`RelationAlreadyExists`) and an unresolvable host (`unknown host`).
  Untested: resolvable host with closed 4300, wrong password.
- `sys.jobs_log` keeps `CREATE SUBSCRIPTION … CONNECTION '…password=…'`
  verbatim; obsi's `Redact` misses the password inside the URL.
- Partitions: a publication takes whole tables (`FOR TABLE t PARTITION …`
  is a parse error). New partitions on the publisher show up on the
  subscriber; a partition removed on the publisher (`DELETE … WHERE` on the
  partition column) is removed on the subscriber too. The subscriber can't
  keep a different retention than the publisher.
- Replication settings live in `sys.cluster.settings['replication']['logical']`
  (`reads_poll_duration`, `ops_batch_size`, `recovery.chunk_size`,
  `recovery.max_concurrent_file_chunks`).

## Phase 0 — redaction and change classification (own PR)

- [ ] `Redact` masks `password=…` inside connection strings, keeping the
      rest of the URL.
      *Verify:* unit test with `crate://h:4300?user=u&password=p&sslmode=disable`
      and with password as the last parameter; the Queries tab and the
      changes board on the lab show `password=***` for a real
      `CREATE SUBSCRIPTION` from `sys.jobs_log`.
- [ ] `ClassifyChange` counts `CREATE/DROP SUBSCRIPTION` and
      `CREATE/ALTER/DROP PUBLICATION` as changes.
      *Verify:* unit tests per statement; on the lab they appear on the
      changes board (`c`) after running them.
- [ ] CHANGES entry.

## Phase 1 — Replication tab, monitoring only

- [ ] Lab: `obsi-sub` as a compose service plus `scenarios.sh replication`
      (publication, subscription, partitioned table, a failing table) and
      `replication-reset`.
      *Verify:* from a clean lab, the scenario ends with one table in `r`
      and one in `e` on 44220.
- [ ] Collector: `pg_subscription` + `pg_subscription_rel` and
      `pg_publication` + `pg_publication_tables` every cycle; the
      `sys.shards` columns (`seq_no_stats`, `retention_leases`, recovery)
      only for the replicated tables and only while any exist.
      *Verify:* jobs_log on a cluster without replication shows only the two
      catalog queries; test with fixture rows for both sides.
- [ ] Subscriber view: per subscription the masked conninfo and
      publications; per table state (preparing, copying, in sync, FAILED +
      reason), copy progress in bytes, docs, changes/s from the
      `max_seq_no` delta, last change seen by obsi; one dim line that lag is
      only visible on the publisher.
      *Verify:* on the lab, a 1GB+ table under `slow` shows the copy bar
      moving; a writer loop shows changes/s > 0 and "last change" in
      seconds; the failing table shows its reason.
- [ ] Publisher view: per publication its tables; per subscriber cluster
      (from lease source) tables, ops behind (sum over primaries), last seen,
      time to lease expiry.
      *Verify:* on the lab, stopping `obsi-sub` makes "last seen" grow and
      ops behind rise under a writer loop; starting it brings both back
      down.
- [ ] Alerts: CRIT for a table in `e` (with reason); WARN when a
      subscriber's lease is older than half its period.
      *Verify:* store tests for raise/clear; on the lab, the failing table
      raises once and clears after `DROP SUBSCRIPTION`. A table with
      `soft_deletes.retention_lease.period = '2m'` and `obsi-sub` stopped
      raises the lease warning within about a minute.
- [ ] Tables tab: subscribed tables marked `⇠ <subscription>` (read-only).
      *Verify:* marker on `doc.rep_a` on 44220, not on 44200.
- [ ] Tab hidden or one dim line when the cluster has neither
      publications nor subscriptions.
      *Verify:* the plain shardlab without the scenario.
- [ ] README (tab table, what it answers) and CHANGES.

## Phase 2 — publisher setup

- [ ] Create a publication: name, ALL TABLES or a table picker, SQL preview,
      run with `ChangeTag`.
      *Verify:* the publication shows in the publisher view and on the
      changes board; refused names/tables show CrateDB's error in the form.
- [ ] Pre-checks: flag blob tables, views and foreign tables; flag tables
      whose `soft_deletes.retention_lease.period` is short; list users with
      `DQL` on all chosen tables (`sys.privileges`).
      *Verify:* each flag appears for a lab table set up to trigger it and
      not otherwise.
- [ ] Edit: `ALTER PUBLICATION ADD/DROP/SET TABLE`; delete:
      `DROP PUBLICATION` with confirmation.
      *Verify:* a subscribed lab table added to the publication shows up on
      44220 within a few seconds.
- [ ] Ticket (`y`): publication, tables, transport seed hosts from
      `sys.nodes`, never a password.
      *Verify:* clipboard content round-trips through the phase 3 parser;
      grep the clipboard text for `password`.

## Phase 3 — subscriber setup

- [ ] Before building: test a resolvable host with closed 4300 and a wrong
      password in the lab; record whether `CREATE SUBSCRIPTION` fails or
      goes to `e` later.
- [ ] Form: paste a ticket or type host and publication; user, password,
      `sniff`/`pg_tunnel`, `sslmode` for `pg_tunnel`; SQL preview with the
      password masked.
      *Verify:* the generated CONNECTION string matches the docs' format
      for both modes; the password never shows in the preview, the
      changes board or the Queries tab.
- [ ] Pre-checks: subscription name free; with a ticket, tables that
      already exist locally listed by name.
      *Verify:* `doc.rep_b` (exists on both sides) is flagged before
      running.
- [ ] `DROP SUBSCRIPTION` behind a typed confirmation saying it is one-way
      and makes the tables writable.
      *Verify:* after the drop the table accepts INSERT on 44220 and the
      lease on 44200 disappears.
- [ ] README and CHANGES.

## Not planned

- Pause/resume: `ALTER SUBSCRIPTION` isn't executable in CrateDB.
- Partition selection: publications are per table.
- Connecting obsi to both clusters at once.
