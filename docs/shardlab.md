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

`go test -tags shardlab -run Shardlab -v ./internal/tui` prints the Shards
tab for the current lab state without starting the TUI.

What the lab showed about CrateDB 6.3 and 6.4.5, and why the tab reads
`sys.allocations` rather than trusting `sys.shards`:

- a copy that is recovering is listed in `sys.shards` as UNASSIGNED
  without a node or progress; `sys.allocations` has it as INITIALIZING
- a lost primary (only copy gone) is `primary = false` in `sys.shards`
- relocation targets don't appear in `sys.shards` at all
- `sys.allocations` returns `decisions` as NULL when `explanation` is
  selected before it
