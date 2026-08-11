# Functional test run report

Collect cluster lifecycle and runtime samples while running the functional tests:

```sh
TEMPORAL_TEST_CLUSTER_EVENTS_FILE=/tmp/temporal-test-events.jsonl \
  CI=1 TEST_TOTAL_SHARDS=3 TEST_SHARD_INDEX=0 \
  go test -race -tags test_dep ./tests -timeout 35m
go run ./tools/testreport /tmp/temporal-test-events.jsonl
```

CI runs `./tests` as three shards. Set `CI=1` in control measurements so the
pooled mode also uses its CI-only 50-lease recycling limit.

To measure the opt-in pristine-cluster provider, add:

```sh
TEMPORAL_TEST_CLUSTER_MODE=per-test \
TEMPORAL_TEST_LIVE_CLUSTERS=8 \
TEMPORAL_TEST_WARM_SPARES=2
```

`TEMPORAL_TEST_LIVE_CLUSTERS` is an ownership semaphore: a test holds one slot
until its cluster has been destroyed. It also caps Go's `-test.parallel` setting
when the configured test parallelism is higher. Acquisition blocks at the live
bound, so setting it below the suite's nested parallel ownership burst can turn
admission into test latency.

Warm spares are a separate bound split between core and worker-enabled
inventories. The provider fills both inventories before starting tests and uses
one background filler per inventory, so at most two warm boots run concurrently.
Pooled mode remains the default.

Pass several files, or append several runs to one file, to get median, minimum,
maximum, and population standard deviation across runs:

```sh
go run ./tools/testreport run-{1..10}.jsonl
```

The JSON Lines file contains run boundaries, cluster creation phases, acquisition
latency and source, namespace registrations, logical destruction, and one-second
runtime samples. `testreport` reports both total acquisition latency and the
warm-eligible population. The latter contains warm hits and eligible inventory
misses; startup-specific and custom clusters are counted separately because they
cannot use a generic warm spare. These are the run-level timing and resource
high-water marks used by `PLAN.md` gates. Only complete
`run_started`/`run_finished` pairs are accepted, so interrupted measurements
cannot silently affect the aggregate.
