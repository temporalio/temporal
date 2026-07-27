# Scylla/Cassandra Persistence Performance Notes

This note tracks the Scylla-focused persistence changes and the benchmark evidence still needed before calling the
optimization complete.

## Current 3 Node x 4 Shard Target

- Use 12 history shards by default for Cassandra/Scylla deployments, matching total Scylla shards in the target cluster.
- Use 12 matching task queue read/write partitions by default for normal task queues.
- Keep Scylla gocql shard-aware port enabled. It is enabled by default in the Scylla gocql fork; use
  `maxExcessShardConnectionsRate` to tune per-shard connection reuse/ramp behavior.

## CCM 3 Node x 4 Shard Setup

Use CCM for the local Scylla cluster. Scylla 2026 enforces RF/rack validity for keyspaces with secondary indexes, so an
RF=3 Temporal keyspace needs three racks in the single local datacenter.

```bash
ccm create temporal-bench-3x4 --scylla --version release:2026.1.7 -n 3 --ipprefix 127.0.80. --vnodes
```

Before first start, set the seed, snitch, and rack for each node:

```yaml
seed_provider:
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      - seeds: "127.0.80.1"
endpoint_snitch: GossipingPropertyFileSnitch
```

Use `cassandra-rackdc.properties` values `dc=datacenter1` and `rack=rack1`, `rack=rack2`, `rack=rack3` for nodes 1-3.
Start CCM from a persistent shell/session while profiling so the Scylla processes stay alive:

```bash
ccm start --no-wait --jvm_arg=--smp=4 --jvm_arg=--memory=4G
```

Install the Temporal Cassandra schema with NetworkTopologyStrategy RF=3:

```bash
./temporal-cassandra-tool --endpoint 127.0.80.1 drop -k temporal -f
./temporal-cassandra-tool --endpoint 127.0.80.1 create -k temporal --rf 3 --dc datacenter1
./temporal-cassandra-tool --endpoint 127.0.80.1 -k temporal setup-schema -v 0.0
./temporal-cassandra-tool --endpoint 127.0.80.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned
```

## Data Model Changes

- `history_node` now uses `(tree_id, branch_id)` as the partition key.
- This reduces the large-partition pattern where all branches for a tree share one Cassandra/Scylla partition.
- Existing history reads and writes already qualify both `tree_id` and `branch_id`, so the query shape remains targeted.
- `queues` now uses `(queue_type, queue_bucket)` as the partition key and `queue_name` as the clustering key, with 12
  metadata buckets for the 3-node x 4-shard target.
- This removes `ALLOW FILTERING` from QueueV2 list-by-type metadata scans without putting every queue of one type in one
  metadata partition. Point reads and CAS updates qualify `queue_type`, deterministic `queue_bucket`, and `queue_name`.
- Legacy `queue` message IDs are now allocated through `queue_message_id_range`, which fences cross-process writers at
  range granularity and lets individual message inserts use regular writes.
- `queue_message_id_ranges` is keyed by `(queue_type, queue_name)`, not by `queue_type` alone, so QueueV2 range
  reservation LWTs do not serialize all queues of one type through one Paxos partition.
- The legacy `queue` table still partitions messages by `queue_type`. Bucketing it by message-ID range would reduce
  partition growth, but it also requires range-aware reads and a persisted delete cursor; otherwise
  `DeleteMessagesBefore` becomes an unbounded fanout over bucket partitions as ack levels advance. This patch keeps the
  FIFO/read/delete contract intact and removes the steady-state per-message LWT from that table.

## LWT Audit

ScyllaDB LWT docs say conditional batches cannot modify multiple partitions, and advise against mixing conditional and
non-conditional writes over the same dataset. ScyllaDB also routes LWT work directly to the right core when the
shard-aware driver is used. Regular reads are still acceptable after successful LWT writes when the read consistency is
compatible with the LWT write's regular, non-serial consistency phase. The Temporal Cassandra store defaults to
local-quorum regular consistency and local-serial LWT consistency unless overridden, so local-quorum reads see completed
local-quorum LWT writes in the default configuration. Serial reads are only needed when the reader must participate in
Paxos and observe an in-flight conditional update before it completes.

Implications for Temporal:

- Do not split the `executions` table partition key by `type` without redesigning mutable-state CAS batches. Those
  batches intentionally update shard, workflow, and task rows under one conditional write path.
- QueueV2 message inserts can use regular writes only when message IDs come from a fenced allocator. This patch uses
  CAS-reserved ID ranges in `queue_message_id_ranges`; writers then use regular inserts for rows inside the reserved
  range. A writer crash can leave unused IDs in the reserved range, so QueueV2 readers and range deletes must tolerate
  gaps. `ListQueues` derives message count from min/max IDs, so that value is an upper bound if a crash leaves gaps.
  The range table is partitioned by queue identity because partitioning by `queue_type` would concentrate all range
  reservation LWTs for a queue type onto one hot Paxos partition.
- Legacy queue message inserts follow the same rule using `queue_message_id_range`. The first reservation starts after
  the current max message ID when a range marker is absent, then steady-state enqueue uses one regular insert per
  message until the local range is exhausted.
- Do not replace metadata/version LWTs with regular writes unless the caller can prove single-writer ownership or a
  separate fencing mechanism. Regular writes can resurrect stale metadata after a concurrent versioned update.
- Do not split `task_queue_user_data` by `build_id` without redesigning its update path. The CAS batch updates the
  task-queue user data row and build-id mapping rows together; splitting by `build_id` would make it multi-partition.

Reference docs:

- https://docs.scylladb.com/manual/stable/features/lwt.html
- https://docs.scylladb.com/manual/stable/kb/lwt-differences.html
- https://docs.scylladb.com/manual/stable/cql/consistency.html
- https://docs.scylladb.com/manual/stable/cql/cqlsh.html#serial-consistency

## Benchmark Checklist

Run each workload before and after the patch on the same 3 node x 4 shard cluster.

Persistence microbenchmark:

```bash
CASSANDRA_SEEDS=node1,node2,node3 \
CASSANDRA_PORT=9042 \
CASSANDRA_MAX_CONNS=12 \
CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2 \
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandra(HistoryNodeAppendRead|QueueV2EnqueueRead)$' -benchtime=30s -count=3
```

Include the legacy namespace-replication queue benchmark when validating queue append changes:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraQueueEnqueueRead$' -benchtime=30s -count=3
```

Include the matching task benchmark when validating server task-queue persistence paths:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraMatchingTaskQueue$' -benchtime=50x -count=3
```

Include the task-queue user data build-ID count benchmark when validating worker-versioning metadata paths:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraTaskQueueUserDataBuildIDCount$' -benchtime=100x -count=3
```

Include the shared-tree history benchmark when validating the `history_node` partition-key change:

```bash
go test -tags test_dep ./common/persistence/tests -run '^$' -bench 'BenchmarkCassandraHistoryNodeMultiBranchRead$' -benchtime=100x -count=3
```

Server workload harness:

```bash
go run ./cmd/tools/scyllaload \
  -address 127.0.0.1:7233 \
  -namespace scylla-load \
  -task-queue scylla-load \
  -workflows 10000 \
  -concurrency 200 \
  -activities-each 1 \
  -signals-each 0 \
  -payload-bytes 256 \
  -cpu-profile /tmp/scyllaload.cpu.pprof \
  -heap-profile /tmp/scyllaload.heap.pprof \
  -server-pprof http://127.0.0.1:7936 \
  -server-cpu-profile /tmp/temporal.cpu.pprof \
  -server-heap-profile /tmp/temporal.heap.pprof \
  -profile-summary /tmp/scyllaload.cpu.pprof=/tmp/scyllaload.cpu.top.txt \
  -profile-summary /tmp/scyllaload.heap.pprof=/tmp/scyllaload.heap.top.txt \
  -profile-summary /tmp/temporal.cpu.pprof=/tmp/temporal.cpu.top.txt \
  -profile-summary /tmp/temporal.heap.pprof=/tmp/temporal.heap.top.txt \
  -metrics-snapshot-before http://127.0.0.1:8000/metrics=/tmp/temporal.before.metrics.prom \
  -metrics-snapshot-before http://scylla-node-1:9180/metrics=/tmp/scylla-node-1.before.metrics.prom \
  -metrics-snapshot-before http://scylla-node-2:9180/metrics=/tmp/scylla-node-2.before.metrics.prom \
  -metrics-snapshot-before http://scylla-node-3:9180/metrics=/tmp/scylla-node-3.before.metrics.prom \
  -metrics-snapshot-after http://127.0.0.1:8000/metrics=/tmp/temporal.after.metrics.prom \
  -metrics-snapshot-after http://scylla-node-1:9180/metrics=/tmp/scylla-node-1.after.metrics.prom \
  -metrics-snapshot-after http://scylla-node-2:9180/metrics=/tmp/scylla-node-2.after.metrics.prom \
  -metrics-snapshot-after http://scylla-node-3:9180/metrics=/tmp/scylla-node-3.after.metrics.prom \
  -result-file /tmp/scyllaload.result.json \
  -run-metadata-file /tmp/scyllaload.metadata.json
```

Use `-signals-each` to stress mutable-state update paths after workflow start, and increase `-activities-each` to
stress matching task writes and reads. Run the same command before and after persistence changes while collecting
Temporal persistence metrics and Scylla per-shard/LWT metrics. The emitted result JSON includes the load result and any
load-generator/server profile paths, pprof top summary paths, pre-run and post-run metrics snapshot paths, the result
JSON path, and the run-metadata JSON path. The metadata file records runtime/process settings, selected Cassandra/Scylla
environment variables, and pprof/metrics endpoint reachability so before/after samples can be tied back to their CPU,
heap, Prometheus, and cluster-configuration evidence.

Load-generator profiles can be inspected with:

```bash
go tool pprof -top /tmp/scyllaload.cpu.pprof
go tool pprof -top /tmp/scyllaload.heap.pprof
```

The `-profile-summary` flag records the same `go tool pprof -top` output during the run, after all requested profiles
are written. Keep those text summaries with the result JSON and metrics snapshots for before/after comparisons; they
are the easiest way to prove whether remaining server time is in persistence calls, matching/history scheduling, SDK
worker code, serialization, or Scylla driver work.

Temporal server profiles can also be captured manually during the same workload window. The development Cassandra
configs expose pprof on `127.0.0.1:7936`; the Docker template enables it when `PPROF_PORT` is set.

```bash
curl -fsS 'http://127.0.0.1:7936/debug/pprof/profile?seconds=30' -o /tmp/temporal.cpu.pprof
curl -fsS 'http://127.0.0.1:7936/debug/pprof/heap' -o /tmp/temporal.heap.pprof
go tool pprof -top /tmp/temporal.cpu.pprof
go tool pprof -top /tmp/temporal.heap.pprof
```

## Local 3 Node x 4 Shard Benchmark Result

Environment:

- Scylla `2026.2.0` Docker image, 3 nodes, `--smp 4` per node.
- Benchmarks ran from a Go container attached to the Scylla Docker network so peer RPC addresses were reachable.
- Existing test helper creates `SimpleStrategy`, RF=1 keyspaces. Tablets were disabled for new keyspaces because
  Scylla rejects `SimpleStrategy` with tablets enabled.
- Data directories were bind-mounted under `/tmp` to avoid host root filesystem critical disk utilization.

Baseline was `HEAD` plus only the benchmark harness backport. Optimized was this patch with
`CASSANDRA_MAX_CONNS=12` and `CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2`.

| Benchmark | Baseline avg ns/op | Optimized avg ns/op | Delta |
| --- | ---: | ---: | ---: |
| `HistoryNodeAppendRead/append` | 1,092,328 | 1,087,782 | -0.4% |
| `HistoryNodeAppendRead/read` | 1,227,479 | 1,212,039 | -1.3% |
| `QueueV2EnqueueRead/enqueue` | 3,389,890 | 1,122,958 | -66.9% |
| `QueueV2EnqueueRead/read` | 2,267,256 | 1,147,147 | -49.4% |
| `QueueV2EnqueueRead/range_delete` | 4,589,317 | 3,399,876 | -25.9% |
| `QueueV2EnqueueRead/list` | 114,611,145 | 112,578,046 | -1.8% |
| `QueueEnqueueRead/enqueue` | 2,256,234 | 1,082,150 | -52.0% |
| `QueueEnqueueRead/read` | 1,215,493 | 1,213,074 | -0.2% |
| `HistoryNodeMultiBranchRead` | 11,824,248 | 12,536,961 | +6.0% |
| `TaskQueueUserDataBuildIDCount/limited_count` | 1,142,512 | 1,103,693 | -3.4% |

Additional current-state matching task queue profile:

| Benchmark | Current avg ns/op |
| --- | ---: |
| `MatchingTaskQueue/legacy/create` | 1,281,397 |
| `MatchingTaskQueue/legacy/read` | 1,280,243 |
| `MatchingTaskQueue/legacy/delete` | 1,087,646 |
| `MatchingTaskQueue/fair/create` | 1,299,069 |
| `MatchingTaskQueue/fair/read` | 1,239,674 |
| `MatchingTaskQueue/fair/delete` | 1,100,506 |

## Server Workload Result

Baseline and optimized worktrees were also run against live Temporal servers backed by the same shape of Scylla cluster:
3 Scylla containers and `--smp 4` each. Baseline was clean `HEAD` plus only the load-generator harness backport, using
the existing Cassandra schema and config defaults. Optimized was this patch, with the new `history_node` and QueueV2
schemas, `numHistoryShards: 12`, `maxConns: 12`, and `maxExcessShardConnectionsRate: 2`. Each server ran all Temporal
services in one process. Rootless Podman in this environment could not apply `--cpuset-cpus`, so this validates server
behavior on 3 x 4 Scylla shards but does not perfectly reproduce CPU-affinity isolation.

The load generator was `cmd/tools/scyllaload`, run inside each server container against `127.0.0.1:7233`.

| Workload | Baseline samples workflows/sec | Optimized samples workflows/sec | Baseline avg | Optimized avg | Delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1000 workflows, concurrency 100, 1 activity, 128 byte payload | 101.55, 97.31, 94.83 | 125.43, 124.53, 125.65 | 97.90 | 125.20 | +27.9% |
| 1000 workflows, concurrency 100, 1 signal, 128 byte payload | 165.31, 164.87, 155.93 | 184.62, 194.02, 185.44 | 162.03 | 188.03 | +16.0% |

All twelve server workload samples completed `1000/1000` workflows with zero load-generator failures. Average elapsed
time improved from `10.22s` to `7.99s` for the activity workload and from `6.18s` to `5.32s` for the signal workload.
After the optimized runs, Scylla reported all three nodes `UN`; Elasticsearch visibility had 11,003 docs in
`temporal_visibility_v1_dev`. Cassandra table counts on one coordinator were `executions=34980`,
`history_node=60007`, `tasks_v2=88`, `queue_messages=0`, and `queue_message_id_ranges=0`. The QueueV2 and legacy queue
tables remain untouched by this workflow workload; queue coverage still comes from the persistence microbenchmarks
above.

Interpretation:

- The QueueV2 metadata cache removes the metadata read from hot enqueue paths after a queue is known. CAS-reserved
  message-ID ranges remove steady-state max-message-ID reads and per-message LWTs; the message row insert is now a
  regular write. Together these reduced QueueV2 enqueue latency by 66.9% in this microbenchmark.
- QueueV2 enqueues are locally serialized per queue. This avoids same-process writers exhausting or updating the cached
  ID range concurrently; cross-process writers are fenced by the range-table CAS.
- QueueV2 range reservations are partitioned by queue identity, so independent queues do not contend on one
  `queue_type`-wide LWT partition.
- QueueV2 metadata CAS conflicts invalidate the local queue metadata cache so a retry fetches the latest version instead
  of repeatedly using stale metadata.
- QueueV2 list latency improved 1.8% in the 100-queue microbenchmark before the final metadata bucketing pass. The final
  schema keeps the no-`ALLOW FILTERING` query shape and also spreads queue metadata across 12 fixed buckets, avoiding
  both cluster-wide filtering and a single large metadata partition as queue counts grow.
- The legacy queue store now reserves message-ID ranges with CAS and uses regular inserts for the namespace replication
  queue and its DLQ path. This removes the steady-state `SELECT message_id ... ORDER BY message_id DESC LIMIT 1` read
  and per-message `IF NOT EXISTS` from same-process appends. It reduced legacy queue enqueue latency by 52.0% in the
  microbenchmark.
- The legacy queue message table remains a residual large-partition risk for very long-lived namespace replication
  queues. A full bucketed redesign should add a persisted minimum live bucket/delete cursor, then teach reads and DLQ
  range deletes to walk bucket partitions without scanning from bucket zero on every cleanup.
- The connection/config changes are otherwise neutral in this microbenchmark.
- The `history_node` partition-key change reduces partition growth and isolates branches, but this RF=1 latency
  microbenchmark does not show a read-latency win. The expected benefit should be validated with Scylla hot-partition,
  partition-size, and per-shard CPU metrics under reset/branch-heavy server workloads.
- QueueV2 no longer uses LWT for message inserts. It still uses LWT when creating queue metadata, updating queue
  metadata, and reserving message-ID ranges. Reserving ranges trades lower enqueue write latency for possible ID gaps
  after a writer crash; normal reads and range deletes are gap-tolerant, while `ListQueues` message count can overstate
  the exact row count in that crash case.
- Matching task queue writes remain on a conditional batch that includes task rows plus the task-queue metadata row.
  Splitting this into regular task inserts and a separate metadata CAS is not safe without a larger redesign: a CAS
  failure after task inserts would leave orphan/duplicate visible tasks, while a metadata success before task inserts
  can advance read levels over rows that were never written.

## CCM Profile Run

The optimized tree and regular Temporal `v1.31.2` were profiled against a CCM-managed Scylla `2026.1.7` cluster on
`2026-07-27`: 3 nodes, `--smp 4`, one datacenter, three racks, RF=3 Temporal keyspace, and Elasticsearch 7.10.1 for
visibility only. Temporal ran all services in one process with Cassandra seed `127.0.80.1`, pprof on `127.0.0.1:7936`,
and Prometheus metrics on `127.0.0.1:8000`.

Regular Temporal was the upstream `v1.31.2` tag (`19a774302`) with `numHistoryShards: 4` and the stock Cassandra
datastore config. Optimized Temporal was this patch on commit `11a35d86a`, with `numHistoryShards: 12`, Scylla gocql,
`maxConns: 12`, `maxExcessShardConnectionsRate: 2`, and the schema/query changes described above.

`ccm node1 nodetool status` confirmed all nodes `UN`:

| Address | Rack |
| --- | --- |
| `127.0.80.1` | `rack1` |
| `127.0.80.2` | `rack2` |
| `127.0.80.3` | `rack3` |

| Workload | Regular v1.31.2 | Optimized | Delta |
| --- | ---: | ---: | ---: |
| 1000 workflows, concurrency 100, 1 activity, 128 byte payload | 139.32 workflows/sec | 196.88 workflows/sec | +41.3% |
| 1000 workflows, concurrency 100, 1 signal, 128 byte payload | 253.25 workflows/sec | 286.74 workflows/sec | +13.2% |

All four CCM samples completed `1000/1000` workflows with zero load-generator failures. The load tool recorded pprof and
Prometheus endpoint reachability under `/tmp/temporal-regular-profile` and `/tmp/temporal-ccm-profile`; all Temporal
and Scylla metrics endpoints returned `200 OK`. The first temporary Elasticsearch container later reported flood-stage
disk watermark blocks while processing visibility queue retries. The regular rerun disabled ES disk thresholds. Treat
the profile data as valid for Cassandra/Scylla persistence hotspots, but not as a clean visibility backend latency
measurement.

Server CPU profile summaries:

- Activity run: 30s profile, 15.03s total samples. Top flat entries were Go runtime/GC helpers, including
  `runtime.(*lfstack).pop` at 12.24% flat and `runtime.gcDrain` at 32.87% cumulative. History queue execution appeared
  as `go.temporal.io/server/service/history/queues.(*executableImpl).Execute` at 13.17% cumulative.
- Signal run: 30s profile, 11.76s total samples. Top flat entries were again runtime/GC helpers, with
  `runtime.(*lfstack).pop` at 7.48% flat and `runtime.gcDrain` at 34.27% cumulative. Workflow mutable-state loading
  appeared at 1.36% cumulative.

Scylla metric deltas during the CCM runs show the remaining write contention is in mutable state and matching task CAS,
not in QueueV2:

| Workload | Regular top Paxos counters across 3 nodes | Optimized top Paxos counters across 3 nodes |
| --- | --- | --- |
| Activity | `executions$paxos` 84,112 writes / 42,066 reads; `tasks$paxos` 22,396 writes / 11,211 reads | `executions$paxos` 84,000 writes / 42,000 reads; `tasks$paxos` 25,156 writes / 12,579 reads |
| Signal | `executions$paxos` 48,060 writes / 24,030 reads; `tasks$paxos` 10,716 writes / 5,358 reads | `executions$paxos` 48,112 writes / 24,057 reads; `tasks$paxos` 9,574 writes / 4,791 reads |

Non-Paxos hot tables during the same windows were `executions`, `history_node`, and `tasks`. QueueV2 tables had no
meaningful traffic in these workflow workloads, so their LWT reduction remains covered by the persistence
microbenchmarks. The next performance redesign should target the `executions` conditional batch only if it can preserve
the shard/workflow/task atomicity guarantees described in the LWT audit above.
- Switching matching task-write batches from logged to unlogged was tested and rejected. On the same 3 node x 4 shard
  Scylla cluster, legacy create changed from `2,347,673` to `2,361,531 ns/op` (`+0.6%`) while fair create changed from
  `2,349,971` to `2,308,822 ns/op` (`-1.8%`). The mixed/noisy result is not enough to justify changing the conditional
  range-fenced write path.
- Cluster membership queries no longer append `ALLOW FILTERING` for partition-local scans or full primary-key equality
  reads. The store still keeps `ALLOW FILTERING` for host-ID-without-role, RPC address, session-start, and heartbeat
  filters that cannot be served by the table's primary key alone.
- Worker-versioning build-ID limit checks now pass the configured threshold into persistence. Cassandra/Scylla uses a
  `SELECT task_queue_name ... LIMIT ?` query for that path instead of `COUNT(*)`, so reads stop once the limit is
  reached. The 100-mapping microbenchmark shows only a 3.4% latency reduction, but the data read is bounded by the
  configured limit rather than by all task queues mapped to a build ID.
- Worker-versioning build-ID listing now issues the next Cassandra page explicitly when `PageState()` is returned.
  Previously the loop checked the page token without creating a new iterator, which could spin on large build-ID
  mappings instead of advancing to the next page. It also stops if Cassandra returns the same empty page token twice,
  matching the QueueV2 list guard against repeated empty paging tokens.
- History branch reads and history-tree branch scans now read the Cassandra page token after consuming each iterator,
  avoiding dropped next-page tokens on large histories or many branches.
- History branch row conversion now returns typed field errors instead of panicking on malformed rows, and closes the
  iterator before returning those errors.
- History scheduled-task and timer-task scan templates now keep all `executions` predicates separated in the generated
  CQL. This is a correctness cleanup in the same hot table path, not a benchmarked latency change.
- `ListConcreteExecutions` now closes the Cassandra iterator on normal completion and malformed-row early exits, and
  returns close errors on the normal path. This avoids leaking driver-side scan resources during shard-level
  `executions` table walks.
- Matching `GetTasks` now closes the Cassandra iterator before returning malformed-row errors in both classic and fair
  task stores. Normal reads already closed the iterator; this covers the early-exit path on the matching task hot table.
- Nexus endpoint listing now closes iterators before version-conflict and malformed-row errors on first-page and
  continuation scans, matching the same driver-resource cleanup pattern used in the hotter persistence scans.
- The range-delete row was measured separately with `-benchtime=100x -count=3` and `CASSANDRA_MAX_CONNS=12` on both
  baseline and optimized worktrees. The optimized samples were `3,439,679`, `3,391,419`, and `3,368,529 ns/op`.
- The list row was measured separately with `-benchtime=100x -count=3`, `CASSANDRA_MAX_CONNS=12`, and 100 seeded queue
  metadata rows. Baseline samples were `115,401,614`, `114,936,553`, and `113,495,269 ns/op`; optimized samples were
  `111,834,750`, `111,807,282`, and `114,092,106 ns/op`.
- The final QueueV2 enqueue row includes the message-ID range allocator. It was measured separately with
  `-benchtime=100x -count=3` and `CASSANDRA_MAX_CONNS=12`. The optimized samples were `1,147,301`, `1,113,305`, and
  `1,108,269 ns/op`. Compared with the prior cached-LWT optimized average of `1,184,106 ns/op`, the allocator reduces
  enqueue latency by another 5.2%.
- The final bucketed QueueV2 list schema was covered by unit tests for schema shape, no `ALLOW FILTERING`, invalid page
  tokens, repeated empty Cassandra page tokens, and bucket-boundary page tokens. A follow-up 3-node Scylla list
  benchmark attempt failed before measurement because local Scylla 2026.2.0 containers repeatedly banned joining nodes
  during raft topology bootstrap, even with sequential node startup. The local live-profile attempt on 2026-07-27 used
  three temporary `scylladb/scylla:latest` containers with `--smp 4`, `--memory 4G`, `--developer-mode 1`, and
  per-node listen/broadcast names. The seed node accepted CQL after 3 seconds, but both joining nodes logged
  `raft_topology - received notification of being banned from the cluster` before CQL was ready. The temporary
  containers and network were removed after the failed bootstrap. A retry against the repo-pinned
  `docker.io/scylladb/scylla:2026.1` image could not start because the local container runtime could not pull the image
  from Docker Hub without registry credentials.
- QueueV2 `ListQueues` now closes the metadata-list iterator before returning row-level metadata/count errors, avoiding
  driver-side scan resource leaks while walking bucketed queue metadata partitions.
- The legacy queue rows were measured separately with `-benchtime=100x -count=3`, `CASSANDRA_MAX_CONNS=12`, and
  `CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE=2`. Baseline was `HEAD` plus the benchmark harness and test-helper
  benchmark compatibility only. Baseline enqueue samples were `2,255,904`, `2,247,092`, and `2,265,705 ns/op`;
  optimized enqueue samples with the range allocator were `1,078,713`, `1,086,525`, and `1,081,212 ns/op`. Baseline
  read samples were `1,193,265`, `1,193,471`, and `1,259,743 ns/op`; optimized read samples were `1,193,336`,
  `1,212,139`, and `1,223,748 ns/op`. Compared with the prior cached-LWT optimized enqueue average of
  `1,157,858 ns/op`, the range allocator reduces enqueue latency by another 6.5%.
- The matching task queue rows were measured separately on the optimized worktree with `-benchtime=50x -count=3` and
  `CASSANDRA_MAX_CONNS=12`. Legacy samples were: create `1,278,914`, `1,310,330`, `1,254,947`; read `1,268,179`,
  `1,287,101`, `1,285,449`; delete `1,097,834`, `1,086,432`, `1,084,673 ns/op`. Fair queue samples were: create
  `1,317,606`, `1,275,299`, `1,304,303`; read `1,208,190`, `1,292,227`, `1,218,606`; delete `1,122,207`,
  `1,086,902`, `1,092,410 ns/op`.
- The task-queue user data build-ID count row was measured separately on the optimized worktree with
  `-benchtime=100x -count=3`, `CASSANDRA_MAX_CONNS=12`, and 100 task queues mapped to one build ID. The exact
  `COUNT(*)` samples were `1,131,529`, `1,150,767`, and `1,145,239 ns/op`; the limited threshold-read samples were
  `1,103,459`, `1,105,617`, and `1,102,004 ns/op`.

Collect Temporal metrics:

- persistence latency: `persistence_latency`, tagged by `operation`
- persistence request/error rate: `persistence_requests`, `persistence_errors`, `persistence_error_with_type`
- service latency and task latency: `service_latency`, `task_schedule_to_start_latency`, `task_load_latency`
- focus operation tags: `CreateWorkflowExecution`, `UpdateWorkflowExecution`, `AppendRawHistoryNodes`,
  `ReadHistoryBranch`, `ReadHistoryBranchReverse`, `CreateTasks`, `GetTasks`, `CompleteTasksLessThan`,
  `EnqueueMessage`, `ReadQueueMessages`, `DeleteMessagesBefore`, `CountTaskQueuesByBuildId`

Example Prometheus queries with the development configs' histogram timers:

```promql
histogram_quantile(0.99, sum by (le, operation) (rate(persistence_latency_bucket[5m])))
sum by (operation) (rate(persistence_requests[5m]))
sum by (operation) (rate(persistence_errors[5m]))
histogram_quantile(0.99, sum by (le) (rate(task_schedule_to_start_latency_bucket[5m])))
```

Collect Scylla metrics:

- per-shard CPU utilization
- coordinator foreground read/write latency
- LWT/paxos latency and contention counters
- large partition warnings
- hot partition/table metrics for `executions`, `history_node`, `tasks_v2`, `queue_messages`

Workloads:

- high workflow start rate across many workflow IDs
- high signal/update rate against active workflows
- high activity and workflow task queue load on a small set of task queues
- history-heavy workflows with long event histories and branch reads
- DLQ/QueueV2 enqueue, read, and range-delete stress
- `cmd/tools/scyllaload` for repeatable workflow start/activity/signal load against a running server

Compare:

- p50/p95/p99 Temporal persistence latency
- completed workflows per second
- top CPU and heap profile entries from the load generator and Temporal server
- `scyllaload.metadata.json` endpoint status and Scylla/Temporal environment settings
- Scylla CPU balance across 12 shards
- LWT operation rate and p99 latency
- partition size and hot-partition warnings before/after
