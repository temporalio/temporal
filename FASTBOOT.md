# Fast boot: making functional test clusters start faster

An investigation into what `tests/` pays to stand up a test cluster, with a
reproducible baseline and ranked suggestions.

## TL;DR

Booting a functional test cluster takes **~42 ms** and allocates **~20 MB /
260 k objects**, but it leaves **~2 400 goroutines** running. Of those, **2 240
(94 %) are idle task-scheduler workers** sized from production dynamic-config
defaults (512 + 512 + 512 + 64 + 512 + 128). They cost 5 MB of stacks per
cluster, and the cluster pools hold 1.5 × GOMAXPROCS clusters at once — at 8
clusters that is **19 300 goroutines and 42 MB of stacks**.

Wall-clock boot time is not the main problem; **resident cost per cluster is**,
because it bounds how many clusters a test binary can hold and therefore how much
the pool can amortize. The three cheapest wins are: cap the scheduler worker
pools in test mode (or make them grow lazily), stop emitting per-fx-hook debug
logs, and remove the hardcoded 1 s floor in frontend shutdown.

## Implementation result

The implementation is in progress. The measurement harness, cheap-cluster work,
database lifetime fix, and per-test provider are implemented. Go test parallelism
16 / live 40 / warm 12 now passes all three CI shards without `-race` while meeting the
Gate 2 acquire-latency and RSS limits. Independent core and worker fillers bring
warm-eligible p99 to 17–33 ms across those shards. Race-enabled per-test and
pooled controls still overload this machine at the CI scheduler width, so neither
is valid flake-rate evidence. CI-class race validation and the repeated Phase 3
runs are still pending; pooled mode remains the default and the sharing machinery
has not been deleted.

### Changes implemented

- The run-level JSONL stream records cluster creation, acquisition and
  destruction, boot phases, namespaces, live-cluster counts, goroutines, heap,
  `Sys`, RSS and the process exit code. Acquisitions identify pooled, warm-hit,
  warm-miss and custom sources. `tools/testreport` reports total latency and the
  warm-eligible population separately for both single and multi-run gates.
- Functional SQLite databases are copied from a process-wide schema template
  instead of replaying all DDL for every cluster. Per-test clusters use unique
  file-backed copies.
- The two default namespaces and search attributes are seeded before services
  start, avoiding registration and cache-propagation polling on handoff.
- SDK workers stop before server teardown. The production frontend drain no
  longer imposes an unconditional one-second floor when nothing remains to
  drain.
- Per-fx-hook debug logging is gated, and masked server config is not constructed
  when debug logging is disabled.
- SQLite connection ownership now has an explicit database lease. Transient
  persistence wrappers may close and later reopen while the cluster-owned lease
  keeps the database pinned. Cluster teardown releases the lease after managers
  close, allowing the pool entry and file to be removed. This fixes the retained
  database leak without breaking standalone server restarts, where wrapper
  references legitimately reach zero between starts.
- The opt-in per-test provider owns a real live-cluster semaphore. A lease holds
  its slot through cluster destruction; `-test.parallel` is no longer treated as
  the resource bound.
- `TEMPORAL_TEST_WARM_SPARES` is one total bound split between pristine core and
  worker-enabled inventories. Each inventory has one filler, bounding background
  prewarming to two concurrent boots. A request receives the matching class, is
  used once, and is destroyed on release.
- Dynamic config with namespace, task-queue or destination precedence can be
  applied when a spare is handed off. Global/startup-sensitive config and cluster
  options still boot inline. This distinction is required: applying
  `BuildIdScavengerEnabled` after the worker service starts leaves the scavenger
  workflow unregistered.
- The ten suites that previously needed suite-scoped ownership now declare a
  suite-wide worker-service capability. Every leaf still receives its own
  cluster in per-test mode. Representative worker suites pass with this model.
- Warm-spare readiness probing is used only for plaintext prebuilt clusters.
  Inline custom clusters, including mTLS, retain their protocol-specific startup
  path.
- `TestWorkflowTaskRedirectInRetry*` no longer signals that a workflow-task
  timeout occurred at the exact same one-second deadline as the server timeout.
  Its signal is delayed until two seconds and the SDK deadlock watchdog remains
  later at five seconds, so the redirect cannot race timeout persistence under
  full-suite load.

One legacy suite, `ActivityApiResetClientTestSuite`, still constructs its cluster
directly through `FunctionalTestBase`; it has not yet been migrated to `NewEnv`.

### Wins measured

The original exploratory shard 1/3 run (`GOMAXPROCS=12`, race detector off)
showed that the model can beat the pooled control:

| metric | pooled control | per-test experiment |
|---|---:|---:|
| wall time | 5m53.3s | **5m33.2s** |
| peak RSS | 3,849 MB | **3,747 MB** |
| peak goroutines | 104,188 | **59,101** |
| acquire p50 / p99 | 6.1 us / 95.1 ms | **8.7 us / 85.9 ms** |
| exit code | 0 | **0** |

The per-test run created 560 clusters and accounted for 1,298 namespaces. Mean
boot time was 31.3 ms. SQLite templates, namespace preseeding, bounded scheduler
counts, readiness probing and log gating all contribute to this result.

More recent shard 2/3 sweeps exercise the suites with the deepest parallel
subtest trees and most worker-service clusters:

| go-test parallelism / live / warm | result | wall | acquire p50 / p99 | peak clusters | peak RSS | learning |
|---|---:|---:|---:|---:|---:|---|
| 20 / unbounded / 2 | pass | 5m17s | 13.8 us / 195.4 ms | 33 | 4,499 MB | Too few spares; functionally stable once scheduler headroom is sufficient. |
| 20 / 40 / 4 core-only | pass | 5m20s | 13.8 us / 138.0 ms | 36 | 4,548 MB | A real ownership cap works, but default spares cannot absorb worker/custom startup bursts. |
| 20 / 40 / 4, global boot throttle 6 | pass | 5m19s | 13.1 us / 172.8 ms | 37 | 4,628 MB | Throttling all inline boots made p99 worse; the experiment was removed. |
| 20 / 40 / 12 split | fail | 5m15s | 8.7 us / 123.8 ms | 43 | 4,622 MB | An unsafe first version applied startup-only config after boot; the scavenger test exposed it and the provider now rejects those requests. |
| 16 / 40 / 12 split, startup guard | fail | 5m18s | **9.3 us / 81.4 ms** | 41 | **4,681 MB** | Meets latency and RSS gates, but exposed a pre-existing versioning test timing hazard. |
| 16 / 40 / 12, timing fix, shard 0/3 | pass | 5m04s | 9.4 us / **86.1 ms** | 36 | **4,233 MB** | First complete Gate 2 run, shard 0. |
| 16 / 40 / 12, timing fix, shard 1/3 | pass | 6m14s | 9.3 us / **63.5 ms** | 43 | **4,346 MB** | First complete Gate 2 run, shard 1. |
| 16 / 40 / 12, timing fix, shard 2/3 | pass | 5m17s | 9.5 us / **87.2 ms** | 39 | **4,641 MB** | First complete Gate 2 run, shard 2. |
| 16 / 40 / 12, dual filler, shard 0/3 | pass | **4m58s** | 9.5 us / **90.6 ms** | 36 | **4,163 MB** | Warm-eligible p99 32.9 ms; 636 hits / 13 misses / 63 custom. |
| 16 / 40 / 12, dual filler, shard 1/3 | pass | **6m09s** | 9.8 us / **82.7 ms** | 42 | **4,523 MB** | Warm-eligible p99 17.1 ms; 643 hits / 7 misses / 57 custom. |
| 16 / 40 / 12, dual filler, shard 2/3 | pass | 5m19s | 9.5 us / **87.7 ms** | 40 | **4,707 MB** | Warm-eligible p99 19.2 ms; 692 hits / 8 misses / 52 custom. |

The highest passing peak, 4,707 MB, remains below the current +25% allowance
derived from the 3,849 MB pooled control (4,811 MB). The table is exploratory,
not the required 10-run A/B result.

The table above keeps the history task-scheduler worker cap at 64. A separate
single-variable sweep tested whether per-test ownership made a lower static cap
safe and cheaper:

| history worker cap | shard | result | wall | warm p50 / p99 | peak goroutines | peak RSS |
|---:|---:|---:|---:|---:|---:|---:|
| 64 | 0 | pass | 4m58s | 9.3 us / 32.9 ms | 52,113 | **4,163 MB** |
| 64 | 1 | pass | 6m09s | 9.4 us / 17.1 ms | 58,324 | **4,523 MB** |
| 16 | 0 | pass | 5m05s | 2.5 ms / 37.8 ms | **43,597** | 5,063 MB |
| 16 | 1 | pass | 6m27s | 2.5 ms / 66.3 ms | **44,447** | 4,811 MB |
| 32 | 0 | pass | 5m04s | 2.6 ms / 45.2 ms | 51,052 | 4,959 MB |
| 32 | 1 | fail | 6m22s | 2.5 ms / 29.3 ms | 49,604 | 5,031 MB |
| 48 | 0 | pass | 4m59s | 2.5 ms / 40.2 ms | 54,942 | 5,257 MB |

Cap 16 is live: shards 0 and 1 passed, the known shard-2 retry-history leaf
passed three focused repeats, and XDC/NDC passed while retaining their production
replication counts. Its cluster boot benchmark reached 205 core goroutines. It is
not a viable suite default, however. Caps 16, 32 and 48 all exceeded the RSS gate;
32 also exposed schedule-migration and eager-start failures. Lower static
concurrency leaves more queued work and longer-lived heap, offsetting idle stack
savings. Cap 64 remains the lowest tested setting that passes the complete Gate 2
resource envelope.

The first race-enabled shard 2/3 comparison used `GOMAXPROCS=12` and
`-parallel=16`. A second comparison used Go's CI-default `-parallel=12`. Both
modes failed broadly from the same late resource and persistence deadline
cascades, with no race detector report:

| go-test parallelism / mode | result | wall | clusters | acquire p50 / p99 | peak goroutines | peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| 16 / pooled control | fail | 12m58s | 319 | 42.6 us / 2.31 s | 112,251 | 11,544 MB |
| 16 / per-test, live 40 / warm 12 | fail | 10m37s | 766 | 548.9 us / 4.59 s | 56,837 | 9,840 MB |
| 12 / pooled control | fail | 11m19s | 320 | 25.3 us / 2.22 s | 106,463 | 13,071 MB |
| 12 / per-test, live 40 / warm 12 | fail | **8m33s** | 766 | 252.6 us / **1.28 s** | **53,542** | **11,420 MB** |
| 12 / per-test, dual filler | fail | 10m06s | 766 | 364.7 us / 1.49 s | **53,600** | **10,000 MB** |
| 16 / per-test, worker cap 16, shard 0 | fail | 7m48s | 726 | 14.6 ms / 1.21 s | 48,616 | 10,135 MB |
| 16 / per-test, worker cap 16, shard 1 | fail | 9m17s | 721 | 13.5 ms / 1.21 s | **42,739** | 10,567 MB |

This is useful stress evidence, not a performance win or a valid A/B sample.
At the CI-default scheduler setting, per-test ownership finished 25% sooner, cut
peak goroutines almost exactly in half and used 1.65 GB less RSS, despite creating
every leaf cluster independently. The pooled control failed in the same activity,
versioning and update families, proving that the local race failures are not
introduced by per-test ownership. This machine cannot produce passing race-mode
flake evidence for either mode at this shard width.

The event join also explained the original per-test acquisition tail: 151
warm-eligible requests found an empty serialized inventory and booted inline.
Independent fillers reduced the measured race-mode misses to 90, but their extra
boot contention increased wall time and left warm-eligible p99 at 1.00 s. On this
machine the change solves normal-mode refill without solving the overloaded race
run. More fundamentally, custom requests exceed 1% of acquisitions and each takes
more than 100 ms under `-race`, so the report now gates warm-eligible requests and
reports custom boots and total latency separately.

### Learnings

1. **Go test parallelism is not a live-cluster bound.** A parallel parent releases
   its scheduler slot while waiting for subtests but retains its cleanups and
   cluster. `-parallel 20` therefore produced 31 test-owned clusters. The provider
   semaphore is the correct ownership seam.
2. **The live bound cannot be too low.** A cap of 24 held total clusters at 32,
   but acquisition queued for up to nine seconds. Parent-scoped 90-second contexts
   expired while nested subtests waited. A cap near 40 covers the observed
   31-owner burst without turning admission into test latency.
3. **Go-test scheduler headroom and resource bounds are different knobs.** Parallelism 12
   starves nested parallel suites; scheduler 20 avoids that but creates a larger
   startup burst. Scheduler 16 is the best measured compromise so far.
4. **Warm inventories need capabilities, not suite ownership.** Core and
   worker-enabled clusters can both remain pristine. Choosing the right inventory
   at handoff preserves per-test isolation without restoring suite-scoped reuse.
5. **“Dynamic config” does not always mean hot-swappable.** Precedence tells us
   which overrides the existing shared path already applies safely at runtime.
   Global settings remain startup config unless they are explicitly proven safe.
6. **Global boot throttling is the wrong control.** Throttling inline boots merely
   moves contention into acquire wait and regresses p99. The provider instead
   bounds each capability inventory to one filler.
7. **The database must be leased at cluster lifetime.** Wrapper reference counts
   are allowed to hit zero temporarily. A cluster-level lease is what makes both
   restart correctness and eventual cleanup true.
8. **A test signal must follow the condition it represents.** The versioning test
   sent `timedoutTask` after one second while the server timeout was also one
   second, then treated three receives as proof of three committed timeouts. Under
   load the redirect could win that race and retain an extra task-started event.
   Separating the deadlines makes the existing channel protocol truthful; the
   formerly failing variant then passed 10 consecutive focused runs.
9. **Race instrumentation changes the safe concurrency envelope.** At
   `GOMAXPROCS=12`, both pooled and per-test shard 2 runs exhausted enough CPU and
   memory to trigger unrelated context deadlines at scheduler widths 16 and 12.
   Lowering the scheduler improved wall time and the per-test acquisition tail but
   did not make either mode reliable. The absence of a race report does not make
   those runs successful, and their failure counts cannot be used to compare
   correctness.
10. **Two bounded fillers solve normal refill, not an overloaded race run.** Core
    and worker inventories independently achieve 17–33 ms warm-eligible p99 on
    all three non-race shards. Under `-race`, they reduce misses but compete with
    tests for the same saturated CPU, worsening wall time on this machine. More
    filler concurrency is not justified without passing CI-class evidence.
11. **The acquisition gate needs an explicit population.** Generic warm spares
    cannot serve mTLS, fault-injection, custom-logger or startup-sensitive dynamic
    config requests. Those requests are more than 1% of this shard and necessarily
    boot inline. Under `-race`, their boot time alone makes an all-request p99 below
    100 ms unattainable. The report therefore keeps total latency, warm-eligible
    latency, hit/miss counts and custom counts distinct.
12. **The largest shard is a stress ceiling, not the tuning oracle.** Versioning3
    dominates shard 2's cluster churn. Its result must be balanced against shards
    0 and 1 and the full A/B distribution before changing default inventory or
    scheduler settings.
13. **Cluster teardown now has a concrete leak proof.** Eighteen fresh,
    worker-enabled clusters completed build, run and teardown cycles with no
    unexpected goroutines and no unexpected retained objects. The obsolete
    `database/sql.(*DB).connectionOpener` exemption was removed before the passing
    run, so SQLite cleanup is covered by the assertion rather than hidden by an
    ignore. Existing named gRPC and SDK worker exemptions remain separate known
    work.
14. **The event stream proves one-use ownership and bounded namespace state.** In
    the three passing dual-filler shards, no cluster ID was acquired more than
    once, every one of the 2,213 created clusters started with exactly the two
    preseeded namespaces, and each run finished with zero live clusters. Tests may
    register additional namespaces while they own a cluster, but that cluster is
    destroyed rather than returned to another test.
15. **A lower static worker cap can increase total memory.** Per-test ownership
    reduces aggregate throughput demand, but it does not eliminate task dependency
    depth or queued state. Caps 16, 32 and 48 saved idle scheduler goroutines yet
    retained enough additional heap that every setting exceeded the 4,811 MB RSS
    gate. Cap 16 also left local race runs above 10 GB and did not prevent deadline
    failures. The working static floor is therefore 64; getting the idle cost lower
    safely requires on-demand growth to the unchanged ceiling, which remains
    deferred.

### Current blockers

- **Gate 2 is green only without `-race`.** One complete three-shard run passed
  with p99 between 63.5 and 87.2 ms and RSS between 4,233 and 4,641 MB. Initial
  race-enabled shard 2 runs failed in both modes at `-parallel=16` and the
  CI-default 12. At 12, per-test was materially lighter and faster than pooled,
  but both modes suffered the same late deadline cascade. Passing race and 10-run
  A/B evidence must be collected on CI-class hardware.
- The first shard 2 run after the timing change still exited nonzero, but its
  failure block was lost in truncated expected-error logs; an immediately repeated
  JSON-filtered run passed. Until the repeat sweep quantifies or reproduces that
  failure, the flake-rate criterion is unproven.
- The default per-test settings are still experimental. They must not be flipped
  until all three shards pass repeatedly with and without `-race`.
- The Phase 3 leak check now passes without a SQLite connection-opener exemption,
  and provider tests prove each released cluster is destroyed rather than reused.
  The 10-run A/B comparison and flake-rate quantification are still missing, so
  the default flip and deletion of pool reuse, poison tracking, shared log fanout
  and dedicated-cluster guards remain blocked.
- Lazy task-scheduler growth and lazy per-namespace workers were deliberately
  skipped. The test-only scheduler ceiling remains 64, so the boot benchmark is
  still roughly 500 goroutines per core cluster rather than the plan's 200 target.
  The temporary cap cannot yet be retired, and the goroutine target cannot be
  claimed.
- The one-Fx-graph rewrite remains deferred because the run-level evidence has
  not justified that invasive conditional step.

## How to reproduce

Instrumentation and benchmarks added by this investigation:

- `tests/testcore/cluster_boot_bench_test.go` — `BenchmarkClusterBoot`,
  `TestClusterBootBaseline`, `TestClusterBootScaling`.
- `bootPhaseObserver` in `tests/testcore/test_cluster.go` — an optional hook that
  reports the duration of each boot phase (`persistence`, `cluster-metadata`,
  `fx-graph`, `service-start`, `total`). Nil in normal runs.
- `newTemporal` / `NewCluster` / `newClusterWithPersistenceTestBaseFactory` use
  the small `clusterTest` interface, so the boot path can be driven by benchmarks
  and by a warm-spare owner that is not tied to an unrelated `*testing.T`.

```bash
# per-boot phases, allocations, goroutines
go test -tags test_dep ./tests/testcore -run '^$' -bench BenchmarkClusterBoot -benchtime 5x

# same numbers in a plain test run, plus teardown and stack growth
go test -tags test_dep ./tests/testcore -run '^TestClusterBoot' -v

# where the CPU goes (set the log level, or logging dominates the profile)
TEMPORAL_TEST_LOG_LEVEL=error go test -tags test_dep ./tests/testcore -run '^$' \
    -bench 'BenchmarkClusterBoot/Core$' -benchtime 5x -cpuprofile boot.prof

# which goroutines are alive right after boot
# (add a pprof.Lookup("goroutine").WriteTo(f, 1) after newBenchCluster)
```

Wall-clock numbers below are machine dependent (measured on an M-series mac,
GOMAXPROCS=12, sqlite in-memory persistence, race detector off unless stated).
**Allocation, object, goroutine and stack counts are machine independent — use
those to compare runs.**

## Baseline

`BenchmarkClusterBoot`, `-benchtime 5x`, default test log level (debug):

| variant | ns/op | persistence | fx-graph | service-start | namespaces | boot MB | boot objects | goroutines |
|---|---|---|---|---|---|---|---|---|
| Core | 29.9 ms | 5.6 ms | 19.0 ms | 4.4 ms | — | 15.6 | 227 549 | 2 372 |
| Core + namespaces | 31.8 ms | 4.8 ms | 17.6 ms | 3.4 ms | 5.4 ms | 16.5 | 243 104 | 2 467 |
| + worker service | 41.0 ms | 7.6 ms | 26.4 ms | 6.2 ms | — | 20.8 | 274 274 | 2 512 |
| + worker + namespaces | 49.2 ms | 6.8 ms | 24.8 ms | 5.8 ms | 11.0 ms | 26.5 | 349 598 | 2 964 |

Single boot including teardown and SDK client/worker setup, which is what one
test actually pays:

| config | boot | persistence | fx | start | namespace | sdk start | teardown | alloc | objects | stacks | goroutines |
|---|---|---|---|---|---|---|---|---|---|---|---|
| default | 42 ms | 9 ms | 25 ms | 6 ms | 3 ms | 1 ms | 8 ms | 18 MB | 242 k | 5 MB | 2 394 |
| default + worker service | 32 ms | 4 ms | 22 ms | 5 ms | 6 ms | 0 ms | **1 015 ms** | 20 MB | 274 k | 3 MB | 2 506 |

With the race detector on (what CI runs), boot grows ~7×:

| config | boot | persistence | fx | start | goroutines |
|---|---|---|---|---|---|
| default | 320 ms | 158 ms | 133 ms | 24 ms | 2 347 |

Holding several clusters at once, which is what the pools do
(`TestClusterBootScaling`, 8 clusters):

| config | boot (parallel) | teardown | goroutines | stacks | heap in use |
|---|---|---|---|---|---|
| default | 114 ms | 19 ms | **19 334** | **42 MB** | 75 MB |

Fixed costs that are *not* per cluster: the `./tests` test binary is 221 MB and
takes ~2.6 s to start cold, ~0.03 s warm. Package init is not a bottleneck.

## Where the cost is

### 1. Idle task-scheduler workers: 94 % of goroutines

A goroutine profile taken right after boot (2 357 goroutines total, worker
service disabled, 4 history shards):

| count | goroutine |
|---|---|
| 1 600 | `common/tasks.(*FIFOScheduler[...]).processTask` |
| 640 | `common/tasks.(*SequentialScheduler[...]).pollTaskQueue` |
| 24 | grpc `CallbackSerializer.run` |
| ~90 | everything else (membership watchers, refresh loops, timer gates, shard controllers, grpc transports) |

The counts match the dynamic-config defaults exactly:

| setting | default | scheduler |
|---|---|---|
| `history.transferProcessorSchedulerWorkerCount` | 512 | FIFO |
| `history.timerProcessorSchedulerWorkerCount` | 512 | FIFO |
| `history.visibilityProcessorSchedulerWorkerCount` | 512 | FIFO |
| `history.memoryTimerProcessorSchedulerWorkerCount` | 64 | FIFO |
| `history.ReplicationProcessorSchedulerWorkerCount` | 512 | Sequential |
| `history.ReplicationLowPriorityProcessorSchedulerWorkerCount` | 128 | Sequential |
| `history.archivalProcessorSchedulerWorkerCount` | 512 | FIFO (archival tests only) |

`FIFOScheduler.Start` calls `updateWorkerCount(initialWorkerCount)` which starts
*all* workers eagerly (`common/tasks/fifo_scheduler.go`), regardless of load.
These are **host-level** pools, so shard count does not matter: 1, 4 and 16
shards all produce ~2 350 goroutines. Measured demand across 10 functional suites:
a peak of **6** concurrent tasks and a maximum queue depth of **0** — see A.0 for
the full table.

Measured effect of capping all six to 4 (`DynamicConfigOverrides`):

| | goroutines | stacks | boot | alloc | objects |
|---|---|---|---|---|---|
| default | 2 394 | 5 MB | 42 ms | 18 MB | 242 k |
| capped | 129 | 0 MB | 30 ms | 13 MB | 212 k |
| default, 8 clusters | 19 334 | 42 MB | 114 ms | — | 75 MB heap |
| capped, 8 clusters | 1 587 | 3 MB | 94 ms | — | 38 MB heap |

### 2. Debug logging of every fx hook

The default functional-test log level is **debug**
(`testlogger.NewTestLogger` → `zapcore.DebugLevel`, overridable with
`TEMPORAL_TEST_LOG_LEVEL`). `FxLogAdapter` logs `OnStart hook executing` /
`executed` / `OnStop hook …` / `invoking` at debug for *every* provider, in each
of the 4 service graphs plus the server graph. That is hundreds of lines per boot.

In the CPU profile, **16.6 % of all samples** are
`sharedClusterT.Logf → zapcore → os.File.Write`, and within the `fx-graph` phase
specifically **260 ms of 330 ms** is the log write. (In the benchmark those go to
stderr; in a real test they go to `t.Logf`, so the cost moves from syscalls to
retained memory — the test log buffer holds every line until the test ends.)

Measured effect of `TEMPORAL_TEST_LOG_LEVEL=error`:

| | boot | fx phase |
|---|---|---|
| default (debug) | 42 ms | 25 ms |
| error | 37 ms | 22 ms |
| error + capped schedulers | **20 ms** | **13 ms** |

Separately, `ServerImpl.Start` does `s.logger.Debug(s.so.config.String())`
(`temporal/server_impl.go:90`). The argument is evaluated unconditionally, so
`config.String()` → `masker.MaskYaml` → regexp work runs even when debug is off:
**~3.2 % of all boot allocations** (~38 k objects) for a line that is usually
discarded.

### 3. fx / dig graph construction: ~80 % of boot allocations

Allocation profile by object count, per boot:

| share | frame |
|---|---|
| 41 % | `fx.(*module).provide` — graph *registration* |
| 20 % | `dig.(*graphHolder).EdgesFrom` |
| 18 % | `dig.getParamOrder` |
| 15 % | `dig.(*Scope).getAllProviders` |
| 7 % | `fxreflect.Frame.String` (caller strings for every provider) |
| 2 % | `dig.paramSingle.DotParam` (DOT graph metadata, built eagerly) |

By service graph: **history 39 %, frontend 30 %, matching 11 %** of all boot
allocations.

The structural cause: `temporal.NewServer` builds **one fx container per
service** (`HistoryServiceProvider`, `FrontendServiceProvider`,
`MatchingServiceProvider`, `WorkerServiceProvider`), each re-registering the full
`GetCommonServiceOptions` set — `resource.DefaultOptions`, `chasm.Module`,
`ServiceTracingModule`, the membership module, and ~25 pass-through providers.
`GetCommonServiceOptions`' own doc comment already calls this out as a workaround
for propagating deps between graphs. In a onebox test cluster all four graphs live
in one process and share almost everything, and every new test cluster rebuilds
all of them from scratch.

### 4. Teardown: a hardcoded 1 s floor in frontend shutdown

`service/frontend/service.go`:

```go
requestDrainTime := max(time.Second, s.config.ShutdownDrainDuration())
```

`frontend.shutdownDrainDuration` defaults to 0, but the `max` puts a **1 s
floor** on the drain. `GracefulStop` then blocks until either all in-flight RPCs
finish or the 1 s timer fires and hard-stops the server. Any long poll still open
at shutdown — the system worker's pollers when the worker service is enabled, or
a test's SDK worker that was not stopped first — turns teardown into a flat
**~1 015 ms**. A cluster with no in-flight polls tears down in 8 ms.

Also in this area: `matching.Service.Stop` has a 2 s `AfterFunc` fallback, and
history's health check only reports `SERVING` after `InitialShardsAcquired` plus a
**hardcoded 5 s** stabilization sleep (`service/history/service.go:86`), so any
test that waits for history health pays 5 s.

### 5. sqlite test databases are never released

`common/persistence/sql/sqlplugin/sqlite/conn_pool.go` refcounts DSNs, but the
close path is commented out:

```go
e.refCount--
// if e.refCount == 0 {
// 	e.db.Close()
// 	delete(cp.pool, dsn)
// }
```

So every in-memory database created by every cluster in a test binary stays
resident for the process lifetime. This is very likely the "resource
accumulation" that `test_cluster_pool.go` works around with `maxLeases = 50` in
CI. Also, sqlite connections are capped at `SetMaxOpenConns(1)` /
`SetMaxIdleConns(1)`, so all persistence traffic for a cluster is serialized
through one connection.

### 6. Non-issues (measured, don't bother)

- **Namespace registration**: 3–6 ms for both namespaces. The
  `ForceSearchAttributesCacheRefreshOnRead` override plus read-through already
  avoid the cache-refresh waits that `RegisterNamespace`'s deadline loop guards
  against.
- **Shard count**: 1 / 4 / 16 shards → same goroutines (2 342 / 2 347 / 2 359)
  and same allocations. The expensive pools are host-level.
- **Cluster metadata writes**: 0–3 ms.
- **Package init / binary size**: 2.6 s cold, 0.03 s warm, once per test binary.

## Suggestions, ranked

### A. Make the task-scheduler pools lazy (and, as a stopgap, cap them in test mode)

*Impact: 2 394 → ~130 goroutines and 5 MB → 0 MB stacks per cluster; ~30 % faster
boot; 42 MB → 3 MB stacks at 8 clusters. Effort: low (test-mode cap) to medium
(lazy growth).*

#### A.0 What the pools are actually used for

Instrumenting `FIFOScheduler` / `SequentialScheduler` with peak counters and
running 10 functional suites (cron, child workflow, continue-as-new, describe,
cancel, activity, advanced visibility, signal, timer, client misc — 240 scheduler
instances):

| pool | configured workers | max concurrent tasks | max queue depth | max tasks submitted (per instance) |
|---|---|---|---|---|
| fifo (transfer / timer / visibility) | 512 | **6** | **0** | 20 |
| fifo (memory timer) | 64 | 0 | 0 | 0 |
| sequential (replication) | 512 | 0 | — | 0 |
| sequential (low-pri replication) | 128 | 0 | — | 0 |

So 2 240 goroutines exist to serve a peak of **6** concurrent tasks, the buffered
channel never held a single queued task, and **640 of them (the two replication
pools) process literally zero tasks** in single-cluster tests.

Two facts from the code decide how safe each option is:

- **History queue tasks do not park a worker.**
  `queues.executableImpl.RetryPolicy()` returns `backoff.DisabledRetryPolicy`,
  with the comment *"never retry task while holding the goroutine, and rely on
  shouldResubmitOnNack"*. A worker in the transfer/timer/visibility pools is held
  only for the duration of one `Execute()`.
- **Replication tasks do park a worker.**
  `replication.ExecutableTaskImpl.RetryPolicy()` is a real exponential policy with
  an expiration interval, and `FIFOScheduler.executeTask` /
  `SequentialScheduler.executeTask` run `backoff.ThrottleRetry` *inside* the
  worker. So a retrying replication task occupies its worker for the whole backoff.

That asymmetry means a cap is cheap and low-risk on the replication pools (idle in
single-cluster tests) and carries the real risk on the history queue pools.

#### A.1 Least invasive: cap only what is provably idle

One file, no production code:

```go
// tests/testcore/dynamic_config_overrides.go
dynamicconfig.ReplicationProcessorSchedulerWorkerCount.Key():            4,
dynamicconfig.ReplicationLowPriorityProcessorSchedulerWorkerCount.Key(): 4,
dynamicconfig.MemoryTimerProcessorSchedulerWorkerCount.Key():            4,
```

Removes 640 + 64 = **704 of 2 240 goroutines (31 %)** against pools measured at
zero tasks, with no change to any code path a single-cluster test exercises.

**Caveat:** `defaultDynamicConfigOverrides` is applied by `newTemporal` to *every*
cluster, including `tests/xdc` and `tests/ndc`, where replication is the thing
under test. Either set a value with real headroom there (see A.2) or scope the
override so the XDC/NDC suites keep the default.

#### A.2 Stopgap: cap the history queue pools too

Adding the three 512-worker history pools takes it to ~130 goroutines total. The
measured peak is 6, so **64 gives >10× headroom** while still cutting 88 % of the
goroutines; 4 gives no headroom worth relying on, and is what the one unexplained
282 s outlier ran with (8 of 9 runs passed; not reproduced in 8 further runs;
baseline 5/5).

Why a cap is a real risk and not just a slower pool: these are **host-level** pools
shared by every shard on the host. If a task's `Execute()` synchronously waits on
something that itself requires another task from the same pool to complete, a pool
smaller than that dependency depth deadlocks rather than merely queues. 512 is
effectively "large enough that this never happens". Nothing in the pool detects or
breaks such a cycle, so a cap converts a latency property into a liveness property.

Given that, treat A.2 as a stopgap: use 64, and validate against the full suite
plus `tests/xdc` and `tests/ndc`, repeatedly.

#### A.3 The actual fix: grow on demand, keep the ceiling

The important insight is that **lazy is not the same as capped**. If the pool can
still reach the configured 512, no workload that made progress before can fail to
make progress now — the reachable concurrency is unchanged and only goroutine
*creation* is deferred. A cap changes the reachable set; lazy growth does not.
That is why lazy growth is simultaneously the bigger win (converges to ~6–8, not
64) and the safer change.

The repo already contains this pattern twice, so this is not new design work:

- **`common/tasks/dynamic_worker_pool_scheduler.go`** — `DynamicWorkerPoolScheduler`.
  Workers are created at submit time up to `limiter.Concurrency()`; a worker
  drains the buffer and exits when it finds it empty. Already used in production by
  the **outbound queue factory** (`service/history/outbound_queue_factory.go`) —
  which is exactly why the outbound queue does not appear in the goroutine dump
  with hundreds of idle workers. Adapters already exist: `RunnableTask` turns a
  `Task` into a `Runnable`, and `RunnableScheduler` is the matching interface.
- **`common/goro/adaptive_pool.go`** — `AdaptivePool`. min/max workers, grows when
  a dispatch cannot be handed off within `targetDelay`, shrinks with jitter so it
  shrinks slower than it grows.

**Why `DynamicWorkerPoolScheduler` cannot starve** — worth copying, because the
guarantee is structural rather than timer-based. Enqueue and "worker decides to
exit" happen under the *same* mutex:

```go
// executeUntilBufferEmpty
pool.mu.Lock()
nextTask, ok := pool.dequeueLocked()
if !ok {
    pool.runningGoroutines--   // only decremented after observing an empty buffer
    pool.mu.Unlock()
    break
}
```

A task is only buffered when `runningGoroutines >= Concurrency()`, i.e. when at
least one worker is running; and the last worker can only exit after seeing an
empty buffer while holding the lock. A submitter blocked on that lock therefore
observes the *decremented* count and starts a fresh goroutine instead of buffering.
So "buffer non-empty ⇒ at least one worker running" holds at every point. (The one
precondition: `Concurrency()` must be ≥ 1 — a limiter returning 0 buffers tasks
with no worker to run them.)

**If you keep `FIFOScheduler` instead** (least invasive *code* change — ~40 lines,
no interface, backpressure or shutdown change, and `TrySubmit`'s "queue full"
semantics preserved):

1. Split target from actual. `updateWorkerCount(n)` stores the *target* and only
   shrinks eagerly; `Start` brings the pool up to a small `minWorkers` (1–2)
   instead of to the target. `Stop` still calls `updateWorkerCount(0)`, which
   shrinks actual to zero, so shutdown is unchanged.
2. Track idle workers: `idleWorkers.Add(1)` immediately before the blocking
   receive in `processTask`, `Add(-1)` on both wake-up paths.
3. On every successful send in `Submit` / `TrySubmit`, call a `maybeAddWorker()`
   that starts one worker if `idleWorkers <= 0 && len(workerShutdownCh) < target`,
   taking `workerLock` only on the slow path.

That is starvation-free by a counting argument rather than by the mutex trick:
every submitted task performs its own growth attempt, and growth is skipped only
while some worker is idle — and an idle worker consumes exactly one queued task.
The pool therefore converges on the peak number of concurrently executing tasks,
bounded by the unchanged target.

Because the argument rests on a racy read of `idleWorkers`, it is worth adding
cheap hardening: one watchdog goroutine per scheduler (6 per cluster — noise
against 2 240) ticking every ~100 ms that adds a worker whenever
`len(tasksChan) > 0 && idleWorkers == 0 && actual < target`. That makes eventual
growth to the ceiling unconditional, so the worst case degenerates to today's
behaviour plus one tick of latency. `ExecutionAwareScheduler` delegates `Submit`
straight to the base scheduler, so `len(tasksChan)` is the correct pending-work
signal here.

Do **not** shrink in v1. Shrinking re-introduces the "too few workers" state
repeatedly and buys nothing for short-lived test clusters, while for a production
host any converged size is already far better than 512 always-on. If added later,
shrink at most one worker per interval, only when every worker has been idle for
several consecutive intervals, and never below `minWorkers`.

**Keep the measurement.** The peak counters used for the table above are ~30 lines
and belong in the codebase as metrics (`DynamicWorkerPoolScheduler` already emits
`ActiveWorkers` / `BufferSize` every 10 s). Emitting active-workers and queue-depth
gauges for the FIFO and sequential schedulers makes the 512 defaults reviewable
with production data instead of guesses, and gives the lazy version an alarm if it
ever pins to its ceiling.

### B. Stop logging every fx hook at debug in tests

*Impact: ~12 % faster boot on its own, ~52 % combined with A; large reduction in
retained test-log memory. Effort: low.*

1. Gate the `OnStartExecuting` / `OnStartExecuted` / `OnStopExecuting` /
   `OnStopExecuted` / `invoking` cases in `fxLogAdapter.LogEvent`
   (`temporal/fx.go`) behind a flag, or drop them to a trace level that tests
   disable. They are per-provider and there are four graphs.
2. Default functional tests to `info` (or `warn`) instead of `debug`, keeping
   `TEMPORAL_TEST_LOG_LEVEL=debug` available for debugging a failure.
3. Fix `s.logger.Debug(s.so.config.String())` in `temporal/server_impl.go` so the
   masked YAML is only built when debug is enabled.

### C. Remove the 1 s floor on frontend shutdown drain

*Impact: ~1 s per cluster teardown whenever a long poll is open. Effort: low.*

Change `max(time.Second, s.config.ShutdownDrainDuration())` so the floor applies
only when the setting is unset, or let it be driven purely by dynamic config, and
set it to something small in `defaultDynamicConfigOverrides`. While there, make
history's 5 s readiness stabilization sleep a dynamic-config value so tests that
wait on history health don't pay it.

### D. Raise the shared-cluster hit rate

*Impact: the biggest multiplier available — it removes boots rather than making
them cheaper. Effort: medium.*

`clusterRequest.mustBeFresh()` (`tests/testcore/test_cluster_pool.go`) forces a
brand-new, non-reusable cluster whenever a test passes *any* dynamic config or
cluster option:

```go
func (r clusterRequest) mustBeFresh() bool {
	return r.needWorkerService || len(r.dynamicConfig) > 0 || len(r.clusterOpts) > 0
}
```

But per-test dynamic config overrides are already supported at runtime, with
cleanup, via `dcClient.PartialOverrideValue` / `overrideDynamicConfigForTest`. Most
`dynamicConfig` requests could therefore be served by a *pooled* cluster with a
scoped override instead of a fresh boot. Only genuinely un-hot-swappable settings
(and `clusterOpts`, mTLS, archival, fault injection, custom loggers) need a fresh
cluster.

`clusterRequest.recordCreation` already writes JSON Lines with a `reason` field
for exactly this analysis — run the suite with
`TEMPORAL_TEST_CLUSTER_EVENTS_FILE=/tmp/clusters.jsonl`, count clusters by
`kind`/`reason`, and convert the top offenders.

### E. Release sqlite databases so pooled clusters can be recycled

*Impact: removes unbounded per-binary memory growth; would let `maxLeases = 50`
go away. Effort: medium (needs care — the comment explains why it was disabled).*

Make `connPool.Close` actually close at `refCount == 0`. The commented-out code
notes that Temporal starts and stops DB connections multiple times, which loses
the in-memory DB; the fix is to tie the pool entry's lifetime to the test
cluster's lifetime rather than to individual factory close calls.

### F. Collapse the four fx graphs in onebox mode

*Impact: up to ~80 % of boot allocations are graph registration; history+frontend
+matching alone are 80 % of that. Effort: high.*

Build the common provider set once and share it across services rather than
re-registering `GetCommonServiceOptions` four times — this is the workaround the
function's own doc comment describes. A cheaper partial step: `fxreflect` caller
strings (7 %) and eager DOT metadata (2 %) are pure overhead with no test value,
but suppressing them needs an upstream `fx`/`dig` option.

Given D exists, this is worth doing only if the number of boots per run cannot be
reduced further.

## Suggested order of work

1. **A.1** (cap the replication + memory-timer pools, measured at zero tasks),
   **B** and **C** — small, safe, no behavioral risk, immediate. Together: −31 % of
   goroutines, ~12 % faster boot, −1 s per worker-service teardown.
2. **A.3** — lazy growth to the unchanged ceiling, ideally by moving the history
   queue pools onto `DynamicWorkerPoolScheduler` the way the outbound queue already
   does. This is the change that gets to ~130 goroutines without a cap, and it
   helps production too. **A.2** only if a stopgap is needed before A.3 lands, at
   64 rather than 4.
3. **D** — measure with the cluster-events file first, then convert the biggest
   `mustBeFresh` offenders.
4. **E**, then **F** if still needed.

Re-run `BenchmarkClusterBoot` and `TestClusterBootScaling` after each step and
compare the goroutine, stack, allocation and object counts rather than wall time.

## Future direction: a dedicated cluster per test?

Worth taking seriously. The honest summary is: **you cannot make a dedicated
cluster as cheap as a shared one, but you do not need to** — the payoff is
deleting the sharing machinery, not saving wall time. And the pool is already
carrying much less weight than it looks.

### The pool already only avoids about half the boots

Running the same 10 suites with `TEMPORAL_TEST_CLUSTER_EVENTS_FILE` set: **93 leaf
tests produced 46 clusters** — one per two tests.

| clusters | kind | reason |
|---|---|---|
| 35 | dedicated | `custom config` (a test passed a global dynamic config → `mustBeFresh`) |
| 6 | shared | shared pool |
| 5 | dedicated | worker service required |

**85 % of clusters are already one-shot dedicated boots.** The shared pool saved 6
boots out of 46. So the question is not "should we switch to dedicated" — the suite
is mostly there — it is "is the remaining sharing worth its complexity?"

What that complexity is: `sharedClusterT` proxy `T` with log fanout, cluster
poisoning plus teardown-on-poison plus `RegisterTest`, `maxLeases = 50` recreation
in CI, `isShared` panics on `CloseShard`/`InjectHook`, `mustBeFresh`, and the
documented caveat that per-test dynamic config overrides change the value for every
test in the suite (`dynamic_config_overrides.go`, NOTE2). Going all-dedicated
collapses `clusterPool` to a plain semaphore that bounds how many clusters are live
at once — no leases, no reuse, no poison tracking.

### The arithmetic

Marginal cost of a test on a shared cluster is **not zero**: `NewEnv` registers a
fresh namespace for every test (`base.RegisterNamespace`), which measures **~3 ms**.
So the starting comparison is:

| | per test, no race | per test, `-race` |
|---|---|---|
| shared cluster (namespace only) | ~3 ms | ~10 ms |
| dedicated today (boot + teardown + ns) | ~53 ms | ~345 ms |

Extrapolating to the whole suite (892 suite methods + 298 `s.Run` subtests ≈ 1 100
leaf tests): per-test dedicated roughly doubles cluster creations, from ~550 to
~1 100.

### Yes — the overhead can come down 2–3×, and then be hidden almost entirely

#### The schema DDL is the whole persistence phase, and it is 6× reducible

`schema/sqlite/setup_bench_test.go` measures this directly. The default in-memory
mode makes the plugin run all 101 DDL statements (37 in
`schema/sqlite/v3/temporal`, 64 in `.../visibility`) on first connect, per cluster:

| per fresh database | no race | `-race` |
|---|---|---|
| open in-memory DB + run schema (today) | 4.5–4.8 ms | **148.6 ms** |
| copy a 628 KiB template file + open, no DDL | 1.1–1.6 ms | **23.4 ms** |

148.6 ms against a measured persistence phase of 158 ms: the schema DDL *is* that
phase. `modernc.org/sqlite` is a pure-Go sqlite, so `-race` instruments every page
operation — which is also why the template copy wins so much more under `-race`
(6.3×) than without it (3–4×).

This means building the schema once per process and handing each cluster a byte
copy is worth **~125 ms per cluster under `-race`** — the single largest lever
available, and it is a self-contained change to the sqlite test cluster setup.
Note it moves tests from in-memory to file-backed sqlite; that configuration
already exists and is already exercised (`GetSQLiteFileTestClusterOption` is used
for shared clusters today), which de-risks it considerably. Put the directory on
tmpfs in CI.

#### Budget for a per-test dedicated cluster

| line item | today, `-race` | after | how |
|---|---|---|---|
| persistence (schema DDL) | 149 ms | 23 ms | template copy (measured) |
| fx graph (4 graphs) | 133 ms | ~113 ms → ~70 ms | B, then F (**F figure is an estimate**) |
| service start (2 240 goroutines) | 24 ms | ~10 ms | A.3 lazy pools |
| namespace registration | ~10 ms | ~1 ms | seed the namespace + search attributes into persistence before the server starts, skipping two cache-propagation poll loops |
| teardown | ~15 ms | ~5 ms | A.3, and C for the worker-service case |
| **total** | **~330 ms** | **~110–150 ms** | |

At 12-way parallelism, 1 100 per-test clusters costs ~32 s of wall time today and
~10–14 s after — against a suite that runs ~15 min. So even measured naively this
is a 1–3 % tax, not a blocker.

#### But the real answer: stop paying it on the critical path

The functional suite is **latency-bound, not CPU-bound**. The 10-suite subset used
above: 27.0 s wall, 11.7 s user + 5.0 s sys — **0.62 of 12 cores, ~5 % CPU
utilisation**, 795 MB peak RSS. The suite spends its time waiting on workflow
timers, long polls and `Eventually` loops.

Cluster boot is pure CPU work, and there is roughly **19× more CPU available than
the suite currently uses**. So the highest-leverage move is not to make boot
cheaper — it is to take it off the critical path:

> Keep a pool, but **never reuse a cluster**. Pre-boot pristine clusters on
> background goroutines and hand each test a fresh one, destroying it afterwards.

That gives full per-test isolation *and* near-zero per-test latency, paid for out
of idle CPU. It is also a smaller change than it sounds: `clusterPool` already has
slots and lazy creation — `acquire` stops reusing, and a background filler keeps
the slots stocked. Everything that exists to make reuse safe (leases, poison
tracking, `sharedClusterT`, `maxLeases`, `mustBeFresh`, `isShared` panics) is
deleted rather than extended.

Constraints on the warm-spare model:

- **E (sqlite release) is mandatory**, and now also means deleting template copies.
  1 100 never-closed databases is an OOM, not a slowdown.
- **A.3 (lazy pools) is mandatory** — live clusters plus warm spares at 2 400
  goroutines each is untenable; at ~130 each it is fine.
- Bound both live clusters and warm spares (a small multiple of GOMAXPROCS) so peak
  RSS stays where it is today. Memory, not CPU, is the binding constraint.
- A spare must be pristine — booted, namespace seeded, never touched by a test.
  That is exactly the property the current pool cannot offer.

The costs that per-test dedicated does *not* improve: the 221 MB test binary and
its ~2.6 s cold start, and total memory (which must be bounded explicitly rather
than bounded incidentally by cluster reuse).

### On sharing workers across clusters

Three different things get called "workers" here, and they answer differently:

- **The per-test SDK worker** (`sdkworker.New` in `TestEnv`, lazily via
  `sync.Once`): not shareable — it is bound to one client, endpoint and namespace,
  and every test already has its own namespace and task queue, so there is nothing
  to share. It also measures ~1 ms. The thing worth fixing is *ordering*: an SDK
  worker still polling at teardown is what triggers the 1 s frontend drain (C).
- **The system worker service**: sharing it across clusters would move its cost,
  not remove it — see the measurement below, which is the real reason.
- **The task-scheduler goroutine pools**: technically shareable across clusters in
  one process, and this is the tempting one — it would drive per-cluster goroutine
  cost to ~0. **Don't.** A process-global pool means a task stuck in `Execute()` in
  cluster A occupies a slot cluster B needs, which reintroduces exactly the
  cross-test coupling that per-test dedicated clusters exist to eliminate — and it
  would be invisible, intermittent coupling of the worst kind. A.3 delivers the same
  goroutine savings (~130 per cluster, ~1 600 across 12 live clusters) with no
  shared failure domain. Sharing the pools is only worth revisiting if A.3 lands and
  the goroutine count is still the binding constraint, which the numbers say it will
  not be.

The generalisable rule: **share immutable structure across clusters, never
stateful execution capacity.** The schema template, the fx provider graph, and
(possibly) a pre-allocated port block are stateless and safe to share. Worker
pools, connections and services carry state and coupling them defeats the purpose
of a dedicated cluster.

### Why the system worker service specifically cannot be hoisted out

Its cost is not a fixed per-cluster cost that could be shared — it is a
**per-namespace** cost, and every functional test registers its own namespace.
Measured by booting one cluster and registering namespaces one at a time (2 s
settle between samples, because per-namespace workers start asynchronously off the
namespace-change callback):

| namespaces registered | goroutines, worker service off | goroutines, worker service on |
|---|---|---|
| 0 (just booted) | 2 348 | 2 506 |
| 1 | +104 | +493 |
| 5 | +176 | +773 |
| 10 | +265 | +1 122 |

So a namespace costs ~26 goroutines on its own and **~112 with the worker service —
about 86 of them being the per-namespace SDK worker**. `PerNamespaceWorkerManager`
starts a worker for any namespace where *any* registered component reports
`Enabled`, and the scheduler component reports enabled for essentially every
namespace, so a test namespace that never touches a schedule still pays for a full
SDK worker with its pollers.

That is why "share one worker service across clusters" does not help: an
`sdkclient`/`sdkworker` is bound to one frontend endpoint *and* one namespace, so a
shared worker process would still need one SDK worker per (cluster, namespace) pair.
The goroutines move to a different owner; none of them disappear. The scanners are
worse — they read that cluster's persistence directly, so they cannot be hoisted at
all. (Membership is the weakest part of this argument, not the strongest: onebox has
exactly one worker host, so ownership is trivial.)

Two much better directions fall out of the same table:

1. **Make per-namespace workers start on demand.** If a namespace's worker started
   only when that namespace first needs it (first schedule, first batch operation)
   instead of on registration, the worker service would cost ~nothing for the
   overwhelming majority of test namespaces. Then it could be **on by default for
   pooled clusters**, and `WithWorkerService` would stop forcing a dedicated cluster
   — 5 of the 46 clusters in the sample above, and the ones that pay the 1 s
   teardown from C. As a cheap test-mode approximation, the scheduler component's
   `enabledForNs` is already dynamic-config driven, so it can be disabled globally in
   tests and enabled per namespace for the suites that exercise schedules. This is
   plausibly a production win too: a cluster with 10 k namespaces currently starts
   10 k SDK workers regardless of whether those namespaces use any of the features.
2. **This is an argument *for* per-test dedicated clusters.** `NewEnv` registers a
   namespace per test and never deletes it, so a pooled cluster accumulates every
   namespace of every test that has ever used it. At `maxLeases = 50` that is roughly
   +1 300 goroutines without the worker service and +5 600 with it, on top of the
   2 350 baseline, growing monotonically over the cluster's life. A dedicated cluster
   registers one namespace and throws it away. This accumulation is very likely a
   large part of why CI needs `maxLeases = 50` at all — so the sharing model is
   paying a cost that dedicated clusters simply do not have.

### Suggested decision path

Don't commit to per-test dedicated yet — the decision is cheap to make once the
prerequisites land, and expensive to reverse:

1. Land A.1, B, C (small and independently valuable).
2. Land E (sqlite release) and A.3 (lazy pools). Both are prerequisites for
   anything below and both pay off on their own.
3. Land the **schema template** (`schema/sqlite/setup_bench_test.go` already sizes
   the prize: ~125 ms per cluster under `-race`). Re-run `BenchmarkClusterBoot`
   under `-race` to confirm the boot drops to ~200 ms.
4. Make per-namespace workers lazy (or disable the per-namespace scheduler
   component in tests). This removes the last reason `WithWorkerService` implies a
   dedicated cluster, and removes the per-namespace accumulation that makes
   long-lived pooled clusters expensive.
5. Convert the pool to **warm spares with no reuse**, and delete the sharing
   machinery. This is the step that makes per-test dedicated cost ~nothing in wall
   time, and it does not depend on hitting any particular boot-time number.
6. F (one fx graph) only if boot CPU turns out to be the constraint after 5 — with
   ~19× CPU headroom it probably will not be, which is the case for deferring the
   most expensive change until last.
