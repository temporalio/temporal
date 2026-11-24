# In-memory immediate history tasks

WHN dispatch uses a host-wide, on-demand goroutine pool, separate from speculative
Workflow Task timers. Submission does not wait for capacity. Tasks that exceed
`history.memoryImmediateProcessorMaxConcurrentTasks` are dropped without persistence,
retry, or rescheduling. The default limit is 512 concurrent tasks. No notifications
wait in a backlog. Shard shutdown cancels its active tasks; host shutdown cancels
and waits for the pool. Completing the originating RPC does not cancel dispatch.

The existing `dynamic_worker_pool_scheduler_rejected_tasks` metric, tagged with
operation `MemoryImmediateQueueProcessor`, counts rejected submissions. Existing WHN
result and latency metrics continue to describe attempted dispatches.

## Sizing blocking dispatch

A dispatch occupies a goroutine while waiting for Matching and the worker response.
For throughput N notifications/second and mean latency L seconds, the theoretical
minimum concurrency is N × L. Add headroom for burst arrivals, latency variation,
and scheduling delays. These are goroutines, not dedicated OS threads.

At a simulated 300 ms dispatch latency:

| Target notifications/s | Theoretical minimum concurrency | Example with 30% headroom |
| --- | --- | --- |
| 100 | 30 | 39 |
| 1,000 | 300 | 390 |
| 10,000 | 3,000 | 3,900 |

The default 512 supports the example 1,000/s load with headroom at 300 ms. It is not
a universal throughput guarantee. If dispatches occupy the full 600 ms default
timeout, the theoretical capacity falls to about 853/s, before headroom. Set the cap
using actual per-host WHN arrival rates and dispatch latency; increasing it increases
concurrent RPCs and resource use in History, Matching, and workers.

## Blocking-dispatch benchmark

Run from the repository root:

```sh
go test -tags test_dep ./service/history/queues -run '^$' \
  -bench '^BenchmarkMemoryImmediateDispatch$' -benchtime=3000x -count=1
```

This benchmark exercises the same zero-buffer worker pool with a fixed 300 ms wait
per accepted task. It offers 1,000 submissions/s for 3,000 submissions. It simulates
blocking latency; it does not measure RPC, serialization, persistence, or worker
execution costs. There is no speculative-task persistence fallback.

One run on an Apple M4 Pro (darwin/arm64):

| Concurrency cap | Admitted/s during submissions | Completed/s including final drain | Dropped |
| --- | --- | --- | --- |
| 64 | 213.4 | 208.3 | 78.67% |
| 128 | 426.8 | 407.4 | 57.33% |
| 256 | 853.6 | 783.5 | 14.67% |
| 512 | 1,000 | 909.0 | 0% |

The completed rate includes waiting for the last accepted tasks to finish, so it
is lower over this short run. The admission rate demonstrates why the previous
64-worker draft cap cannot sustain 1,000/s at 300 ms latency. Production sizing
still needs the actual arrival distribution and latency measurements.
