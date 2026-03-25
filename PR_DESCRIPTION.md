## What changed?

**Move CancelOutstandingWorkerPolls partition fan-out to matching and fix root recursion**

1. **Matching fan-out** – When matching receives a root partition request for `CancelOutstandingWorkerPolls`, it fans out to all partitions (0, 1, 2, …) and aggregates `CancelledCount`. Uses `NumReadPartitions()` for partition count.

2. **CancelOutstandingWorkerPollsPartition API** – New internal RPC that takes `TaskQueuePartition` with explicit partition ID (like `DescribeTaskQueuePartition`). Fixes recursion: previously, fan-out sent partition 0 as `"test-queue"`, which parses as root again and would recurse. The partition API avoids this by using partition ID directly.

3. **Frontend sends root only** – Frontend no longer iterates over partitions. It sends one request per task type (workflow, activity) with the root partition. Matching handles fan-out internally.

4. **Single CancelAll path** – `CancelAll` + `removePollerFromHistory` live in one place (partition handler). Refactored `removePollerFromHistory` to take `(ctx, partition, workerIdentity)`.

## Why?

- **Correctness** – The original fan-out sent partition 0 via `CancelOutstandingWorkerPolls` with `"test-queue"`, which parses as root and would cause infinite recursion. The partition API fixes this.
- **Consistency** – Matches the Describe API pattern (`DescribeTaskQueue` → `DescribeTaskQueuePartition`).
- **Simplicity** – Fan-out logic lives only in matching; frontend is a thin client. Single branch, single deployment.

## How did you test it?

- [x] built
- [x] covered by existing tests
- [x] added new unit test(s) – `TestCancelOutstandingWorkerPolls` root partition fan-out cases

## Potential risks

- During rollback, if matching is reverted before frontend, shutdown may not cancel polls on child partitions for a short period. Acceptable per product decision.
