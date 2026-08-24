# Add Durable Batch Schedule Migration to `tdbg`

## Summary

This change adds durable, server-side batch migration for Temporal schedules. Operators can migrate all schedules selected from visibility from the workflow-backed scheduler (V1) to the CHASM-backed scheduler (V2), or roll V2 schedules back to V1, without manually supplying scheduler namespace-division predicates or running one RPC per schedule from `tdbg`.

The implementation extends the existing admin batch framework used by workflow-task refresh. A visibility-based invocation starts one internal batch workflow in one namespace. That workflow runs a heartbeat-enabled batch activity, pages through matching schedules, applies existing server-side concurrency and rate limits, and invokes the existing single-schedule `MigrateSchedule` API for each result.

The single-schedule migration path remains available and unchanged.

## Motivation

The existing `MigrateSchedule` admin API initiates migration for one schedule. Migrating or rolling back a large population previously required an operator-controlled loop. A client-side loop has several operational weaknesses:

- The operation stops if `tdbg` exits or loses its connection.
- Progress is not durable across worker or client failures.
- Pagination, retries, concurrency, and throttling must be implemented by every caller.
- It is easy to query the wrong scheduler implementation or omit `TemporalNamespaceDivision` entirely.
- Repeated requests need carefully managed request IDs to remain idempotent.

The admin batcher already solves these problems for workflow-task refresh. Reusing it keeps schedule migration namespace-scoped, durable, rate-limited, and consistent with existing operational tooling.

## API Changes

`StartAdminBatchOperationRequest.operation` now supports a schedule migration operation:

```protobuf
oneof operation {
  BatchOperationRefreshTasks refresh_tasks_operation = 10;
  BatchOperationMigrateSchedules migrate_schedules_operation = 11;
}

message BatchOperationMigrateSchedules {
  MigrateScheduleRequest.SchedulerTarget target = 1;
}
```

The target uses the existing scheduler target enum:

- `SCHEDULER_TARGET_CHASM`: migrate V1 schedules to V2.
- `SCHEDULER_TARGET_WORKFLOW`: roll V2 schedules back to V1.

For schedule migration, `visibility_query` is treated as an additional user filter. The server derives and enforces the source scheduler predicate from the target. An empty user query means all running schedules using the source implementation.

An explicit `executions` list is also supported by the admin API. As with other admin batch operations, all selected executions belong to the request namespace.

## How It Works

### Request flow

```text
tdbg schedule migrate --from-visibility
    |
    | AdminService.StartAdminBatchOperation
    v
internal BatchWorkflowProtobuf
    |
    | one heartbeat-enabled BatchActivityWithProtobuf
    v
visibility pagination and in-process worker pool
    |
    | one AdminService.MigrateSchedule call per schedule
    v
existing V1 -> V2 or V2 -> V1 migration implementation
```

The frontend validates the namespace, job ID, reason, identity, and migration target. It stores the complete `StartAdminBatchOperationRequest` in `BatchOperationInput` and starts an internal batch workflow on the namespace's per-namespace worker task queue.

The batch workflow executes one long-running activity. The activity:

1. Verifies that the request namespace and namespace ID match the namespace-bound worker.
2. Builds the effective visibility query from the target and the user filter.
3. Counts the estimated number of matching schedules.
4. Fetches visibility results in pages.
5. Processes schedules through the batcher's in-process concurrency pool.
6. Records page tokens and success/failure counters in activity heartbeats.
7. Calls `MigrateSchedule` once for each selected schedule.

If the activity or worker restarts, the Temporal activity retry restores the latest heartbeat and resumes from the last completed page. It does not create one Temporal activity per schedule.

### Namespace selection

Each batch job operates in exactly one namespace. The namespace comes from the standard `tdbg -n`/`--namespace` option and is passed in `StartAdminBatchOperationRequest.namespace`.

Visibility queries are already evaluated within that namespace, so users must not add namespace selection to the visibility query. The per-namespace worker additionally verifies that the namespace name and namespace ID agree before processing any schedules.

Cross-namespace migration requires one batch job per namespace. This matches the workflow-task refresh batch API and preserves namespace-level authorization, throttling, concurrency limits, and job ownership.

### Automatic source selection

Users do not need to know or enter `TemporalNamespaceDivision` values. The target determines the source implementation.

For a V1 to V2 migration (`--target chasm`), the server uses:

```sql
TemporalNamespaceDivision = 'TemporalScheduler'
AND ExecutionStatus = 'Running'
```

For a V2 to V1 rollback (`--target workflow`), the server uses:

```sql
TemporalNamespaceDivision = '<CHASM scheduler archetype ID>'
AND ExecutionStatus = 'Running'
```

If the user supplies `--query`, the server combines it with the source query:

```sql
(<source scheduler query>) AND (<user query>)
```

This composition happens in the server-side batch activity. `tdbg` independently constructs the same effective query only to show an accurate dry-run count. Keeping the authoritative composition on the server prevents direct API callers from accidentally selecting schedules from the target implementation.

### Schedule ID normalization

Operators always use the schedule ID, not the V1 scheduler workflow ID.

For example, use:

```text
critical-hourly
```

Do not require users to provide:

```text
temporal-sys-scheduler:critical-hourly
```

V1 visibility records contain the prefixed scheduler workflow ID, while V2 visibility records contain the schedule business ID directly. Before calling `MigrateSchedule`, the batch worker removes `temporal-sys-scheduler:` when present. Removing the prefix is a no-op for V2 IDs and explicit raw schedule IDs.

`--query` uses generic workflow visibility semantics. It does not apply the synthetic `ScheduleId` rewrite performed by the public `ListSchedules` API. To select one schedule by raw ID, use `--schedule-id`. If a visibility batch must filter by `WorkflowId`, V1 values include the `temporal-sys-scheduler:` prefix while V2 values use the raw schedule ID.

### Idempotency and retries

Each per-schedule `MigrateSchedule` request uses a deterministic request ID derived from:

- Batch job ID.
- Operation name.
- Migration target.
- Schedule ID.

If activity retry or heartbeat recovery revisits a schedule, it sends the same request ID. Existing migration deduplication can therefore collapse repeated initiation requests from the same batch job.

The batcher's existing rate limits and concurrency settings apply:

- Namespace batch concurrency controls the number of schedules processed concurrently in one job.
- Admin batch host/global RPS limits control aggregate admin batch traffic.
- The frontend's admin-batch concurrency limit controls the number of active admin batch jobs in a namespace.

## Usage

### Preview a V1 to V2 migration

Visibility-based migration defaults to a dry run. The dry run counts schedules with the effective source query but does not start a batch job.

```bash
tdbg -n payments schedule migrate \
  --target chasm \
  --from-visibility
```

Example output:

```text
Dry-run: 125 schedule(s) in namespace "payments" match "TemporalNamespaceDivision = 'TemporalScheduler' AND ExecutionStatus = 'Running'" and would be migrated to chasm. Re-run with --execute to start the batch.
```

### Start a V1 to V2 migration

```bash
tdbg -n payments schedule migrate \
  --target chasm \
  --from-visibility \
  --execute \
  --reason "CHASM scheduler rollout" \
  --job-id migrate-payments-to-chasm
```

If `--job-id` is omitted, `tdbg` generates one. A reason is required when starting the durable batch.

### Preview a V2 to V1 rollback

```bash
tdbg -n payments schedule migrate \
  --target workflow \
  --from-visibility
```

### Start a V2 to V1 rollback

```bash
tdbg -n payments schedule migrate \
  --target workflow \
  --from-visibility \
  --execute \
  --reason "Rollback CHASM scheduler rollout" \
  --job-id rollback-payments-to-workflow
```

### Narrow the selection

`--query` narrows the automatically selected source population; it does not replace the source scheduler predicate.

```bash
tdbg -n payments schedule migrate \
  --target workflow \
  --from-visibility \
  --query "WorkflowId STARTS_WITH 'critical-'"
```

After reviewing the count, execute the same selection:

```bash
tdbg -n payments schedule migrate \
  --target workflow \
  --from-visibility \
  --query "WorkflowId STARTS_WITH 'critical-'" \
  --execute \
  --reason "Rollback critical schedules"
```

### Migrate one schedule

The existing single-schedule mode remains available. Supply the raw schedule ID without the V1 workflow prefix:

```bash
tdbg -n payments schedule migrate \
  --schedule-id critical-hourly \
  --target chasm
```

## Completion Semantics

The single-schedule migration API initiates an asynchronous migration. Consequently, a successful batch item means that the server accepted or idempotently handled the migration request. It does not guarantee that the schedule has already completed conversion when the batch item is counted as successful.

The two directions complete asynchronously:

- V1 to V2 signals the legacy scheduler workflow. The scheduler workflow subsequently runs its migration logic and transitions authority to the CHASM scheduler.
- V2 to V1 records migration state on the CHASM scheduler and schedules a side-effect task. That task restores V1 state, starts the workflow-backed scheduler, and then closes the V2 scheduler.

Use migration status and visibility counts to confirm convergence after the batch has initiated all requests:

```bash
# Namespace-wide V1/V2 counts
tdbg -n payments schedule migrate status

# Detailed state for one schedule, including sentinel detection
tdbg -n payments schedule migrate status \
  --schedule-id critical-hourly
```

The detailed status command probes both the V1 workflow ID and the V2 CHASM business ID. It distinguishes genuine schedulers, migration sentinels, missing entities, and unexpected cases where both sides appear authoritative.

## Sentinel Handling

Sentinels reserve a schedule ID on the destination implementation while authority is being transferred. Their presence can be expected during migration and does not by itself indicate data loss or duplicate schedule authority.

### V1 to V2 sentinel

During V1 to V2 migration, a CHASM scheduler sentinel can exist at the schedule business ID while the genuine V1 scheduler workflow is still present. In this state:

- V1 remains authoritative.
- The V2 entity is only reserving the destination ID.
- Operators should not delete the V1 workflow or the V2 sentinel manually.
- The migration should be allowed to finish through the V1 scheduler workflow's migration path.

The status command reports this state as a genuine V1 schedule plus a V2 sentinel:

```bash
tdbg -n payments schedule migrate status \
  --schedule-id critical-hourly
```

Expected interpretation:

```text
V1 (workflow-backed): genuine
V2 (CHASM): sentinel (V1->V2 migration placeholder)
```

If the state persists, inspect both sides using the commands printed by `migrate status`. Check the V1 scheduler workflow for pending migration work and migration activity failures before retrying. A new batch with the same selection is safe after the underlying failure is addressed because source visibility continues to select the still-authoritative V1 schedule and migration creation handles existing destination state idempotently.

### V2 to V1 sentinel

During V2 to V1 rollback, a dummy V1 workflow can reserve `temporal-sys-scheduler:<schedule-id>` while the genuine V2 scheduler still exists. In this state:

- V2 remains authoritative.
- The dummy V1 workflow is a rollback placeholder, not a scheduler workflow.
- The migration API returns the sentinel-blocked `Unavailable` error until the sentinel's idle period expires.
- Operators should not terminate the V2 scheduler or manually replace the dummy workflow.

The status command reports this state as a genuine V2 schedule plus a V1 sentinel:

```text
V1 (workflow-backed): sentinel (V2->V1 rollback placeholder)
V2 (CHASM): genuine
```

A sentinel-blocked schedule may be counted as a failed item by the batch after its per-item retry budget is exhausted. Handle it as follows:

1. Run `tdbg schedule migrate status --schedule-id <id>` to confirm that the existing V1 workflow is a dummy sentinel rather than a genuine scheduler.
2. Wait for the sentinel idle period to expire. The server logs include the remaining sentinel idle time when the request is blocked.
3. Re-run a narrowly filtered rollback batch, or migrate the schedule individually.

For example:

```bash
tdbg -n payments schedule migrate \
  --target workflow \
  --from-visibility \
  --query "WorkflowId = 'critical-hourly'" \
  --execute \
  --reason "Retry after V1 rollback sentinel expired"
```

Or:

```bash
tdbg -n payments schedule migrate \
  --schedule-id critical-hourly \
  --target workflow
```

The source query still selects the genuine V2 scheduler, so rerunning after expiration does not require changing namespace-division predicates.

### Unexpected dual-authority state

If status reports genuine V1 and genuine V2 schedulers simultaneously, do not blindly rerun the batch or delete either side. This is not an expected transient sentinel state and requires manual investigation. Inspect both mutable states and determine which scheduler is authoritative before taking corrective action.

## Failure and Recovery Behavior

- A visibility listing or counting failure fails the batch activity and is retried according to its activity retry policy.
- Per-schedule failures are recorded in batch success/failure counters and do not stop processing unrelated schedules.
- `NotFound` is treated as an idempotent no-op by the shared batch processor. This covers schedules that closed, were deleted, or already moved after visibility selected them.
- Worker or process failure resumes from activity heartbeat state.
- Reusing the same job ID is rejected by workflow ID reuse/conflict policy. Use a new job ID when intentionally starting a follow-up batch.
- A follow-up batch is safe for schedules that remain visible on the source implementation. Completed migrations no longer match the source namespace-division query.

## Safety Properties

- Bulk visibility migration requires the explicit `--from-visibility` selector.
- Visibility mode defaults to dry-run and requires `--execute` to start a job.
- Executing a durable batch requires an operator-supplied reason.
- Each job is restricted to one namespace.
- The server, rather than the CLI, enforces source scheduler selection.
- Namespace name and namespace ID are revalidated by the per-namespace worker.
- Deterministic request IDs protect per-schedule retries.
- Existing admin batch concurrency and RPS controls limit load.
- The target implementation is validated before the internal batch workflow starts.

## Testing

The change includes tests covering:

- Validation of supported, unspecified, and unknown migration targets.
- Preservation of selector requirements for workflow-task refresh.
- Automatic V1 source query generation for migration to CHASM.
- Automatic V2 source query generation for rollback to workflow.
- Composition of user visibility filters with server-generated source predicates.
- V1 workflow ID prefix removal and V2 raw business ID handling.
- Propagation of namespace, target, identity, and deterministic request ID to `MigrateSchedule`.
- `tdbg` dry-run counting without starting a migration.
- Creation of one durable admin batch request for one namespace.
- Propagation of reason, job ID, target, and user query from `tdbg`.
- Rejection of client-side worker/output-log options for server-side visibility batches.

Generated admin service protobufs, helpers, and mocks are updated as part of the change.

## Operational Notes

- Roll out frontend and worker support before operators start schedule migration batches. An older worker does not understand the new admin batch operation.
- Batch success counts initiation, while `schedule migrate status` and visibility counts confirm actual convergence.
- Prefer several namespace-scoped jobs over a cluster-wide client loop. This preserves per-namespace controls and makes partial rollback easier to reason about.
- Use unique, descriptive job IDs and reasons so internal batch workflows are easy to audit.
