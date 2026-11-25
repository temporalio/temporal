# Admin batch operations with `tdbg`

Admin batch workflows run in the `temporal-system` namespace and operate on executions in the namespace supplied with `--namespace`. Both commands below require a visibility query and a reason. The optional `--job-id` must not contain `:`; use `-` or `_` instead. If omitted, `tdbg` generates an ID. After a successful start, `tdbg` prints the workflow ID returned by the server, which has the form `target-namespace:job-id`.

## Delegate termination or deletion

Use `delegated-batch start` to terminate or delete matching workflows or activities. The supported `--batch-type` values are `terminate-workflows`, `delete-workflows`, `terminate-activities`, and `delete-activities`.

```sh
tdbg --namespace payments delegated-batch start \
  --batch-type terminate-workflows \
  --query "WorkflowType='OrderWorkflow' AND ExecutionStatus='Running'" \
  --reason "clean up stuck orders" \
  --job-id cleanup-orders
```

`tdbg` shows the target namespace, operation, query, and current match count before asking for confirmation. For example:

```text
Proceed with terminate-workflows on the currently matching 12 workflows? [y/N]:
```

Enter `y` to start the batch. A successful response prints the server-returned workflow ID, such as `payments:cleanup-orders`.

For a global target namespace, run any delegated termination or deletion against its **active cluster**. If `payments` is passive in the connected cluster, `tdbg` stops before the count and confirmation prompt and reports which cluster is active. No batch job starts. For example:

```text
namespace "payments" is active in cluster "cluster-a", but this cluster is "cluster-b": a batch operation must be started in the active cluster
```

The Admin API also rejects a delegated batch started through a passive cluster, even if the caller does not use `tdbg`.

## Refresh workflow tasks

Use `execution refresh-tasks` with a query to refresh tasks for matching workflows:

```sh
tdbg --namespace payments execution refresh-tasks \
  --query "WorkflowType='OrderWorkflow'" \
  --reason "recover stuck workflow tasks"
```

`tdbg` reports whether the connected cluster is active or passive for the target namespace, the current match count, and where the batch workflow will start. For example, when `payments` is passive in the connected cluster and the query currently matches 12 executions:

```text
This cluster is passive for namespace "payments". A batch workflow will be started in "temporal-system" to refresh tasks for 12 execution(s) matching query "WorkflowType='OrderWorkflow'". Continue? [y/N]:
```

If the connected cluster is active for `payments`, the prompt says `This cluster is active`. Enter `y` to start the batch workflow in that cluster's `temporal-system` namespace. A successful response prints the server-returned workflow ID, for example:

```text
Batch Refresh Workflow Tasks started successfully for Job ID: payments:batch-refresh-<timestamp>
```

This command can run in either the active or passive cluster of a global target namespace. Refreshing tasks on the passive cluster is an intended use: the batch operates on workflow state replicated to that cluster.

### Refresh tasks across all namespaces

Add `--all-namespaces` to refresh tasks for the workflows matching `--query` in every namespace, including `temporal-system`, with one batch job. `--query` is required, and `--workflow-id` and `--run-id` are rejected. `--namespace` is ignored, so a namespace set through `TEMPORAL_CLI_NAMESPACE` does not need to be unset. The query can only use system search attributes (such as `ExecutionStatus`, `StartTime`, `TaskQueue`) and predefined search attributes; tdbg and the Admin API reject a blank query or a query that uses any other field, including raw custom field names such as `Keyword01`. Set an accurate query so the job includes only the executions that need a refresh.

Use canonical field names such as `TemporalNamespaceDivision`. Unprefixed aliases such as `NamespaceDivision` and `ScheduleId` are rejected because custom fields can have those names. Use `TemporalScheduleId` for the schedule ID alias.

Unlike a query in one namespace, an all-namespaces query also matches executions whose `TemporalNamespaceDivision` is set: schedules, batch jobs, worker deployments, and CHASM executions. Add `TemporalNamespaceDivision IS NULL` to skip them. It does not skip `temporal-system` workflows that leave `TemporalNamespaceDivision` unset, such as scanners, so the query can still match those:

```sh
tdbg execution refresh-tasks \
  --all-namespaces \
  --query "ExecutionStatus='Running' AND StartTime < '2026-11-20T00:00:00Z' AND TemporalNamespaceDivision IS NULL" \
  --reason "recover stuck workflow tasks"
```

The confirmation prompt shows the number of matching executions across all namespaces and states that `temporal-system` is included. A successful response prints a workflow ID of the form `all-namespaces-<job-id>`.

Only task refresh supports all namespaces: the Admin API field is `BatchOperationRefreshTasks.all_namespaces`. Delegated termination and deletion must run in each target namespace's active cluster; start one `delegated-batch` per namespace instead.

Each command has one confirmation prompt. The global `--yes` flag skips it.
