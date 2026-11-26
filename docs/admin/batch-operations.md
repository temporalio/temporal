# Admin batch operations with `tdbg`

Admin batch jobs run in the connected cluster's `temporal-system` namespace. Their targets are the executions selected by `--namespace` and `--query`, or by `--all-namespaces` and `--query` for task refreshes.

All commands in this guide require `--query` and `--reason`. They show one confirmation prompt before starting a job; the global `--yes` flag skips it.

You can also supply `--job-id`. It must not contain `:`; use `-` or `_` instead. If omitted, `tdbg` generates an ID. The command prints the batch workflow ID returned by the server:

| Target scope | Batch workflow ID |
| --- | --- |
| One namespace | `<target-namespace>:<job-id>` |
| All namespaces | `all-namespaces-<job-id>` |

## Delegate termination or deletion

Use `delegated-batch start` to terminate or delete matching workflows or activities in one namespace:

```sh
tdbg --namespace payments delegated-batch start \
  --batch-type terminate-workflows \
  --query "WorkflowType='OrderWorkflow' AND ExecutionStatus='Running'" \
  --reason "clean up stuck orders" \
  --job-id cleanup-orders
```

Supported `--batch-type` values:

- `terminate-workflows`
- `delete-workflows`
- `terminate-activities`
- `delete-activities`

The confirmation shows the operation and current match count. For example:

```text
Proceed with terminate-workflows on the currently matching 12 workflows? [y/N]:
```

Enter `y` to start the job. In this example, the batch workflow ID is `payments:cleanup-orders`.

For a global namespace, connect to its **active cluster**. Both `tdbg` and the Admin API reject delegated batches started through a passive cluster. `tdbg` reports which cluster is active before asking for confirmation.

## Refresh workflow tasks in one namespace

Use `execution refresh-tasks` with a query to regenerate pending tasks for matching workflows:

```sh
tdbg --namespace payments execution refresh-tasks \
  --query "WorkflowType='OrderWorkflow'" \
  --reason "recover stuck workflow tasks"
```

The confirmation shows the target namespace, whether the connected cluster is active or passive for it, and the current match count. After confirmation, the job starts in `temporal-system` and prints its workflow ID. For example:

```text
Batch Refresh Workflow Tasks started successfully for Job ID: payments:batch-refresh-<timestamp>
```

Task refreshes operate on the **connected cluster**. They can run in either the active or passive cluster of a global namespace. On a passive cluster, they refresh tasks for the workflow state replicated there.

## Refresh tasks across all namespaces

Add `--all-namespaces` to refresh matching executions across every namespace in the connected cluster, including `temporal-system`, with one job:

```sh
tdbg execution refresh-tasks \
  --all-namespaces \
  --query "ExecutionStatus='Running' AND StartTime < '2026-11-20T00:00:00Z' AND TemporalNamespaceDivision IS NULL" \
  --reason "recover stuck workflow tasks"
```

With `--all-namespaces`:

- `--query` is required and must not be blank.
- `--namespace`, `TEMPORAL_CLI_NAMESPACE`, `--workflow-id`, and `--run-id` are ignored. The query selects the targets.
- The confirmation shows the match count across all namespaces and states that `temporal-system` is included.
- The batch workflow ID has the form `all-namespaces-<job-id>`.

Only task refresh supports this scope. Start a separate `delegated-batch` in each target namespace's active cluster for termination or deletion.

### Choose a query

Use system search attributes, such as `WorkflowType`, `ExecutionStatus`, `StartTime`, and `TaskQueue`, or predefined search attributes. Both `tdbg` and the Admin API reject custom search attributes, including aliases such as `CustomerId` and raw field names such as `Keyword01`.

Use canonical field names such as `TemporalNamespaceDivision`. Unprefixed aliases such as `NamespaceDivision` and `ScheduleId` are rejected because custom fields can have those names. Use `TemporalScheduleId` for the schedule ID alias.

All-namespaces queries also include executions with `TemporalNamespaceDivision` set, such as schedules, batch jobs, worker deployments, and CHASM executions. Add `TemporalNamespaceDivision IS NULL` to exclude those executions, as in the example above.

That condition still includes workflows whose division is unset, including some `temporal-system` workflows such as scanners. Narrow the query to the executions that need a refresh.

### Visibility store selection

With dual visibility configured, counting and listing use the store selected by `system.enableReadFromSecondaryVisibility` evaluated for an empty namespace. This follows the existing Admin visibility API behavior; namespace-specific reader overrides do not apply.

During a visibility store migration, ensure the selected store contains the executions you intend to refresh. To use namespace-specific routing, run separate refresh batches with `--namespace`.

### Direct Admin API requests

Set `BatchOperationRefreshTasks.all_namespaces` to `true` and provide `visibility_query`. Leave `namespace` and `executions` empty; the Admin API rejects either field when populated. `tdbg` omits these fields when `--all-namespaces` is selected.
