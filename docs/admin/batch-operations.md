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

Each command has one confirmation prompt. The global `--yes` flag skips it.
