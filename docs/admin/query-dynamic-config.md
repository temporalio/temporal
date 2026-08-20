# Query effective dynamic config with TDBG

Use `tdbg dynamic-config get` to query the effective value of one dynamic config key from a running frontend server. The `dc` alias can be used in place of `dynamic-config`.

## Build TDBG

From the Temporal repository root:

```bash
make tdbg
```

## Query a namespace setting

The global `--address` and `--namespace` flags must appear before the command:

```bash
./tdbg \
  --address 127.0.0.1:7233 \
  --namespace A \
  dc get \
  --key frontend.WorkflowTimeSkippingEnabled
```

Example output:

```json
true
```

The server uses the setting's existing property function, so this example checks values in the following order:

1. The configured value with `{Namespace: "A"}`.
2. The configured value with no constraints.
3. The registered code default, which is `false` for `frontend.WorkflowTimeSkippingEnabled`.

## Other filters

Supply only the filters used by the setting being queried.

| Setting filter | TDBG flags |
| --- | --- |
| Global | No additional flags |
| Namespace | `--namespace` before `dc` |
| Namespace ID | `--namespace-id` |
| Task queue | `--namespace`, `--task-queue`, `--task-queue-type` |
| Shard ID | `--shard-id` |
| History task type | `--task-type` |
| Destination | `--namespace`, `--destination` |
| CHASM task type | `--chasm-task-type` |

Use protobuf enum names for enum filters, for example:

```bash
./tdbg \
  --address 127.0.0.1:7233 \
  --namespace A \
  dc get \
  --key some.task.queue.key \
  --task-queue payments \
  --task-queue-type TASK_QUEUE_TYPE_WORKFLOW
```

## Errors

- `dynamic config key is not set`: `--key` was empty.
- `unregistered dynamic config key`: the running server does not have a registered setting for the key.
- `invalid task queue type` or `invalid task type`: use a valid protobuf enum name.
- RPC connection errors: verify `--address` points to the target frontend admin service and that the TDBG and server versions are compatible.

## Lookup cost

An ad hoc benchmark on an Apple M4 Pro measured the direct property function at approximately 33 ns per lookup with no allocations. The reflection-based lookup used by this command measured approximately 435 ns, 249 bytes, and 9 allocations per lookup. This overhead is acceptable for an operator command that is not called on a service's request path.

## Current scope

- The command returns one effective value as JSON.
- It queries the in-memory dynamic config state of the frontend process receiving the RPC.
- It does not dump all keys or explain how the value was selected.
- It uses the setting registered for the key. It cannot observe a service-specific default introduced at a call site with `WithDefault`.
